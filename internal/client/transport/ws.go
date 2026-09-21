package transport

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/musix/backhaul/config"
	"github.com/musix/backhaul/internal/utils"
	"github.com/musix/backhaul/internal/utils/handlers"
	"github.com/musix/backhaul/internal/utils/network"
	"github.com/musix/backhaul/internal/web"

	"github.com/sirupsen/logrus"
)

type WsTransport struct {
	config    *WsConfig
	parentctx context.Context
	// ctx and cancel belong to gen and are republished with it by Restart.
	ctx    context.Context
	cancel context.CancelFunc
	// gen is the current generation (nil = untracked, see wsGeneration).
	gen            *wsGeneration
	logger         *logrus.Logger
	controlChannel *network.WebSocketConn
	// controlMu guards controlChannel. The dialer installs it, the channel
	// handler reads it on every heartbeat and Restart clears it, all from
	// different goroutines - unsynchronized that is a data race, and Restart
	// nilling the pointer while the handler dereferences it is a crash.
	controlMu       sync.Mutex
	restartMutex    sync.Mutex
	usageMonitor    *web.Usage
	poolConnections int32
	loadConnections int32
	controlFlow     chan struct{}
	// userAgent is picked once per process instead of per dial - see the
	// same field on WsMuxTransport for why.
	userAgent string

	// endpoints and dialSeq mirror the wsmux round-robin: all pool and
	// control dials spread across every configured entry point.
	endpoints []wsEndpoint
	dialSeq   int32
}
type WsConfig struct {
	RemoteAddr     string
	RemoteAddrs    []string // optional list; non-empty wins over RemoteAddr
	EdgeIPs        []string // per-entry edge IPs, aligned by index with RemoteAddrs
	Token          string
	SnifferLog     string
	TunnelStatus   string
	Nodelay        bool
	Sniffer        bool
	KeepAlive      time.Duration
	RetryInterval  time.Duration
	DialTimeOut    time.Duration
	ConnPoolSize   int
	WebPort        int
	Mode           config.TransportType
	AggressivePool bool
	EdgeIP         string
	Path           string
	SO_RCVBUF      int
	SO_SNDBUF      int
	MSS            int
	TLSVerify      bool // wss: verify the server certificate during the TLS handshake
}

// nextEndpoint returns the next entry point via atomic round-robin.
func (c *WsTransport) nextEndpoint() wsEndpoint {
	if len(c.endpoints) == 1 {
		return c.endpoints[0]
	}
	i := atomic.AddInt32(&c.dialSeq, 1)
	return c.endpoints[int(uint32(i))%len(c.endpoints)]
}

func NewWSClient(parentCtx context.Context, config *WsConfig, logger *logrus.Logger) *WsTransport {
	// Create the first generation; its context derives from the parent context
	gen := newWsGeneration(parentCtx)
	ctx, cancel := gen.ctx, gen.cancel

	// Initialize the TcpTransport struct
	client := &WsTransport{
		gen:             gen,
		config:          config,
		parentctx:       parentCtx,
		ctx:             ctx,
		cancel:          cancel,
		logger:          logger,
		controlChannel:  nil, // will be set when a control connection is established
		usageMonitor:    web.NewDataStore(fmt.Sprintf(":%v", config.WebPort), ctx, config.SnifferLog, config.Sniffer, &config.TunnelStatus, logger),
		poolConnections: 0,
		loadConnections: 0,
		controlFlow:     make(chan struct{}, 100),
		userAgent:       network.RandomUserAgent(),
	}

	client.endpoints = buildEndpoints(config.RemoteAddrs, config.EdgeIPs, config.RemoteAddr, config.EdgeIP)

	return client
}

func (c *WsTransport) Start() {
	g := c.gen
	// for  webui
	if c.config.WebPort > 0 {
		// A worker, so that Restart waits for the web port to be released before
		// the next generation's monitor tries to bind it.
		g.start(c.usageMonitor.Monitor)
	}

	c.config.TunnelStatus = fmt.Sprintf("Disconnected (%s)", c.config.Mode)

	g.start(c.channelDialer)

}

// requestRestart asks for a Restart from outside the generation. Restart joins
// every worker of the generation it stops, so a worker calling it inline would
// be waiting for itself; every worker requests through here instead.
func (c *WsTransport) requestRestart() {
	go c.Restart()
}

// Restart stops the current generation, waits for every one of its workers, and
// only then publishes the next one. See WsMuxTransport.Restart.
func (c *WsTransport) Restart() {
	if !c.restartMutex.TryLock() {
		c.logger.Warn("client is already restarting")
		return
	}
	defer c.restartMutex.Unlock()

	c.logger.Info("restarting client...")

	// for removing timeout logs
	level := c.logger.Level
	c.logger.SetLevel(logrus.FatalLevel)

	old := c.gen
	if old == nil && c.cancel != nil {
		c.cancel() // untracked transport: nothing to join
	}
	old.stop()
	joined := old.join(restartJoinTimeout)

	// set the log level again
	c.logger.SetLevel(level)

	if !joined {
		c.logger.Errorf("restart: previous generation's workers did not end within %s; not starting a new generation over them, retrying", restartJoinTimeout)
		time.AfterFunc(restartJoinTimeout, c.Restart)
		return
	}
	if c.parentctx.Err() != nil {
		c.logger.Info("restart abandoned: the client is shutting down")
		return
	}

	// No worker of the old generation is left, so everything below is ours alone.
	c.controlMu.Lock()
	stale := c.controlChannel
	c.controlChannel = nil
	c.controlMu.Unlock()
	if stale != nil {
		stale.Close()
	}

	g := newWsGeneration(c.parentctx)
	c.gen, c.ctx, c.cancel = g, g.ctx, g.cancel

	// Re-initialize variables
	c.usageMonitor = web.NewDataStore(fmt.Sprintf(":%v", c.config.WebPort), g.ctx, c.config.SnifferLog, c.config.Sniffer, &c.config.TunnelStatus, c.logger)
	c.config.TunnelStatus = ""
	atomic.StoreInt32(&c.poolConnections, 0)
	atomic.StoreInt32(&c.loadConnections, 0)
	c.controlFlow = make(chan struct{}, 100)

	c.Start()
}

func (c *WsTransport) channelDialer() {
	// The generation this worker belongs to, captured once: Restart republishes
	// c.ctx/c.gen only after this worker has returned.
	g, ctx := c.gen, c.ctx

	c.logger.Info("attempting to establish a new websocket control channel connection")

	for {
		select {
		case <-ctx.Done():
			return
		default:
			ep := c.nextEndpoint()
			tunnelWSConn, err := network.WebSocketDialer(ctx, ep.addr, ep.edgeIP, network.NormalizeBasePath(c.config.Path)+"/channel", c.config.DialTimeOut, c.config.KeepAlive, true, c.config.Token, c.userAgent, c.config.Mode, 3, 0, 0, 0, c.config.TLSVerify)
			if err != nil {
				c.logger.Errorf("control channel dialer: %v", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(c.config.RetryInterval):
				}
				continue
			}
			// Owned from the moment it exists: if the generation was stopped while
			// this dial was in flight, own closes the socket and we just leave.
			if !g.own(tunnelWSConn) {
				return
			}
			c.controlMu.Lock()
			c.controlChannel = tunnelWSConn
			c.controlMu.Unlock()
			c.logger.Info("control channel established successfully")

			c.config.TunnelStatus = fmt.Sprintf("Connected (%s)", c.config.Mode)

			g.start(c.poolMaintainer)
			g.start(func() { c.channelHandler(tunnelWSConn) })

			return
		}
	}
}

func (c *WsTransport) poolMaintainer() {
	g, ctx := c.gen, c.ctx

	// Stagger the initial pool fill instead of firing every dial at once - a
	// burst of ConnPoolSize near-simultaneous handshakes to the same host is a
	// distinctive connection pattern real browser traffic doesn't produce, and
	// a CDN that rate-limits the burst fails the whole pool at once instead of
	// one connection. Same stagger wsmux/wssmux already use.
	for i := 0; i < c.config.ConnPoolSize; i++ { //initial pool filling
		g.start(c.tunnelDialer)
		if i < c.config.ConnPoolSize-1 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(50+rand.Intn(200)) * time.Millisecond):
			}
		}
	}

	// factors
	a := 4
	b := 5
	x := 3
	y := 4.0

	if c.config.AggressivePool {
		c.logger.Info("aggressive pool management enabled")
		a = 1
		b = 2
		x = 0
		y = 0.75
	}

	tickerPool := time.NewTicker(time.Second * 1)
	defer tickerPool.Stop()

	tickerLoad := time.NewTicker(time.Second * 10)
	defer tickerLoad.Stop()

	newPoolSize := c.config.ConnPoolSize // intial value
	var poolConnectionsSum int32 = 0

	for {
		select {
		case <-ctx.Done():
			return

		case <-tickerPool.C:
			// Accumulate pool connections over time (every second)
			atomic.AddInt32(&poolConnectionsSum, atomic.LoadInt32(&c.poolConnections))

		case <-tickerLoad.C:
			// Calculate the loadConnections over the last 10 seconds
			loadConnections := (int(atomic.LoadInt32(&c.loadConnections)) + 9) / 10 // +9 for ceil-like logic
			atomic.StoreInt32(&c.loadConnections, 0)                                // Reset

			// Calculate the average pool connections over the last 10 seconds
			poolConnectionsAvg := (int(atomic.LoadInt32(&poolConnectionsSum)) + 9) / 10 // +9 for ceil-like logic
			atomic.StoreInt32(&poolConnectionsSum, 0)                                   // Reset

			// Dynamically adjust the pool size based on current connections
			if (loadConnections + a) > poolConnectionsAvg*b {
				c.logger.Debugf("increasing pool size: %d -> %d, avg pool conn: %d, avg load conn: %d", newPoolSize, newPoolSize+1, poolConnectionsAvg, loadConnections)
				newPoolSize++

				// Add a new connection to the pool
				g.start(c.tunnelDialer)
			} else if float64(loadConnections+x) < float64(poolConnectionsAvg)*y && newPoolSize > c.config.ConnPoolSize {
				c.logger.Debugf("decreasing pool size: %d -> %d, avg pool conn: %d, avg load conn: %d", newPoolSize, newPoolSize-1, poolConnectionsAvg, loadConnections)
				newPoolSize--

				// send a signal to controlFlow; a full channel must not keep
				// this worker (and so a restart) waiting once cancelled
				select {
				case c.controlFlow <- struct{}{}:
				case <-ctx.Done():
					return
				}
			}
		}
	}

}

// channelHandler drives one control channel. It takes the connection as an
// argument rather than reading c.controlChannel on every use: the field is
// shared with the dialer and Restart, so a handler whose connection has died
// must not touch a pointer that by then may hold something else.
func (c *WsTransport) channelHandler(conn *network.WebSocketConn) {
	g, ctx := c.gen, c.ctx
	msgChan := make(chan byte, 1000)

	// Goroutine to handle the blocking ReceiveBinaryString. A worker of the
	// generation, so Restart waits for it: it is woken by the generation closing
	// the socket, and its send gives way to cancellation instead of blocking on a
	// full msgChan that nothing drains any more.
	g.start(func() {
		for {
			select {
			case <-ctx.Done():
				return

			default:
				_, msg, err := conn.ReadMessage()
				if err != nil {
					// After cancellation the read fails because the generation
					// closed the socket, and a restart is already under way.
					if ctx.Err() == nil && c.cancel != nil {
						c.logger.Error("failed to read from channel connection. ", err)
						c.requestRestart()
					}
					return
				}

				// A zero-length binary frame (or padding-only payload) would
				// panic on msg[0] and take down this read goroutine; skip it.
				if len(msg) == 0 {
					continue
				}
				select {
				case msgChan <- msg[0]:
				case <-ctx.Done():
					return
				}
			}
		}
	})

	// Main loop to listen for context cancellation or received messages
	for {
		select {
		case <-ctx.Done():
			// Best effort and bounded: the peer may be gone, and an unbounded
			// write would keep this handler alive past cancellation.
			_ = conn.SetWriteDeadline(time.Now().Add(controlCloseWriteTimeout))
			_ = utils.WriteControlSignal(conn, utils.SG_Closed)
			return

		case msg := <-msgChan:
			switch msg {
			case utils.SG_Chan:
				atomic.AddInt32(&c.loadConnections, 1)
				select {
				case <-c.controlFlow: // Do nothing

				default:
					c.logger.Debug("channel signal received, initiating tunnel dialer")
					g.start(c.tunnelDialer)
				}

			case utils.SG_HB:
				c.logger.Debug("heartbeat signal received successfully")
				// send heartbeat back
				err := utils.WriteControlSignal(conn, utils.SG_HB)
				if err != nil {
					c.logger.Errorf("failed to send heartbeat: %v", err)
					c.requestRestart()
					return
				}
				c.logger.Trace("heartbeat signal sent successfully")

			case utils.SG_Closed:
				c.logger.Warn("control channel has been closed by the server")
				c.requestRestart()
				return

			default:
				c.logger.Errorf("unexpected response from channel: %v", msg)
				c.requestRestart()
				return
			}
		}
	}
}

func (c *WsTransport) tunnelDialer() {
	g, ctx := c.gen, c.ctx

	ep := c.nextEndpoint()
	c.logger.Debugf("initiating new websocket tunnel connection to address %s", ep.addr)

	// Dial to the tunnel server
	tunnelConn, err := network.WebSocketDialer(ctx, ep.addr, ep.edgeIP, network.NormalizeBasePath(c.config.Path)+"/tunnel", c.config.DialTimeOut, c.config.KeepAlive, c.config.Nodelay, c.config.Token, c.userAgent, c.config.Mode, 3, c.config.SO_RCVBUF, c.config.SO_SNDBUF, c.config.MSS, c.config.TLSVerify)
	if err != nil {
		c.logger.Errorf("tunnel server dialer: %v", err)

		return
	}

	// Own the socket before anything else touches it. If the generation was
	// stopped while this dial was in flight, own has already closed it.
	if !g.own(tunnelConn) {
		return
	}
	defer g.release(tunnelConn)

	// Increment active connections counter. The matching decrement runs exactly
	// once, whichever way this returns: the idle loop below used to leave it
	// incremented when the context was cancelled.
	atomic.AddInt32(&c.poolConnections, 1)
	counted := true
	leavePool := func() {
		if counted {
			counted = false
			atomic.AddInt32(&c.poolConnections, -1)
		}
	}
	defer leavePool()

	for {
		select {
		case <-ctx.Done():
			tunnelConn.Close()
			return
		default:
			// Blocked here until the server assigns this connection a
			// destination; ctx cannot wake the read, closing the socket (which
			// the generation does on stop) does.
			_, remoteAddrBytes, err := tunnelConn.ReadMessage()
			if err != nil {
				c.logger.Debugf("unable to get port from websocket connection %s: %v", tunnelConn.RemoteAddr().String(), err)
				tunnelConn.Close()
				return
			}

			if len(remoteAddrBytes) > 0 && remoteAddrBytes[0] == utils.SG_Ping {
				c.logger.Trace("ping received from the server")
				continue
			}

			// No longer an idle pool member
			leavePool()

			remoteAddr := string(remoteAddrBytes)

			// Extract the port from the received address
			port, resolvedAddr, err := network.ResolveRemoteAddr(remoteAddr)
			if err != nil {
				c.logger.Infof("failed to resolve remote port: %v", err)
				tunnelConn.Close() // Close the connection on error
				return
			}

			c.localDialer(tunnelConn, resolvedAddr, port)
			return
		}
	}
}

func (c *WsTransport) localDialer(tunnelCon *network.WebSocketConn, remoteAddr string, port int) {
	ctx := c.ctx // this worker's generation, read once
	var sendBuf, recvBuf int

	if strings.Contains(remoteAddr, "127.0.0.1") {
		// Use 32 KB for localhost
		sendBuf = 32 * 1024
		recvBuf = 32 * 1024
	} else {
		// Use your custom buffer sizes
		sendBuf = 0
		recvBuf = 0
	}

	localConnection, err := network.TcpDialer(ctx, remoteAddr, "", c.config.DialTimeOut, c.config.KeepAlive, true, 1, recvBuf, sendBuf, 0)
	if err != nil {
		c.logger.Errorf("local dialer: %v", err)
		tunnelCon.Close()
		return
	}
	c.logger.Debugf("connected to local address %s successfully", remoteAddr)

	handlers.WSConnectionHandler(ctx, false, tunnelCon, localConnection, c.logger, c.usageMonitor, int(port), c.config.Sniffer)
}
