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
	// pendingDials counts tunnel dials in flight (not yet established), so the
	// refill does not re-dial a connection that is still handshaking.
	pendingDials int32
	// owedConns is how many pool connections died idle (or failed to dial) and
	// have not been replaced yet. Only these are refilled: a connection that is
	// consumed by a flow is replaced by the server's own request, and refilling
	// it here too would grow the pool by one per flow.
	owedConns int32
	// lastShrink is when a pool shrink last queued a controlFlow token; see
	// swallowShrinkToken.
	lastShrink int64
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
	atomic.StoreInt32(&c.pendingDials, 0)
	atomic.StoreInt32(&c.owedConns, 0)
	atomic.StoreInt64(&c.lastShrink, 0)
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

			// Replace idle pool connections that died on their own (a CDN
			// max-age or idle reset). Nothing else rebuilt them: the initial fill
			// runs once and the sizing below only grows on load, so after an idle
			// spell the pool could sit empty until traffic forced growth. Only
			// connections that are owed are refilled (see owedConns), gently, and
			// never past the configured floor.
			for i, n := 0, c.refillOwed(); i < n; i++ {
				g.start(c.tunnelDialer)
			}

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
				markShrink(&c.lastShrink)

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
// argument rather than reading c.controlChannel on every use: a handler whose
// connection has died must not touch the shared pointer that by then may hold
// its replacement.
func (c *WsTransport) channelHandler(conn *network.WebSocketConn) {
	g, ctx := c.gen, c.ctx
	msgChan := make(chan byte, 1000)

	// One handler owns one connection and its reader; they end together.
	//   connDone    closed when the reader goroutine exits (handler <- reader).
	//   hctx        cancelled when the handler returns (handler -> reader).
	//   handlerDone closed once both are gone; reconnectControl waits on it so
	//               a replacement handler never overlaps this one.
	connDone := make(chan struct{})
	handlerDone := make(chan struct{})
	hctx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
		// Wake a ReadMessage blocked on the (now abandoned) connection, then
		// join the reader: it cannot block again because its only other wait,
		// the msgChan send, also selects on hctx.
		_ = conn.SetReadDeadline(time.Now())
		<-connDone
		close(handlerDone)
	}()

	// Goroutine to handle the blocking ReceiveBinaryString
	go func() {
		defer close(connDone)
		for hctx.Err() == nil {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				if hctx.Err() != nil {
					// The handler already returned and owns the reconnect
					// decision; reconnecting here too would double it.
					return
				}
				c.logger.Warn("control channel read failed. ", err)
				// Not fatal: the control channel carries only heartbeats
				// and new-connection requests, so it is re-dialled without
				// touching the pool or the flows running on it.
				g.start(func() { c.reconnectControl(conn, handlerDone) })
				return
			}
			// A zero-length binary frame (or padding-only payload) would
			// panic on msg[0] and take down this read goroutine; skip it.
			if len(msg) == 0 {
				continue
			}
			select {
			case msgChan <- msg[0]:
			case <-hctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			// Bounded: the peer may be gone, and an unbounded write would keep
			// this handler (and so its reader) alive past cancellation.
			_ = conn.SetWriteDeadline(time.Now().Add(controlCloseWriteTimeout))
			_ = utils.WriteControlSignal(conn, utils.SG_Closed)
			return

		case <-connDone:
			// Reader exited and has already scheduled the reconnect.
			return

		case msg := <-msgChan:
			switch msg {
			case utils.SG_Chan:
				atomic.AddInt32(&c.loadConnections, 1)
				if c.swallowChanSignal() {
					c.logger.Debug("channel signal absorbed by a recent pool shrink")
				} else {
					c.logger.Debug("channel signal received, initiating tunnel dialer")
					g.start(c.tunnelDialer)
				}

			case utils.SG_HB:
				c.logger.Debug("heartbeat received successfully")
				err := utils.WriteControlSignal(conn, utils.SG_HB)
				if err != nil {
					c.logger.Warnf("failed to send heartbeat: %v", err)
					g.start(func() { c.reconnectControl(conn, handlerDone) })
					return
				}
				c.logger.Trace("heartbeat signal sent successfully")

			case utils.SG_Closed:
				c.logger.Warn("control channel has been closed by the server")
				c.requestRestart()
				return

			default:
				c.logger.Errorf("unexpected response from control channel: %v", msg)
				c.requestRestart()
				return
			}

		}
	}
}

// swallowChanSignal reports whether this SG_Chan should be ignored because a
// recent shrink queued a token for it (see swallowShrinkToken).
func (c *WsTransport) swallowChanSignal() bool {
	return swallowShrinkToken(c.controlFlow, &c.lastShrink)
}

// reconnectControl re-dials the control channel after it dropped, deliberately
// without cancelling ctx. The pool connections and every flow on them are
// independent of the control channel, which carries nothing but heartbeats and
// new-connection requests - so a CDN resetting the control connection (the
// oldest, longest-lived one, and therefore the first to hit a max-age limit)
// becomes a brief pause in *new* connection setup instead of a dropped tunnel.
//
// Restart, which tears everything down, stays as the fallback for a control
// channel that cannot be re-established at all.
//
// oldDone is closed when the handler that owned old has fully returned (reader
// included). The replacement is dialled and started only after that, so a stale
// handler can never consume from, or race, its successor.
func (c *WsTransport) reconnectControl(old *network.WebSocketConn, oldDone <-chan struct{}) {
	g, ctx := c.gen, c.ctx

	c.controlMu.Lock()
	if c.controlChannel != old {
		// Another goroutine already handled this drop, or a restart is under way.
		c.controlMu.Unlock()
		return
	}
	c.controlChannel = nil
	c.controlMu.Unlock()
	old.Close()
	g.release(old)

	c.config.TunnelStatus = fmt.Sprintf("Reconnecting (%s)", c.config.Mode)
	c.logger.Warn("control channel dropped, re-dialing without tearing down the pool")

	// old.Close() above makes the old handler's read and write fail, so this
	// wait is short; it involves no I/O and holds no lock.
	select {
	case <-oldDone:
	case <-ctx.Done():
		return
	}

	deadline := time.Now().Add(controlReconnectWindow)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// retry=1: the dialer's own retry loop would spend most of the window
		// backing off against one endpoint, and each failure is worth logging.
		ep := c.nextEndpoint()
		conn, err := network.WebSocketDialer(ctx, ep.addr, ep.edgeIP, network.NormalizeBasePath(c.config.Path)+"/channel", c.config.DialTimeOut, c.config.KeepAlive, true, c.config.Token, c.userAgent, c.config.Mode, 1, 0, 0, 0, c.config.TLSVerify)
		if err == nil {
			if !g.own(conn) {
				return
			}
			c.controlMu.Lock()
			c.controlChannel = conn
			c.controlMu.Unlock()

			c.config.TunnelStatus = fmt.Sprintf("Connected (%s)", c.config.Mode)
			c.logger.Info("control channel re-established, pool preserved")

			// Only the handler restarts here - poolMaintainer is still running
			// under the same context and starting a second one would double the
			// pool management.
			g.start(func() { c.channelHandler(conn) })
			return
		}

		c.logger.Errorf("control channel re-dial: %v (endpoint %s)", err, ep.addr)

		if time.Now().After(deadline) {
			c.logger.Warn("control channel could not be re-established within the grace window, falling back to a full restart")
			// This worker is joined by the restart it asks for, so it cannot
			// run it itself.
			c.requestRestart()
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(c.config.RetryInterval):
		}
	}
}

// refillOwed settles the owed-connection debt (see owedConns) against the pool's
// current deficit and returns how many dials to launch this tick. With no deficit
// the debt is forgiven: the pool is already at its floor (a rotation replacement
// arrived first), and keeping it would dial a surplus connection the moment a
// flow consumes one.
func (c *WsTransport) refillOwed() int {
	deficit := poolRefillCount(c.config.ConnPoolSize, int(atomic.LoadInt32(&c.poolConnections)), int(atomic.LoadInt32(&c.pendingDials)), poolRefillPerTick)
	owed := int(atomic.LoadInt32(&c.owedConns))
	if deficit <= 0 {
		atomic.StoreInt32(&c.owedConns, 0)
		return 0
	}
	if owed <= 0 {
		return 0
	}
	n := deficit
	if owed < n {
		n = owed
	}
	atomic.AddInt32(&c.owedConns, int32(-n))
	return n
}

// owe records one pool connection that has to be replaced, capped at the floor
// so a long outage cannot build up a debt that would burst-dial on recovery.
func (c *WsTransport) owe() {
	if int(atomic.AddInt32(&c.owedConns, 1)) > c.config.ConnPoolSize {
		atomic.StoreInt32(&c.owedConns, int32(c.config.ConnPoolSize))
	}
}

func (c *WsTransport) tunnelDialer() {
	g, ctx := c.gen, c.ctx

	// In flight only while dialing; handed to poolConnections once established.
	atomic.AddInt32(&c.pendingDials, 1)

	ep := c.nextEndpoint()
	c.logger.Debugf("initiating new websocket tunnel connection to address %s", ep.addr)

	// Dial to the tunnel server
	tunnelConn, err := network.WebSocketDialer(ctx, ep.addr, ep.edgeIP, network.NormalizeBasePath(c.config.Path)+"/tunnel", c.config.DialTimeOut, c.config.KeepAlive, c.config.Nodelay, c.config.Token, c.userAgent, c.config.Mode, 3, c.config.SO_RCVBUF, c.config.SO_SNDBUF, c.config.MSS, c.config.TLSVerify)
	if err != nil {
		atomic.AddInt32(&c.pendingDials, -1)
		c.logger.Errorf("tunnel server dialer: %v", err)
		if ctx.Err() == nil {
			c.owe() // retried by the refill, a second at a time
		}

		return
	}

	// Own the socket before anything else touches it. If the generation was
	// stopped while this dial was in flight, own has already closed it.
	if !g.own(tunnelConn) {
		atomic.AddInt32(&c.pendingDials, -1)
		return
	}
	defer g.release(tunnelConn)

	// Increment active connections counter. The matching decrement runs exactly
	// once, whichever way this returns: the idle loop below used to leave it
	// incremented when the context was cancelled.
	atomic.AddInt32(&c.poolConnections, 1)
	atomic.AddInt32(&c.pendingDials, -1)
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
				if ctx.Err() == nil {
					c.owe() // an idle pool connection died; the refill replaces it
				}
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
