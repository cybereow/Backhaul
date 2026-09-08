package transport

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/musix/backhaul/config"
	"github.com/musix/backhaul/internal/utils"
	"github.com/musix/backhaul/internal/utils/handlers"
	"github.com/musix/backhaul/internal/utils/network"
	"github.com/musix/backhaul/internal/utils/striping"
	"github.com/musix/backhaul/internal/web"
	"github.com/xtaci/smux"

	"github.com/sirupsen/logrus"
)

type WsMuxTransport struct {
	config         *WsMuxConfig
	smuxConfig     *smux.Config
	parentctx      context.Context
	ctx            context.Context
	cancel         context.CancelFunc
	logger         *logrus.Logger
	controlChannel *network.WebSocketConn
	// controlMu guards controlChannel: it is now swapped in place on a
	// reconnect, so the dialer, a dying channelHandler and Restart can all
	// touch it at once.
	controlMu       sync.Mutex
	usageMonitor    *web.Usage
	restartMutex    sync.Mutex
	poolConnections int32
	loadConnections int32
	controlFlow     chan struct{}
	// userAgent is picked once per process instead of per dial, so a single
	// client identity doesn't show up with a different browser signature on
	// every pool connection/reconnect - a pattern no real browser produces.
	userAgent string

	// stripeGroups collects legs of a striped flow (see package striping)
	// until all of them have arrived, keyed by the groupID the server
	// tagged them with.
	stripeGroupsMu sync.Mutex
	stripeGroups   map[uint32]*stripeGroup

	// endpoints is the set of tunnel entry points (one origin behind one or
	// more CDNs/domains). Pool and control dials round-robin across it via
	// dialSeq so traffic spreads over every CDN at once instead of one.
	endpoints []wsEndpoint
	dialSeq   int32

	promotableFlowsMu sync.Mutex
	promotableFlows   map[uint64]*handlers.PumpSwapper
}

// wsEndpoint is a single tunnel entry point: the domain dialed (which also
// becomes the TLS SNI / WebSocket Host) and an optional edge IP to connect to
// instead of resolving the domain.
type wsEndpoint struct {
	addr   string
	edgeIP string
}

// buildEndpoints turns the multi-endpoint config (remote_addrs/edge_ips) into a
// list, falling back to the single remote_addr/edge_ip so existing configs are
// unchanged.
func buildEndpoints(addrs, edges []string, addr, edge string) []wsEndpoint {
	if len(addrs) == 0 {
		return []wsEndpoint{{addr: addr, edgeIP: edge}}
	}
	eps := make([]wsEndpoint, len(addrs))
	for i, a := range addrs {
		e := ""
		if i < len(edges) {
			e = edges[i]
		}
		eps[i] = wsEndpoint{addr: a, edgeIP: e}
	}
	return eps
}

// nextEndpoint returns the next entry point round-robin. With a single
// endpoint it's a plain read; with several, each dial lands on a different
// CDN/domain so the pool aggregates them.
func (c *WsMuxTransport) nextEndpoint() wsEndpoint {
	if len(c.endpoints) == 1 {
		return c.endpoints[0]
	}
	i := atomic.AddInt32(&c.dialSeq, 1)
	return c.endpoints[int(uint32(i))%len(c.endpoints)]
}

// stripeGroup accumulates the legs the server opened for one logical
// connection (one per stripe.Factor) until all of them have shown up.
type stripeGroup struct {
	streams    []*smux.Stream
	remaining  int
	remoteAddr string
	timer      *time.Timer
	parity     uint8 // FEC parity legs among streams; 0 = plain striping
}
type WsMuxConfig struct {
	RemoteAddr           string
	RemoteAddrs          []string
	EdgeIPs              []string
	Token                string
	SnifferLog           string
	TunnelStatus         string
	Nodelay              bool
	Sniffer              bool
	KeepAlive            time.Duration
	RetryInterval        time.Duration
	DialTimeOut          time.Duration
	MuxVersion           int
	MaxFrameSize         int
	MaxReceiveBuffer     int
	MaxStreamBuffer      int
	ConnPoolSize         int
	WebPort              int
	Mode                 config.TransportType
	AggressivePool       bool
	EdgeIP               string
	Path                 string
	MuxKeepaliveDisabled bool
	StripeFactor         int
	StripeParity         int
	SO_RCVBUF            int
	SO_SNDBUF            int
	MSS                  int
	TLSVerify            bool // wssmux: verify the server certificate during the TLS handshake
}

func NewWSMuxClient(parentCtx context.Context, config *WsMuxConfig, logger *logrus.Logger) *WsMuxTransport {
	// Create a derived context from the parent context
	ctx, cancel := context.WithCancel(parentCtx)

	// Initialize the TcpTransport struct
	client := &WsMuxTransport{
		smuxConfig: &smux.Config{
			Version:           config.MuxVersion,
			KeepAliveDisabled: config.MuxKeepaliveDisabled,
			KeepAliveInterval: 20 * time.Second,
			KeepAliveTimeout:  40 * time.Second,
			MaxFrameSize:      config.MaxFrameSize,
			MaxReceiveBuffer:  config.MaxReceiveBuffer,
			MaxStreamBuffer:   config.MaxStreamBuffer,
		},
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
		stripeGroups:    make(map[uint32]*stripeGroup),
		promotableFlows: make(map[uint64]*handlers.PumpSwapper),
	}

	client.endpoints = buildEndpoints(config.RemoteAddrs, config.EdgeIPs, config.RemoteAddr, config.EdgeIP)

	return client
}

func (c *WsMuxTransport) Start() {
	if c.config.WebPort > 0 {
		go c.usageMonitor.Monitor()
	}

	c.config.TunnelStatus = fmt.Sprintf("Disconnected (%s)", c.config.Mode)

	go c.channelDialer()
}

func (c *WsMuxTransport) Restart() {
	if !c.restartMutex.TryLock() {
		c.logger.Warn("client is already restarting")
		return
	}
	defer c.restartMutex.Unlock()

	c.logger.Info("restarting client...")

	// for removing timeout logs
	level := c.logger.Level
	c.logger.SetLevel(logrus.FatalLevel)

	if c.cancel != nil {
		c.cancel()
	}

	// close control channel connection
	c.controlMu.Lock()
	if c.controlChannel != nil {
		c.controlChannel.Close()
	}
	c.controlChannel = nil
	c.controlMu.Unlock()

	time.Sleep(2 * time.Second)

	ctx, cancel := context.WithCancel(c.parentctx)
	c.ctx = ctx
	c.cancel = cancel

	// Re-initialize variables
	c.usageMonitor = web.NewDataStore(fmt.Sprintf(":%v", c.config.WebPort), ctx, c.config.SnifferLog, c.config.Sniffer, &c.config.TunnelStatus, c.logger)
	c.config.TunnelStatus = ""
	c.poolConnections = 0
	c.loadConnections = 0
	c.controlFlow = make(chan struct{}, 100)

	c.stripeGroupsMu.Lock()
	for _, g := range c.stripeGroups {
		g.timer.Stop()
		for _, st := range g.streams {
			if st != nil {
				st.Close()
			}
		}
	}
	c.stripeGroups = make(map[uint32]*stripeGroup)
	c.stripeGroupsMu.Unlock()

	// set the log level again
	c.logger.SetLevel(level)

	go c.Start()
}

func (c *WsMuxTransport) channelDialer() {
	c.logger.Infof("attempting to establish a new %s control channel connection", c.config.Mode)

	for {
		select {
		case <-c.ctx.Done():
			return
		default:

			ep := c.nextEndpoint()
			tunnelWSConn, err := network.WebSocketDialer(c.ctx, ep.addr, ep.edgeIP, network.NormalizeBasePath(c.config.Path)+"/channel", c.config.DialTimeOut, c.config.KeepAlive, true, c.config.Token, c.userAgent, c.config.Mode, 3, 0, 0, 0, c.config.TLSVerify)
			if err != nil {
				c.logger.Errorf("control channel dialer: %v", err)
				time.Sleep(c.config.RetryInterval)
				continue
			}
			c.controlMu.Lock()
			c.controlChannel = tunnelWSConn
			c.controlMu.Unlock()
			c.logger.Info("control channel established successfully")

			c.config.TunnelStatus = fmt.Sprintf("Connected (%s)", c.config.Mode)

			go c.poolMaintainer()
			go c.channelHandler(tunnelWSConn)

			return
		}
	}
}

func (c *WsMuxTransport) poolMaintainer() {
	// Stagger the initial pool fill instead of firing every dial at once -
	// a burst of ConnPoolSize near-simultaneous TLS handshakes to the same
	// host is a distinctive connection pattern real browser traffic doesn't
	// produce.
	for i := 0; i < c.config.ConnPoolSize; i++ { //initial pool filling
		go c.tunnelDialer()
		if i < c.config.ConnPoolSize-1 {
			select {
			case <-c.ctx.Done():
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
		case <-c.ctx.Done():
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
				go c.tunnelDialer()
			} else if float64(loadConnections+x) < float64(poolConnectionsAvg)*y && newPoolSize > c.config.ConnPoolSize {
				c.logger.Debugf("decreasing pool size: %d -> %d, avg pool conn: %d, avg load conn: %d", newPoolSize, newPoolSize-1, poolConnectionsAvg, loadConnections)
				newPoolSize--

				// send a signal to controlFlow
				c.controlFlow <- struct{}{}
			}
		}
	}

}

// channelHandler drives one control channel. It takes the connection as an
// argument rather than reading c.controlChannel on every use: a handler whose
// connection has died must not touch the shared pointer that by then may hold
// its replacement.
func (c *WsMuxTransport) channelHandler(conn *network.WebSocketConn) {
	msgChan := make(chan byte, 1000)

	// Goroutine to handle the blocking ReceiveBinaryString
	go func() {
		for {
			select {
			case <-c.ctx.Done():
				return

			default:
				_, msg, err := conn.ReadMessage()
				if err != nil {
					c.logger.Warn("control channel read failed. ", err)
					// Not fatal: the control channel carries only heartbeats
					// and new-connection requests, so it is re-dialled without
					// touching the pool or the flows running on it.
					go c.reconnectControl(conn)
					return
				}
				// A zero-length binary frame (or padding-only payload) would
				// panic on msg[0] and take down this read goroutine; skip it.
				if len(msg) == 0 {
					continue
				}
				msgChan <- msg[0]
			}
		}
	}()

	for {
		select {
		case <-c.ctx.Done():
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
					go c.tunnelDialer()
				}

			case utils.SG_HB:
				c.logger.Debug("heartbeat received successfully")
				err := utils.WriteControlSignal(conn, utils.SG_HB)
				if err != nil {
					c.logger.Warnf("failed to send heartbeat: %v", err)
					go c.reconnectControl(conn)
					return
				}
				c.logger.Trace("heartbeat signal sent successfully")

			case utils.SG_Closed:
				c.logger.Warn("control channel has been closed by the server")
				go c.Restart()
				return

			default:
				c.logger.Errorf("unexpected response from control channel: %v", msg)
				go c.Restart()
				return
			}

		}
	}
}

// controlReconnectWindow bounds how long a dropped control channel is re-dialled
// while the pool is kept alive. It matches the server's grace window: past that
// the server gives up on the reattach and rebuilds its own side anyway, so there
// is nothing left to preserve and a full restart is the honest fallback.
const controlReconnectWindow = 30 * time.Second

// reconnectControl re-dials the control channel after it dropped, deliberately
// without cancelling ctx. The pool connections and every flow on them are
// independent of the control channel, which carries nothing but heartbeats and
// new-connection requests - so a CDN resetting the control connection (the
// oldest, longest-lived one, and therefore the first to hit a max-age limit)
// becomes a brief pause in *new* connection setup instead of a dropped tunnel.
//
// Restart, which tears everything down, stays as the fallback for a control
// channel that cannot be re-established at all.
func (c *WsMuxTransport) reconnectControl(old *network.WebSocketConn) {
	c.controlMu.Lock()
	if c.controlChannel != old {
		// Another goroutine already handled this drop, or a restart is under way.
		c.controlMu.Unlock()
		return
	}
	c.controlChannel = nil
	c.controlMu.Unlock()
	old.Close()

	c.config.TunnelStatus = fmt.Sprintf("Reconnecting (%s)", c.config.Mode)
	c.logger.Warn("control channel dropped, re-dialing without tearing down the pool")

	deadline := time.Now().Add(controlReconnectWindow)
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		// retry=1: the dialer's own retry loop would spend most of the window
		// backing off against one endpoint, and each failure is worth logging.
		ep := c.nextEndpoint()
		conn, err := network.WebSocketDialer(c.ctx, ep.addr, ep.edgeIP, network.NormalizeBasePath(c.config.Path)+"/channel", c.config.DialTimeOut, c.config.KeepAlive, true, c.config.Token, c.userAgent, c.config.Mode, 1, 0, 0, 0, c.config.TLSVerify)
		if err == nil {
			c.controlMu.Lock()
			c.controlChannel = conn
			c.controlMu.Unlock()

			c.config.TunnelStatus = fmt.Sprintf("Connected (%s)", c.config.Mode)
			c.logger.Info("control channel re-established, pool preserved")

			// Only the handler restarts here - poolMaintainer is still running
			// under the same context and starting a second one would double the
			// pool management.
			go c.channelHandler(conn)
			return
		}

		c.logger.Errorf("control channel re-dial: %v", err)

		if time.Now().After(deadline) {
			c.logger.Warn("control channel could not be re-established within the grace window, falling back to a full restart")
			c.Restart()
			return
		}

		select {
		case <-c.ctx.Done():
			return
		case <-time.After(c.config.RetryInterval):
		}
	}
}

func (c *WsMuxTransport) tunnelDialer() {
	ep := c.nextEndpoint()
	c.logger.Debugf("initiating new %s tunnel connection to address %s", c.config.Mode, ep.addr)

	// Dial to the tunnel server
	tunnelWSConn, err := network.WebSocketDialer(c.ctx, ep.addr, ep.edgeIP, network.NormalizeBasePath(c.config.Path)+"/tunnel", c.config.DialTimeOut, c.config.KeepAlive, c.config.Nodelay, c.config.Token, c.userAgent, c.config.Mode, 3, c.config.SO_RCVBUF, c.config.SO_SNDBUF, c.config.MSS, c.config.TLSVerify)
	if err != nil {
		c.logger.Errorf("tunnel server dialer: %v", err)

		return
	}

	// Increment active connections counter
	atomic.AddInt32(&c.poolConnections, 1)

	c.handleSession(tunnelWSConn)
}

func (c *WsMuxTransport) handleSession(tunnelConn *network.WebSocketConn) {
	defer func() {
		atomic.AddInt32(&c.poolConnections, -1)
	}()

	// SMUX server
	session, err := smux.Server(tunnelConn.NetConn(), c.smuxConfig)
	if err != nil {
		c.logger.Errorf("failed to create mux session: %v", err)
		return
	}

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			stream, err := session.AcceptStream()
			if err != nil {
				c.logger.Debug("session is closed: ", err)
				session.Close()
				return
			}

			if c.config.MuxVersion >= 2 {
				kind, err := utils.ReadFlowKind(stream)
				if err != nil {
					c.logger.Errorf("unable to read flow kind from stream connection %s: %v", tunnelConn.RemoteAddr().String(), err)
					stream.Close()
					continue
				}
				if kind == utils.FlowPlain {
					flowID, remoteAddr, err := utils.ReceiveFlowPlain(stream)
					if err != nil {
						c.logger.Errorf("unable to read plain flow header: %v", err)
						stream.Close()
						continue
					}
					// A non-zero flowID marks the flow as promotable: run it
					// through the promotable pump so a later FlowPromote can
					// migrate it mid-stream. flowID 0 is a plain flow.
					if flowID != 0 {
						go c.localDialerPlain(stream, flowID, remoteAddr)
					} else {
						go c.localDialer(stream, remoteAddr)
					}
					continue
				} else if kind == utils.FlowStriped {
					go c.handleStripedStream(stream)
					continue
				} else if kind == utils.FlowPromote {
					go c.handlePromoteStream(stream)
					continue
				} else if kind == utils.FlowUDP {
					remoteAddr, err := utils.ReceiveFlowUDP(stream)
					if err != nil {
						c.logger.Errorf("unable to read udp flow header: %v", err)
						stream.Close()
						continue
					}
					go c.localDialerUDP(stream, remoteAddr)
					continue
				} else if kind == utils.FlowPing {
					go c.handlePingStream(stream)
					continue
				}
			} else {
				if c.config.StripeFactor > 1 {
					go c.handleStripedStream(stream)
					continue
				}
			}

			remoteAddr, err := utils.ReceiveBinaryString(stream)
			if err != nil {
				c.logger.Errorf("unable to get port from stream connection %s: %v", tunnelConn.RemoteAddr().String(), err)
				stream.Close()
				continue
			}

			go c.localDialer(stream, remoteAddr)
		}
	}
}

// handleStripedStream reads the stripe header off a newly accepted stream
// and files it under its group. Once every leg the server promised (total)
// has shown up, the group is assembled into a single striping.Conn and
// handed to localDialer exactly like a plain stream would be.
func (c *WsMuxTransport) handleStripedStream(stream *smux.Stream) {
	groupID, index, total, parity, remoteAddr, err := utils.ReceiveStripeHeader(stream)
	if err != nil {
		c.logger.Errorf("failed to read stripe header: %v", err)
		stream.Close()
		return
	}
	if total == 0 || int(index) >= int(total) || int(parity) >= int(total) {
		c.logger.Errorf("invalid stripe header: index=%d total=%d parity=%d", index, total, parity)
		stream.Close()
		return
	}

	c.stripeGroupsMu.Lock()
	g, ok := c.stripeGroups[groupID]
	if !ok {
		g = &stripeGroup{
			streams:    make([]*smux.Stream, total),
			remaining:  int(total),
			remoteAddr: remoteAddr,
			parity:     parity,
		}
		g.timer = time.AfterFunc(10*time.Second, func() {
			c.abortStripeGroup(groupID)
		})
		c.stripeGroups[groupID] = g
	}

	var complete bool
	if g.streams[index] != nil {
		c.logger.Warnf("duplicate stripe leg %d for group %d, closing", index, groupID)
		c.stripeGroupsMu.Unlock()
		stream.Close()
		return
	}
	g.streams[index] = stream
	g.remaining--
	complete = g.remaining == 0
	if complete {
		delete(c.stripeGroups, groupID)
	}
	c.stripeGroupsMu.Unlock()

	if !complete {
		return
	}

	g.timer.Stop()
	conns := make([]net.Conn, len(g.streams))
	for i, st := range g.streams {
		conns[i] = st
	}
	if g.parity > 0 {
		dataShards := len(conns) - int(g.parity)
		fecConn, err := striping.NewFEC(conns, striping.DefaultChunkSize, dataShards, int(g.parity))
		if err != nil {
			c.logger.Errorf("failed to build FEC striped conn: %v", err)
			for _, cn := range conns {
				cn.Close()
			}
			return
		}
		go c.localDialer(fecConn, g.remoteAddr)
		return
	}
	go c.localDialer(striping.New(conns, striping.DefaultChunkSize), g.remoteAddr)
}

// abortStripeGroup gives up on a group that never received all of its legs
// within the timeout (e.g. one pool connection died mid-handshake) and
// closes whatever legs did arrive.
func (c *WsMuxTransport) abortStripeGroup(groupID uint32) {
	c.stripeGroupsMu.Lock()
	g, ok := c.stripeGroups[groupID]
	if ok {
		delete(c.stripeGroups, groupID)
	}
	c.stripeGroupsMu.Unlock()

	if !ok {
		return
	}
	c.logger.Warnf("timed out waiting for stripe legs of group %d, aborting", groupID)
	for _, st := range g.streams {
		if st != nil {
			st.Close()
		}
	}
}

func (c *WsMuxTransport) localDialer(stream net.Conn, remoteAddr string) {
	// Extract the port from the received address
	port, resolvedAddr, err := network.ResolveRemoteAddr(remoteAddr)
	if err != nil {
		c.logger.Infof("failed to resolve remote port: %v", err)
		stream.Close()
		return
	}

	var sendBuf, recvBuf int

	if strings.Contains(resolvedAddr, "127.0.0.1") {
		// Use 32 KB for localhost
		sendBuf = 32 * 1024
		recvBuf = 32 * 1024
	} else {
		// Use your custom buffer sizes
		sendBuf = 0
		recvBuf = 0
	}

	localConnection, err := network.TcpDialer(c.ctx, resolvedAddr, "", c.config.DialTimeOut, c.config.KeepAlive, true, 1, recvBuf, sendBuf, 0)
	if err != nil {
		c.logger.Errorf("local dialer: %v", err)
		stream.Close()
		return
	}

	c.logger.Debugf("connected to local address %s successfully", remoteAddr)

	handlers.TCPConnectionHandler(c.ctx, false, stream, localConnection, c.logger, c.usageMonitor, int(port), c.config.Sniffer)
}

// handlePingStream answers a server RTT probe: it reads the 8-byte nonce and
// writes it straight back, so the server can time the session round-trip and
// steer striped-leg selection toward the lowest-latency CDN. The stream carries
// no user data and is torn down as soon as the echo is sent; a deadline keeps a
// dead probe from leaking a goroutine.
func (c *WsMuxTransport) handlePingStream(stream net.Conn) {
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(10 * time.Second))
	nonce, err := utils.ReceiveFlowPing(stream)
	if err != nil {
		c.logger.Tracef("ping probe read failed: %v", err)
		return
	}
	if err := utils.EchoFlowPing(stream, nonce); err != nil {
		c.logger.Tracef("ping probe echo failed: %v", err)
	}
}

// localDialerUDP handles a stream the server tagged as a UDP flow: it dials the
// local UDP target and shuttles length-framed datagrams between the stream and
// the socket. The framing (utils.WriteUDPFrame/ReadUDPFrame) carries no
// timestamp - smux already provides reliability and ordering - so there is no
// congestion/churn machinery to false-positive on clock skew and cut a live
// handshake like IKE. Unlike UDPDialer it never calls Fatalf (a bad target must
// not kill the whole tunnel) and tears both directions down when either side
// ends, so nothing leaks.
func (c *WsMuxTransport) localDialerUDP(stream net.Conn, remoteAddr string) {
	port, resolvedAddr, err := network.ResolveRemoteAddr(remoteAddr)
	if err != nil {
		c.logger.Infof("failed to resolve remote udp port: %v", err)
		stream.Close()
		return
	}

	remoteUDPAddr, err := net.ResolveUDPAddr("udp", resolvedAddr)
	if err != nil {
		c.logger.Errorf("failed to resolve remote udp address %s: %v", resolvedAddr, err)
		stream.Close()
		return
	}

	remoteConn, err := net.DialUDP("udp", nil, remoteUDPAddr)
	if err != nil {
		c.logger.Errorf("failed to dial remote udp address %s: %v", resolvedAddr, err)
		stream.Close()
		return
	}

	c.logger.Debugf("connected to local udp address %s successfully", remoteAddr)

	done := make(chan struct{})
	go func() {
		// stream -> local udp; returns when the stream is closed/EOF
		buf := make([]byte, 64*1024)
		for {
			n, err := utils.ReadUDPFrame(stream, buf)
			if err != nil {
				break
			}
			if _, err := remoteConn.Write(buf[:n]); err != nil {
				c.logger.Debugf("udp local write failed: %v", err)
				break
			}
			if c.config.Sniffer {
				c.usageMonitor.AddOrUpdatePort(int(port), uint64(n))
			}
		}
		remoteConn.Close() // unblock the udp read below
		close(done)
	}()

	// local udp -> stream; returns when the udp socket is closed or errors
	buf := make([]byte, 64*1024)
	for {
		n, err := remoteConn.Read(buf)
		if err != nil {
			break
		}
		if err := utils.WriteUDPFrame(stream, buf[:n]); err != nil {
			c.logger.Debugf("udp stream write failed: %v", err)
			break
		}
		if c.config.Sniffer {
			c.usageMonitor.AddOrUpdatePort(int(port), uint64(n))
		}
	}
	stream.Close() // unblock the stream read above
	<-done
}

func (c *WsMuxTransport) localDialerPlain(stream *smux.Stream, flowID uint64, remoteAddr string) {
	port, resolvedAddr, err := network.ResolveRemoteAddr(remoteAddr)
	if err != nil {
		c.logger.Infof("failed to resolve remote port: %v", err)
		stream.Close()
		return
	}

	var sendBuf, recvBuf int
	if strings.Contains(resolvedAddr, "127.0.0.1") {
		sendBuf, recvBuf = 32*1024, 32*1024 // localhost
	} else if c.config.AggressivePool {
		sendBuf = 32 * 1024
		recvBuf = 32 * 1024
	}

	localConnection, err := network.TcpDialer(c.ctx, resolvedAddr, "", c.config.DialTimeOut, c.config.KeepAlive, true, 1, recvBuf, sendBuf, 0)
	if err != nil {
		c.logger.Errorf("local dialer: %v", err)
		stream.Close()
		return
	}

	swapper := handlers.PromotablePump(c.ctx, false, localConnection, stream, c.logger, c.usageMonitor, port, c.config.Sniffer)
	if swapper == nil {
		return
	}

	c.promotableFlowsMu.Lock()
	c.promotableFlows[flowID] = swapper
	c.promotableFlowsMu.Unlock()

	<-swapper.DoneWait()

	c.promotableFlowsMu.Lock()
	delete(c.promotableFlows, flowID)
	c.promotableFlowsMu.Unlock()
}

func (c *WsMuxTransport) handlePromoteStream(stream *smux.Stream) {
	flowID, groupID, index, total, parity, err := utils.ReceiveFlowPromote(stream)
	if err != nil {
		c.logger.Errorf("failed to read promote header: %v", err)
		stream.Close()
		return
	}

	c.promotableFlowsMu.Lock()
	swapper, ok := c.promotableFlows[flowID]
	c.promotableFlowsMu.Unlock()

	if !ok {
		c.logger.Errorf("promotion leg for unknown flow %d", flowID)
		stream.Close()
		return
	}

	c.stripeGroupsMu.Lock()
	g, ok := c.stripeGroups[groupID]
	if !ok {
		g = &stripeGroup{
			streams:    make([]*smux.Stream, total),
			remaining:  int(total),
			remoteAddr: "", // not needed for promotion
			parity:     parity,
		}
		g.timer = time.AfterFunc(10*time.Second, func() {
			c.abortStripeGroup(groupID)
		})
		c.stripeGroups[groupID] = g
	}

	var complete bool
	if g.streams[index] != nil {
		c.stripeGroupsMu.Unlock()
		stream.Close()
		return
	}
	g.streams[index] = stream
	g.remaining--
	complete = g.remaining == 0
	if complete {
		delete(c.stripeGroups, groupID)
	}
	c.stripeGroupsMu.Unlock()

	if !complete {
		return
	}

	g.timer.Stop()
	conns := make([]net.Conn, len(g.streams))
	for i, st := range g.streams {
		conns[i] = st
	}

	var clientStriped net.Conn
	if g.parity > 0 {
		dataShards := len(conns) - int(g.parity)
		fecConn, err := striping.NewFEC(conns, striping.DefaultChunkSize, dataShards, int(g.parity))
		if err != nil {
			c.logger.Errorf("failed to build FEC striped conn: %v", err)
			for _, cn := range conns {
				cn.Close()
			}
			return
		}
		clientStriped = fecConn
	} else {
		clientStriped = striping.New(conns, striping.DefaultChunkSize)
	}

	// Server promised D, read from leg 0
	peer, err := utils.ReadCount(g.streams[0])
	if err != nil {
		clientStriped.Close()
		return
	}

	own := swapper.FreezeUp()

	if err := utils.WriteCount(g.streams[0], own); err != nil {
		clientStriped.Close()
		return
	}

	swapper.Install(clientStriped, peer)
}
