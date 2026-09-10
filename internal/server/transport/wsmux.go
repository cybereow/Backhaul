package transport

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gobwas/ws"
	"github.com/musix/backhaul/config" // for mode
	"github.com/musix/backhaul/internal/utils"
	"github.com/musix/backhaul/internal/utils/network"
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
	tunnelChannel  chan *smux.Session
	localChannel   chan LocalTCPConn
	reqNewConnChan chan struct{}
	controlChannel *network.WebSocketConn
	usageMonitor   *web.Usage
	restartMutex   sync.Mutex
	streamCounter  int32
	sessionCounter int32
	// admittedSessions counts every pool session ever admitted and is never
	// decremented. Rotation compares it against a mark taken when it asked for
	// a replacement, which is how it knows a replacement actually arrived - the
	// live sessionCounter cannot tell "replacement is up" from "another
	// connection died at the same moment".
	admittedSessions int32
	stripedFlows     int32 // in-flight striped flows, bounded by the pool's stream budget
	plainFlows       int32

	// sessions is a live registry of pool sessions the striped dispatcher (and
	// the single-leg UDP path) picks legs from. Each entry carries the CDN it
	// arrived over and a live RTT estimate so leg selection can steer toward the
	// lowest-latency, least-loaded connection and spread a flow across distinct
	// CDNs. The non-striped TCP path never touches this.
	sessionsMu    sync.Mutex
	sessions      []*pooledSession
	stripeGroupID uint32
	// plainSelectMu serializes single-leg (plain) flow placement so a burst of
	// concurrent flows can't all read the same pre-OpenStream load and pile onto
	// one session: each pick's OpenStream bumps that session's stream count
	// before the next pick scores, so score-based selection actually spreads them
	// across CDNs instead of concentrating on one connection (whose single-
	// connection upload ceiling then caps the aggregate).
	plainSelectMu sync.Mutex

	fallbackProxy http.Handler

	// controlMu guards controlChannel, handlersStarted and graceTimer. The
	// HTTP handler goroutine may be adopting a reattached control channel at
	// the same moment a dying channelHandler is clearing the old one.
	controlMu       sync.Mutex
	handlersStarted bool
	graceTimer      *time.Timer
	// graceStart marks when the current control-loss grace period began, so the
	// hold can be capped: without a cap, a client that drops silently (no smux
	// keepalive, so sessions never report closed) would be held "up" forever
	// with no working data path. Set on the initial loss, not on each re-arm.
	graceStart time.Time

	// events is a small ring buffer of disruption events (control-channel
	// losses, restarts, replacements) with timestamps, so an operator can see
	// *why* a tunnel dropped after the fact via the /diag endpoint instead of
	// having to catch it live in the logs.
	eventsMu sync.Mutex
	events   []transportEvent
}

type WsMuxConfig struct {
	BindAddr             string
	Token                string
	SnifferLog           string
	TLSCertFile          string   // Path to the TLS certificate file
	TLSKeyFile           string   // Path to the TLS key file
	TLSCerts             []string // Optional: multiple cert files for SNI (multi-domain)
	TLSKeys              []string // Optional: key files aligned with TLSCerts
	TunnelStatus         string
	Ports                []string
	Nodelay              bool
	Sniffer              bool
	KeepAlive            time.Duration
	Heartbeat            time.Duration // in seconds
	ChannelSize          int
	MuxCon               int
	AcceptUDP            bool // forward UDP alongside TCP on each mapped port (requires mux_version >= 2)
	UDPBuffer            int  // datagrams queued per UDP flow before dropping (0 = default 2048)
	Speedtest            bool // expose the token-gated <path>/speedtest endpoint (requires mux_version >= 2)
	MuxVersion           int
	MaxFrameSize         int
	MaxReceiveBuffer     int
	MaxStreamBuffer      int
	WebPort              int
	Mode                 config.TransportType // ws or wss
	ProxyProtocol        bool
	Path                 string
	MuxKeepaliveDisabled bool
	StripeFactor         int
	StripeParity         int
	StripePorts          []string
	Fallback             string        // decoy backend for non-tunnel requests (host:port), optional
	TLSEngine            string        // "go" (default) or "openssl" for wssmux TLS termination
	MaxConnAge           time.Duration // retire pool connections at this age (0 = never); see retireSession
	PromoteBytes         uint64        // bytes transferred before upgrading to a striped connection
	SO_RCVBUF            int           // socket receive buffer forced on the server's accepted tunnel legs (0 = OS default)
	SO_SNDBUF            int           // socket send buffer forced on the server's accepted tunnel legs (0 = OS default)
}

func NewWSMuxServer(parentCtx context.Context, config *WsMuxConfig, logger *logrus.Logger) *WsMuxTransport {
	// Create a derived context from the parent context
	ctx, cancel := context.WithCancel(parentCtx)

	// Build the decoy fallback proxy once, if configured. A bad address is
	// fatal here rather than silently disabling camouflage at runtime.
	fallbackProxy, err := network.NewFallbackProxy(config.Fallback)
	if err != nil {
		logger.Fatalf("invalid fallback address %q: %v", config.Fallback, err)
	}

	// Initialize the TcpTransport struct
	server := &WsMuxTransport{
		smuxConfig: &smux.Config{
			Version:           config.MuxVersion,
			KeepAliveDisabled: config.MuxKeepaliveDisabled,
			KeepAliveInterval: 20 * time.Second,
			KeepAliveTimeout:  40 * time.Second,
			MaxFrameSize:      config.MaxFrameSize,
			MaxReceiveBuffer:  config.MaxReceiveBuffer,
			MaxStreamBuffer:   config.MaxStreamBuffer,
		},
		config:         config,
		parentctx:      parentCtx,
		ctx:            ctx,
		cancel:         cancel,
		logger:         logger,
		tunnelChannel:  make(chan *smux.Session, config.ChannelSize),
		localChannel:   make(chan LocalTCPConn, config.ChannelSize),
		reqNewConnChan: make(chan struct{}, config.ChannelSize),
		streamCounter:  0,
		sessionCounter: 0,
		controlChannel: nil, // will be set when a control connection is established
		usageMonitor:   web.NewDataStore(fmt.Sprintf(":%v", config.WebPort), ctx, config.SnifferLog, config.Sniffer, &config.TunnelStatus, logger),
		fallbackProxy:  fallbackProxy,
	}

	return server
}

func (s *WsMuxTransport) Start() {
	// for  webui
	if s.config.WebPort > 0 {
		go s.usageMonitor.Monitor()
	}

	s.config.TunnelStatus = fmt.Sprintf("Disconnected (%s)", s.config.Mode)

	go s.tunnelListener()

}

func (s *WsMuxTransport) Restart() {
	if !s.restartMutex.TryLock() {
		s.logger.Warn("server restart already in progress, skipping restart attempt")
		return
	}
	defer s.restartMutex.Unlock()

	s.logger.Info("restarting server...")

	// for removing timeout logs
	level := s.logger.Level
	s.logger.SetLevel(logrus.FatalLevel)

	if s.cancel != nil {
		s.cancel()
	}

	// Close the control channel and reset the reattach state, so the next
	// control channel to arrive counts as a first one and starts the pool
	// machinery again.
	s.controlMu.Lock()
	if s.graceTimer != nil {
		s.graceTimer.Stop()
		s.graceTimer = nil
	}
	if s.controlChannel != nil {
		s.controlChannel.Close()
	}
	s.controlChannel = nil
	s.handlersStarted = false
	s.controlMu.Unlock()

	time.Sleep(2 * time.Second)

	ctx, cancel := context.WithCancel(s.parentctx)
	s.ctx = ctx
	s.cancel = cancel

	// Re-initialize variables
	s.tunnelChannel = make(chan *smux.Session, s.config.ChannelSize)
	s.localChannel = make(chan LocalTCPConn, s.config.ChannelSize)
	s.reqNewConnChan = make(chan struct{}, s.config.ChannelSize)
	s.usageMonitor = web.NewDataStore(fmt.Sprintf(":%v", s.config.WebPort), ctx, s.config.SnifferLog, s.config.Sniffer, &s.config.TunnelStatus, s.logger)
	s.config.TunnelStatus = ""
	s.streamCounter = 0
	s.sessionCounter = 0
	s.admittedSessions = 0
	// Reset the in-flight striped-flow count too. Left stale, acquireStripedSlot
	// would see active*StripeFactor already over the budget of a fresh, empty
	// pool and busy-wait (or hang) until leftover goroutines from the previous
	// generation happened to decrement it.
	atomic.StoreInt32(&s.stripedFlows, 0)

	s.sessionsMu.Lock()
	s.sessions = nil
	s.sessionsMu.Unlock()

	// set the log level again
	s.logger.SetLevel(level)

	go s.Start()
}

// channelHandler drives one control channel. It takes the connection as an
// argument rather than reading s.controlChannel on every use: once a dropped
// channel can be replaced without restarting the transport, a handler for a
// dead connection must never touch the shared pointer that now holds its
// successor.
func (s *WsMuxTransport) channelHandler(conn *network.WebSocketConn) {
	// A jittered timer (instead of a fixed-period ticker) so the heartbeat
	// cadence isn't perfectly periodic, which is an easy fingerprint for
	// traffic-pattern based DPI.
	heartbeatTimer := time.NewTimer(utils.JitterDuration(s.config.Heartbeat))
	defer heartbeatTimer.Stop()

	// Channel to receive the message or error
	messageChan := make(chan byte, 10)

	// Separate goroutine to continuously listen for messages
	go func() {
		for {
			select {
			case <-s.ctx.Done():
				return

			default:
				_, msg, err := conn.ReadMessage()
				// Exit if there's an error
				if err != nil {
					s.logger.Warn("control channel read failed. ", err)
					// The control channel carries no user data - only
					// heartbeats and new-connection requests - so losing it
					// must not take the pool, and every flow running on it,
					// down as well. Hold everything and wait for a reattach.
					go s.onControlLost(conn)
					return
				}
				// A zero-length binary frame (or padding-only payload) would
				// panic on msg[0] and take down this read goroutine; skip it.
				if len(msg) == 0 {
					continue
				}
				messageChan <- msg[0]
			}
		}
	}()

	for {
		select {
		case <-s.ctx.Done():
			_ = utils.WriteControlSignal(conn, utils.SG_Closed)
			return
		case <-s.reqNewConnChan:
			err := utils.WriteControlSignal(conn, utils.SG_Chan)
			if err != nil {
				s.logger.Warn("failed to send request new connection signal. ", err)
				go s.onControlLost(conn)
				return
			}

		case <-heartbeatTimer.C:
			err := utils.WriteControlSignal(conn, utils.SG_HB)
			if err != nil {
				s.logger.Warnf("failed to send heartbeat signal. Error: %v.", err)
				go s.onControlLost(conn)
				return
			}
			s.logger.Debug("heartbeat signal sent successfully")
			heartbeatTimer.Reset(utils.JitterDuration(s.config.Heartbeat))

		case msg, ok := <-messageChan:
			if !ok {
				s.logger.Error("channel closed, likely due to an error in WebSocket read")
				return
			}
			switch msg {
			case utils.SG_HB:
				s.logger.Trace("heartbeat signal received successfully")

			case utils.SG_Closed:
				s.logger.Warn("control channel has been closed by the client")
				s.recordEvent("restart", "control channel closed by client; full restart (all flows dropped)")
				s.Restart()
				return

			default:
				s.logger.Errorf("unexpected response from channel: %v", msg)
				s.recordEvent("restart", fmt.Sprintf("unexpected control signal %v; full restart (all flows dropped)", msg))
				go s.Restart()
				return
			}

		}
	}
}

func (s *WsMuxTransport) tunnelListener() {
	addr := s.config.BindAddr
	basePath := network.NormalizeBasePath(s.config.Path)
	channelPath := basePath + "/channel"
	tunnelPathPrefix := basePath + "/tunnel"
	speedtestPath := basePath + "/speedtest"
	diagPath := basePath + "/diag"
	poolPath := basePath + "/pool"

	// Built once rather than per request: this ran through fmt.Sprintf on
	// every probe that reached the listener.
	expectedAuth := "Bearer " + s.config.Token

	// Create an HTTP server
	server := &http.Server{
		Addr: addr,
		// IdleTimeout stays disabled because after the WebSocket upgrade the
		// connection is a long-lived raw tunnel. ReadHeaderTimeout still bounds
		// how long an unauthenticated client may take to send its request
		// headers (auth runs only after they're read), so a slow-header client
		// can't pin a goroutine and a completed TLS session indefinitely.
		IdleTimeout:       -1,
		ReadHeaderTimeout: 10 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			s.logger.Tracef("received http request from %s", r.RemoteAddr)

			// Token-gated speedtest endpoint: not a tunnel upgrade, so it is
			// handled before the upgrade path. Enabled only when configured; when
			// off it falls through to the normal not-a-tunnel-path handling
			// (fallback/401) so the endpoint is invisible.
			if s.config.Speedtest && r.URL.Path == speedtestPath {
				if !authorizedToken(r.Header.Get("Authorization"), expectedAuth) {
					http.Error(w, "unauthorized", http.StatusUnauthorized)
					return
				}
				s.handleSpeedtestRequest(w, r)
				return
			}

			// Token-gated diagnostics: recent disruption events (control-channel
			// losses, restarts) so an operator can see why the tunnel dropped
			// after the fact, without catching it live in the logs.
			if r.URL.Path == diagPath {
				if !authorizedToken(r.Header.Get("Authorization"), expectedAuth) {
					http.Error(w, "unauthorized", http.StatusUnauthorized)
					return
				}
				events := s.snapshotEvents()
				writeJSON(w, http.StatusOK, map[string]interface{}{
					"now":    time.Now(),
					"count":  len(events),
					"events": events,
				})
				return
			}

			// Token-gated live pool view: per-CDN active-stream counts, so you can
			// see during a transfer whether flows are actually spreading across the
			// CDNs or piling onto one. Run it *during* a speed test.
			if r.URL.Path == poolPath {
				if !authorizedToken(r.Header.Get("Authorization"), expectedAuth) {
					http.Error(w, "unauthorized", http.StatusUnauthorized)
					return
				}
				writeJSON(w, http.StatusOK, s.poolSnapshot())
				return
			}

			// A request is legitimate tunnel traffic only if it carries the
			// token AND targets the control or a tunnel path. Anything else -
			// a wrong/absent token, or a probe hitting "/" - is not upgraded:
			// if a decoy fallback is configured it is reverse-proxied there so
			// the origin looks like an ordinary website, otherwise it is
			// rejected as before.
			isTunnelPath := r.URL.Path == channelPath || strings.HasPrefix(r.URL.Path, tunnelPathPrefix)
			if !authorizedToken(r.Header.Get("Authorization"), expectedAuth) || !isTunnelPath {
				if s.fallbackProxy != nil {
					s.logger.Debugf("serving fallback for %s %s from %s", r.Method, r.URL.Path, r.RemoteAddr)
					s.fallbackProxy.ServeHTTP(w, r)
					return
				}
				s.logger.Warnf("unauthorized request from %s, closing connection", r.RemoteAddr)
				http.Error(w, "unauthorized", http.StatusUnauthorized) // Send 401 Unauthorized response
				return
			}

			netConn, brw, _, err := ws.UpgradeHTTP(r, w)
			if err != nil {
				s.logger.Errorf("failed to upgrade connection from %s: %v", r.RemoteAddr, err)
				return
			}
			conn := network.NewWebSocketConn(netConn, ws.StateServerSide, brw.Reader)

			if r.URL.Path == channelPath {
				s.controlMu.Lock()
				// A control channel arriving while one is still registered is
				// not a second client - it is the same client reattaching after
				// a drop this side has not noticed yet. A one-way reset (the
				// common CDN failure) leaves the server's read blocked and
				// controlChannel non-nil, so the client re-dials before
				// onControlLost ever runs. Restarting here would tear down the
				// pool and every flow on it, which is exactly what the reattach
				// path exists to avoid, so adopt the new connection and drop the
				// stale one. Its handler exits by itself: onControlLost bails
				// out when controlChannel is no longer the conn it was called
				// for.
				if old := s.controlChannel; old != nil {
					s.logger.Warn("control channel replaced while the previous one was still registered")
					s.recordEvent("control_replaced", fmt.Sprintf("new control channel from %s adopted, stale one dropped", conn.RemoteAddr()))
					old.Close()
				}
				// The first control channel starts the pool machinery. One
				// arriving after a drop is a reattach: the handle loops, port
				// listeners and pool sessions are all still running, and
				// starting them again would double every listener.
				first := !s.handlersStarted
				s.handlersStarted = true
				s.controlChannel = conn
				if s.graceTimer != nil {
					s.graceTimer.Stop()
					s.graceTimer = nil
				}
				s.controlMu.Unlock()

				go s.channelHandler(conn)

				if !first {
					s.logger.Info("control channel reattached successfully, pool preserved")
					s.config.TunnelStatus = fmt.Sprintf("Connected (%s)", s.config.Mode)
					return
				}

				s.logger.Info("control channel established successfully")

				numCPU := runtime.NumCPU()
				if numCPU > 4 {
					numCPU = 4 // Max allowed handler is 4
				}

				go s.parsePortMappings()

				s.logger.Infof("starting %d handle loops on each CPU thread", numCPU)

				for i := 0; i < numCPU; i++ {
					go s.handleLoop()
				}

				go s.dispatchLoop()

				s.controlMu.Lock()
				s.config.TunnelStatus = fmt.Sprintf("Connected (%s)", s.config.Mode)
				s.controlMu.Unlock()

			} else if strings.HasPrefix(r.URL.Path, tunnelPathPrefix) {
				session, err := smux.Client(netConn, s.smuxConfig)
				if err != nil {
					s.logger.Errorf("failed to create MUX session for connection %s: %v", conn.RemoteAddr().String(), err)
					conn.Close()
					return
				}
				select {
				case s.tunnelChannel <- session: // ok
				default:
					s.logger.Warnf("tunnel listener channel is full, discarding TCP connection from %s", conn.LocalAddr().String())
					// Close the smux session, not just the raw conn: a bare
					// conn.Close() leaves the session's goroutines and buffers
					// leaked. session.Close() also closes the underlying conn.
					session.Close()
					conn.Close()
				}
			}
		}),
	}

	if s.config.Mode == config.WSMUX {
		go func() {
			s.logger.Infof("%s server starting, listening on %s", s.config.Mode, addr)
			if s.controlChannel == nil {
				s.logger.Infof("waiting for %s control channel connection", s.config.Mode)
			}
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				s.logger.Fatalf("failed to listen on %s: %v", addr, err)
			}
		}()
	} else {
		go func() {
			engine := s.config.TLSEngine
			if engine == "" {
				engine = network.TLSEngineGo
			}
			s.logger.Infof("%s server starting, listening on %s (tls engine: %s)", s.config.Mode, addr, engine)
			if s.controlChannel == nil {
				s.logger.Infof("waiting for %s control channel connection", s.config.Mode)
			}
			certs, keys := network.ResolveCertPairs(s.config.TLSCertFile, s.config.TLSKeyFile, s.config.TLSCerts, s.config.TLSKeys)
			ln, err := network.NewTLSListener(s.config.TLSEngine, addr, certs, keys, s.config.SO_RCVBUF, s.config.SO_SNDBUF)
			if err != nil {
				s.logger.Fatalf("failed to create tls listener on %s: %v", addr, err)
			}
			if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
				s.logger.Fatalf("failed to listen on %s: %v", addr, err)
			}
		}()
	}

	<-s.ctx.Done()

	// close connection
	s.controlMu.Lock()
	if s.controlChannel != nil {
		s.controlChannel.Close()
	}
	s.controlMu.Unlock()

	// Gracefully shutdown the server
	s.logger.Infof("shutting down the websocket server on %s", addr)
	if err := server.Shutdown(context.Background()); err != nil {
		s.logger.Errorf("Failed to gracefully shutdown the server: %v", err)
	}
}

func (s *WsMuxTransport) parsePortMappings() {
	// UDP forwarding rides the flow-kind protocol, which only exists on
	// mux_version >= 2. Warn once here rather than silently ignoring accept_udp
	// so a misconfigured setup is obvious in the logs.
	if s.config.AcceptUDP && s.config.MuxVersion < 2 {
		s.logger.Warn("accept_udp is enabled but requires mux_version = 2; UDP forwarding is disabled for this session")
	}

	for _, portMapping := range s.config.Ports {
		parts := strings.Split(portMapping, "=")

		var localAddr, remoteAddr string

		// Check if only a single port or a port range is provided (no "=" present)
		if len(parts) == 1 {
			localPortOrRange := strings.TrimSpace(parts[0])
			remoteAddr = localPortOrRange // If no remote addr is provided, use the local port as the remote port

			// Check if it's a port range
			if strings.Contains(localPortOrRange, "-") {
				rangeParts := strings.Split(localPortOrRange, "-")
				if len(rangeParts) != 2 {
					s.logger.Fatalf("invalid port range format: %s", localPortOrRange)
				}

				// Parse and validate start and end ports
				startPort, err := strconv.Atoi(strings.TrimSpace(rangeParts[0]))
				if err != nil || startPort < 1 || startPort > 65535 {
					s.logger.Fatalf("invalid start port in range: %s", rangeParts[0])
				}

				endPort, err := strconv.Atoi(strings.TrimSpace(rangeParts[1]))
				if err != nil || endPort < 1 || endPort > 65535 || endPort < startPort {
					s.logger.Fatalf("invalid end port in range: %s", rangeParts[1])
				}

				// Create listeners for all ports in the range
				for port := startPort; port <= endPort; port++ {
					localAddr = fmt.Sprintf(":%d", port)
					s.startPortListeners(localAddr, strconv.Itoa(port)) // Use port as the remoteAddr
					time.Sleep(1 * time.Millisecond)                    // for wide port ranges
				}
				continue
			} else {
				// Handle single port case
				port, err := strconv.Atoi(localPortOrRange)
				if err != nil || port < 1 || port > 65535 {
					s.logger.Fatalf("invalid port format: %s", localPortOrRange)
				}
				localAddr = fmt.Sprintf(":%d", port)
			}
		} else if len(parts) == 2 {
			// Handle "local=remote" format
			localPortOrRange := strings.TrimSpace(parts[0])
			remoteAddr = strings.TrimSpace(parts[1])

			// Check if local port is a range
			if strings.Contains(localPortOrRange, "-") {
				rangeParts := strings.Split(localPortOrRange, "-")
				if len(rangeParts) != 2 {
					s.logger.Fatalf("invalid port range format: %s", localPortOrRange)
				}

				// Parse and validate start and end ports
				startPort, err := strconv.Atoi(strings.TrimSpace(rangeParts[0]))
				if err != nil || startPort < 1 || startPort > 65535 {
					s.logger.Fatalf("invalid start port in range: %s", rangeParts[0])
				}

				endPort, err := strconv.Atoi(strings.TrimSpace(rangeParts[1]))
				if err != nil || endPort < 1 || endPort > 65535 || endPort < startPort {
					s.logger.Fatalf("invalid end port in range: %s", rangeParts[1])
				}

				// Create listeners for all ports in the range
				for port := startPort; port <= endPort; port++ {
					localAddr = fmt.Sprintf(":%d", port)
					s.startPortListeners(localAddr, remoteAddr)
					time.Sleep(1 * time.Millisecond) // for wide port ranges
				}
				continue
			} else {
				// Handle single local port case
				port, err := strconv.Atoi(localPortOrRange)
				if err == nil && port >= 1 && port <= 65535 { // format port=remoteAddress
					localAddr = fmt.Sprintf(":%d", port)
				} else {
					localAddr = localPortOrRange // format ip:port=remoteAddress
				}
			}
		} else {
			s.logger.Fatalf("invalid port mapping format: %s", portMapping)
		}
		// Start listeners for single port
		s.startPortListeners(localAddr, remoteAddr)
	}
}

// startPortListeners brings up the TCP listener for a mapped port and, when
// accept_udp is enabled on a mux_version >= 2 tunnel, a UDP listener on the same
// port so both transports are forwarded (needed for e.g. L2TP/IPsec, which uses
// UDP 500/1701/4500).
func (s *WsMuxTransport) startPortListeners(localAddr, remoteAddr string) {
	go s.localListener(localAddr, remoteAddr)
	if s.config.AcceptUDP && s.config.MuxVersion >= 2 {
		go s.udpListener(localAddr, remoteAddr)
	}
}

func (s *WsMuxTransport) handleLoop() {
	for {
		select {
		case <-s.ctx.Done():
			return

		case session := <-s.tunnelChannel:
			// +1 for session counter
			atomic.AddInt32(&s.sessionCounter, 1)
			atomic.AddInt32(&s.admittedSessions, 1)

			ps := s.registerSession(session)
			// Keep this session's RTT estimate fresh so leg selection can steer
			// toward the lowest-latency CDN. Requires mux_version >= 2 (the peer
			// must be able to route the probe to its echoer, which it does on the
			// FlowPing kind regardless of mode). Every leg-selecting path benefits:
			// striping, the UDP flow path, and the plain single-leg path, which now
			// ranks by legScore too so it spreads across the fast CDNs rather than
			// blindly round-robining onto the slow tail.
			if s.config.MuxVersion >= 2 {
				go s.probeSessionRTT(ps)
			}
			go func(sess *smux.Session) {
				<-sess.CloseChan()
				s.unregisterSession(sess)
				atomic.AddInt32(&s.sessionCounter, -1)
			}(session)
			if s.config.MaxConnAge > 0 {
				go s.rotateStripedSession(session)
			}
		}
	}
}

// stripedDispatchLoop replaces the per-session handleSession loop when
// StripeFactor > 1: for each incoming local connection it grabs one stream
// from several distinct pool sessions instead of one stream from one
// session, so the flow isn't pinned to a single underlying TCP connection.
func (s *WsMuxTransport) dispatchLoop() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case incomingConn := <-s.localChannel:
			if s.shouldStripe(incomingConn) {
				go s.dispatchStriped(incomingConn)
			} else {
				go s.dispatchPlain(incomingConn)
			}
		}
	}
}
