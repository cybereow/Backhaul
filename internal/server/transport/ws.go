package transport

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gobwas/ws"
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
	tunnelChannel  chan TunnelChannel
	localChannel   chan LocalTCPConn
	reqNewConnChan chan struct{}
	controlChannel *network.WebSocketConn
	// controlMu guards controlChannel, handlersStarted, lossEpoch and
	// config.TunnelStatus. The HTTP handler installs the channel, the channel
	// handler and the loss path clear it, Restart resets it - all from different
	// goroutines.
	controlMu sync.Mutex
	// handlersStarted is set once the first control channel has started the port
	// listeners and handle loops; a later control channel is a reattach and must
	// not start them again.
	handlersStarted bool
	// lossEpoch advances on every control loss and every adoption, so a grace
	// waiter armed for one loss goes inert as soon as anything newer happens.
	lossEpoch    uint64
	restartMutex sync.Mutex
	usageMonitor *web.Usage
	// activeFlows counts flows currently carried over a handed-over tunnel
	// connection. They are independent of the control channel, so they are what
	// a missing control channel must not tear down.
	activeFlows   int32
	fallbackProxy http.Handler
}

type WsConfig struct {
	BindAddr      string
	SnifferLog    string
	TLSCertFile   string   // Path to the TLS certificate file
	TLSKeyFile    string   // Path to the TLS key file
	TLSCerts      []string // Optional: multiple cert files for SNI (multi-domain)
	TLSKeys       []string // Optional: key files aligned with TLSCerts
	TunnelStatus  string
	Token         string
	Ports         []string
	Nodelay       bool
	Sniffer       bool
	ProxyProtocol bool
	KeepAlive     time.Duration
	Heartbeat     time.Duration // in seconds
	ChannelSize   int
	WebPort       int
	Mode          config.TransportType // ws or wss
	Path          string
	Fallback      string        // decoy backend for non-tunnel requests (host:port), optional
	TLSEngine     string        // "go" (default) or "openssl" for wss TLS termination
	MaxConnAge    time.Duration // idle pool connections are replaced at this age (0 = never); see RotationPlan
}

func NewWSServer(parentCtx context.Context, config *WsConfig, logger *logrus.Logger) *WsTransport {
	// The first generation; Restart replaces it.
	gen := newWsGeneration(parentCtx)
	ctx, cancel := gen.ctx, gen.cancel

	// Build the decoy fallback proxy once, if configured. A bad address is
	// fatal here rather than silently disabling camouflage at runtime.
	fallbackProxy, err := network.NewFallbackProxy(config.Fallback)
	if err != nil {
		logger.Fatalf("invalid fallback address %q: %v", config.Fallback, err)
	}

	// Initialize the TcpTransport struct
	server := &WsTransport{
		config:         config,
		parentctx:      parentCtx,
		ctx:            ctx,
		cancel:         cancel,
		gen:            gen,
		logger:         logger,
		tunnelChannel:  make(chan TunnelChannel, config.ChannelSize),
		localChannel:   make(chan LocalTCPConn, config.ChannelSize),
		reqNewConnChan: make(chan struct{}, config.ChannelSize),
		controlChannel: nil, // will be set when a control connection is established
		usageMonitor:   web.NewDataStore(fmt.Sprintf(":%v", config.WebPort), ctx, config.SnifferLog, config.Sniffer, &config.TunnelStatus, logger),
		fallbackProxy:  fallbackProxy,
	}

	return server
}

func (s *WsTransport) Start() {
	g := s.gen
	// for  webui
	if s.config.WebPort > 0 {
		// A worker, so that Restart waits for the web port to be released before
		// the next generation's monitor tries to bind it.
		g.start(s.usageMonitor.Monitor)
	}

	s.config.TunnelStatus = fmt.Sprintf("Disconnected (%s)", s.config.Mode)

	g.start(s.tunnelListener)
}

// requestRestart asks for a Restart from outside the generation. Restart joins
// every worker of the generation it stops, so a worker calling it inline would
// be waiting for itself; every worker requests through here instead.
func (s *WsTransport) requestRestart() {
	go s.Restart()
}

// Restart stops the current generation, waits for every one of its workers, and
// only then publishes the next one. See WsMuxTransport.Restart.
func (s *WsTransport) Restart() {
	if !s.restartMutex.TryLock() {
		s.logger.Warn("server restart already in progress, skipping restart attempt")
		return
	}
	defer s.restartMutex.Unlock()

	s.logger.Info("restarting server...")

	level := s.logger.Level
	s.logger.SetLevel(logrus.FatalLevel)

	old := s.gen
	if old == nil && s.cancel != nil {
		s.cancel() // untracked transport: nothing to join
	}
	old.stop()

	// A grace waiter armed for the old generation must not restart the new one.
	s.controlMu.Lock()
	s.lossEpoch++
	s.controlMu.Unlock()

	joined := old.join(restartJoinTimeout)

	s.logger.SetLevel(level)

	if !joined {
		s.logger.Errorf("restart: previous generation's workers did not end within %s; not starting a new generation over them, retrying", restartJoinTimeout)
		time.AfterFunc(restartJoinTimeout, s.Restart)
		return
	}
	if s.parentctx.Err() != nil {
		s.logger.Info("restart abandoned: the server is shutting down")
		return
	}

	// No worker of the old generation is left, so everything below is ours alone.
	// The next control channel to arrive counts as a first one and starts the
	// listeners and handle loops again.
	s.controlMu.Lock()
	s.lossEpoch++
	stale := s.controlChannel
	s.controlChannel = nil
	s.handlersStarted = false
	s.controlMu.Unlock()
	if stale != nil {
		stale.Close()
	}

	// Whatever the old workers left queued is closed, not carried over.
	s.drainQueues()

	g := newWsGeneration(s.parentctx)
	s.gen, s.ctx, s.cancel = g, g.ctx, g.cancel

	// Re-initialize variables
	s.tunnelChannel = make(chan TunnelChannel, s.config.ChannelSize)
	s.localChannel = make(chan LocalTCPConn, s.config.ChannelSize)
	s.reqNewConnChan = make(chan struct{}, s.config.ChannelSize)
	s.usageMonitor = web.NewDataStore(fmt.Sprintf(":%v", s.config.WebPort), g.ctx, s.config.SnifferLog, s.config.Sniffer, &s.config.TunnelStatus, s.logger)
	s.config.TunnelStatus = ""
	atomic.StoreInt32(&s.activeFlows, 0)

	s.Start()
}

// drainQueues closes the tunnel connections and user connections a stopped
// generation left queued. Only called once every worker of that generation has
// returned, so nothing sends or receives concurrently.
func (s *WsTransport) drainQueues() {
	for {
		select {
		case tc := <-s.tunnelChannel:
			tc.conn.Close()
			continue
		case lc := <-s.localChannel:
			lc.conn.Close()
			continue
		default:
		}
		return
	}
}

// onControlLost handles a control channel that died on its own. Everything that
// carries traffic - the tunnel connections, the port listeners, the handle loops
// - is independent of the control channel (it only carries heartbeats and
// new-connection requests), so it all stays up and only the channel is dropped;
// the client re-dials it. If it does not come back, awaitReattach decides when a
// restart is the honest fallback.
func (s *WsTransport) onControlLost(g *wsGeneration, conn *network.WebSocketConn) {
	s.controlMu.Lock()
	if s.controlChannel != conn {
		// Already cleared, or the client has since reattached: this is a late
		// error from a connection nothing uses any more.
		s.controlMu.Unlock()
		conn.Close()
		g.release(conn)
		return
	}
	s.controlChannel = nil
	s.lossEpoch++
	epoch := s.lossEpoch
	s.config.TunnelStatus = fmt.Sprintf("Reconnecting (%s)", s.config.Mode)
	s.controlMu.Unlock()

	conn.Close()
	g.release(conn)

	if g.isStopped() {
		return // being torn down; nothing to hold or restart
	}
	s.logger.Warnf("control channel lost, holding the pool for up to %s for the client to reattach", controlGraceWindow)
	g.start(func() { s.awaitReattach(g, epoch) })
}

// awaitReattach waits for the client to bring a control channel back after the
// loss identified by epoch. It restarts only once the grace window has passed
// and nothing is left worth preserving (no flow is running) - or the total hold
// reaches maxControlGraceHold, which guarantees recovery from a client that
// vanished without closing its connections.
func (s *WsTransport) awaitReattach(g *wsGeneration, epoch uint64) {
	start := time.Now()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-g.ctx.Done():
			return
		case <-tick.C:
		}

		s.controlMu.Lock()
		current := s.lossEpoch == epoch && s.controlChannel == nil
		s.controlMu.Unlock()
		if !current {
			return // reattached, restarted or superseded by a newer loss
		}

		held := time.Since(start)
		if held < controlGraceWindow {
			continue
		}
		live := int(atomic.LoadInt32(&s.activeFlows))
		if graceShouldHold(live, held) {
			continue
		}
		s.logger.Warnf("control channel did not reattach within %s (%d flow(s) running), restarting server", held.Round(time.Second), live)
		s.requestRestart()
		return
	}
}

// channelHandler drives one control channel. It takes the connection as an
// argument rather than reading s.controlChannel on every use: a handler whose
// connection has died must not touch the pointer that by then may hold its
// replacement.
func (s *WsTransport) channelHandler(g *wsGeneration, conn *network.WebSocketConn) {
	ctx, reqNewConn := g.ctx, s.reqNewConnChan
	// A control connection this handler is done with is closed (by onControlLost,
	// or by the generation stopping); forget it so a flapping control channel does
	// not accumulate dead connections in the owned set.
	defer g.release(conn)

	// hctx also ends when this handler returns, so the reader below can never be
	// left parked on a send nobody will receive.
	hctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// A jittered timer (instead of a fixed-period ticker) so the heartbeat
	// cadence isn't perfectly periodic, which is an easy fingerprint for
	// traffic-pattern based DPI. wsmux/wssmux already do this.
	heartbeatTimer := time.NewTimer(utils.JitterDuration(s.config.Heartbeat))
	defer heartbeatTimer.Stop()

	// Channel to receive the message or error
	messageChan := make(chan byte, 10)

	// Reader: a worker of the generation, so Restart waits for it; the generation
	// closing the socket is what wakes its read.
	g.start(func() {
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				if ctx.Err() != nil {
					return // the transport is stopping and closed this socket itself
				}
				s.logger.Warn("failed to read from channel connection. ", err)
				go s.onControlLost(g, conn)
				return
			}
			// A zero-length binary frame (or padding-only payload) would
			// panic on msg[0] and take down this read goroutine; skip it.
			if len(msg) == 0 {
				continue
			}
			select {
			case messageChan <- msg[0]:
			case <-hctx.Done():
				return
			}
		}
	})

	for {
		select {
		case <-ctx.Done():
			// Bounded: the peer may be gone, and an unbounded write would keep
			// this handler alive past cancellation.
			_ = conn.SetWriteDeadline(time.Now().Add(controlCloseWriteTimeout))
			_ = utils.WriteControlSignal(conn, utils.SG_Closed)
			return

		case <-reqNewConn:
			err := utils.WriteControlSignal(conn, utils.SG_Chan)
			if err != nil {
				s.logger.Error("failed to send request new connection signal. ", err)
				go s.onControlLost(g, conn)
				return
			}

		case <-heartbeatTimer.C:
			err := utils.WriteControlSignal(conn, utils.SG_HB)
			if err != nil {
				s.logger.Errorf("failed to send heartbeat signal. Error: %v.", err)
				go s.onControlLost(g, conn)
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
				// Not inline: the restart joins this very handler.
				s.requestRestart()
				return

			default:
				s.logger.Errorf("unexpected response from channel: %v", msg)
				s.requestRestart()
				return
			}
		}
	}
}

func (s *WsTransport) tunnelListener() {
	// This listener is a worker of g, started after Restart republished the
	// channels, so it reads them once here.
	g, ctx, tunnelCh := s.gen, s.ctx, s.tunnelChannel
	addr := s.config.BindAddr
	basePath := network.NormalizeBasePath(s.config.Path)
	channelPath := basePath + "/channel"
	tunnelPathPrefix := basePath + "/tunnel"

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

			// From here the request is work of this generation: Restart waits for
			// it, and once the generation is stopped no new socket may join it.
			// (http.Server.Shutdown does not track hijacked connections, so this
			// is what keeps a late upgrade from outliving the teardown.)
			if !g.enter() {
				http.Error(w, "restarting", http.StatusServiceUnavailable)
				return
			}
			defer g.exit()

			netConn, brw, _, err := ws.UpgradeHTTP(r, w)
			if err != nil {
				s.logger.Errorf("failed to upgrade connection from %s: %v", r.RemoteAddr, err)
				return
			}
			conn := network.NewWebSocketConn(netConn, ws.StateServerSide, brw.Reader)

			if r.URL.Path == channelPath {
				// Owned from the moment it exists. If the generation was stopped
				// meanwhile, own has already closed it.
				if !g.own(conn) {
					return
				}
				// A control channel arriving while one is still registered is not a
				// second client - it is the same client reattaching after a drop
				// this side has not noticed yet (a one-way CDN reset leaves the
				// server's read blocked, so the client re-dials before the loss is
				// seen). Restarting here would drop every running flow, so adopt the
				// new connection and close the stale one: its handler exits by
				// itself, because the loss path ignores a connection that is no
				// longer registered.
				s.controlMu.Lock()
				stale := s.controlChannel
				first := !s.handlersStarted
				s.handlersStarted = true
				s.controlChannel = conn
				s.lossEpoch++ // ends any reattach wait
				s.config.TunnelStatus = fmt.Sprintf("Connected (%s)", s.config.Mode)
				s.controlMu.Unlock()
				if stale != nil {
					s.logger.Warn("control channel replaced while the previous one was still registered")
					stale.Close()
					g.release(stale)
				}

				s.logger.Info("control channel established successfully")
				g.start(func() { s.channelHandler(g, conn) })

				if first {
					numCPU := runtime.NumCPU()
					if numCPU > 4 {
						numCPU = 4 // Max allowed handler is 4
					}
					g.start(s.parsePortMappings)

					s.logger.Infof("starting %d handle loops on each CPU thread", numCPU)
					for i := 0; i < numCPU; i++ {
						g.start(s.handleLoop)
					}
				} else {
					s.logger.Info("control channel reattached, pool preserved")
				}

			} else if strings.HasPrefix(r.URL.Path, tunnelPathPrefix) {
				if !g.own(conn) {
					return
				}
				wsConn := TunnelChannel{
					conn: conn,
					ping: make(chan struct{}),
					mu:   &sync.Mutex{},
				}
				select {
				case tunnelCh <- wsConn:
					g.start(func() { s.keepAlive(&wsConn) })
					s.logger.Debugf("websocket connection accepted from %s", conn.RemoteAddr().String())
				default:
					s.logger.Warnf("websocket tunnel channel is full, closing connection from %s", conn.RemoteAddr().String())
					conn.Close()
					g.release(conn)
				}
			}
		}),
	}

	if s.config.Mode == config.WS {
		go func() {
			s.logger.Infof("ws server starting, listening on %s", addr)
			s.logger.Info("waiting for ws control channel connection")
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
			s.logger.Infof("wss server starting, listening on %s (tls engine: %s)", addr, engine)
			s.logger.Info("waiting for wss control channel connection")
			certs, keys := network.ResolveCertPairs(s.config.TLSCertFile, s.config.TLSKeyFile, s.config.TLSCerts, s.config.TLSKeys)
			ln, err := network.NewTLSListener(s.config.TLSEngine, addr, certs, keys, 0, 0, false)
			if err != nil {
				s.logger.Fatalf("failed to create tls listener on %s: %v", addr, err)
			}
			if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
				s.logger.Fatalf("failed to listen on %s: %v", addr, err)
			}
		}()
	}

	<-ctx.Done()

	// Gracefully shutdown the server
	s.logger.Infof("shutting down the webSocket server on %s", addr)
	if err := server.Shutdown(context.Background()); err != nil {
		s.logger.Errorf("Failed to gracefully shutdown the server: %v", err)
	}

	s.controlMu.Lock()
	if s.controlChannel != nil {
		s.controlChannel.Close()
	}
	s.controlMu.Unlock()
}

func (s *WsTransport) parsePortMappings() {
	g := s.gen
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
					s.startListener(g, localAddr, strconv.Itoa(port)) // Use port as the remoteAddr
					time.Sleep(1 * time.Millisecond)                  // for wide port ranges
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
					s.startListener(g, localAddr, remoteAddr)
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
		s.startListener(g, localAddr, remoteAddr)
	}
}

// startListener runs one port's listener as a worker of g.
func (s *WsTransport) startListener(g *wsGeneration, localAddr, remoteAddr string) {
	g.start(func() { s.localListener(g, localAddr, remoteAddr) })
}

func (s *WsTransport) localListener(g *wsGeneration, localAddr string, remoteAddr string) {
	portListener, err := net.Listen("tcp", localAddr)
	if err != nil {
		s.logger.Fatalf("failed to start listener on %s: %v", localAddr, err)
		return
	}

	//close local listener after context cancellation
	defer portListener.Close()

	s.logger.Infof("listener started successfully, listening on address: %s", portListener.Addr().String())

	g.start(func() { s.acceptLocalConn(g, portListener, remoteAddr) })

	<-g.ctx.Done()
}

func (s *WsTransport) acceptLocalConn(g *wsGeneration, listener net.Listener, remoteAddr string) {
	ctx, localCh, reqCh := g.ctx, s.localChannel, s.reqNewConnChan
	for {
		select {
		case <-ctx.Done():
			return

		default:
			s.logger.Debugf("waiting to accept incoming connection on %s", listener.Addr().String())
			conn := acceptWithBackoff(ctx, listener, s.logger)
			if conn == nil {
				return
			}

			// discard any non-tcp connection
			tcpConn, ok := conn.(*net.TCPConn)
			if !ok {
				s.logger.Warnf("disarded non-TCP connection from %s", conn.RemoteAddr().String())
				conn.Close()
				continue
			}

			// trying to enable tcpnodelay
			if !s.config.Nodelay {
				if err := tcpConn.SetNoDelay(s.config.Nodelay); err != nil {
					s.logger.Warnf("failed to set TCP_NODELAY for %s: %v", tcpConn.RemoteAddr().String(), err)
				} else {
					s.logger.Tracef("TCP_NODELAY disabled for %s", tcpConn.RemoteAddr().String())
				}
			}

			// Set keep-alive settings
			if err := tcpConn.SetKeepAlive(true); err != nil {
				s.logger.Warnf("failed to enable TCP keep-alive for %s: %v", tcpConn.RemoteAddr().String(), err)
			} else {
				s.logger.Tracef("TCP keep-alive enabled for %s", tcpConn.RemoteAddr().String())
			}
			if err := tcpConn.SetKeepAlivePeriod(s.config.KeepAlive); err != nil {
				s.logger.Warnf("failed to set TCP keep-alive period for %s: %v", tcpConn.RemoteAddr().String(), err)
			}

			select {
			case localCh <- LocalTCPConn{conn: conn, remoteAddr: remoteAddr, timeCreated: time.Now().UnixMilli()}:

				select {
				case reqCh <- struct{}{}:
					// Successfully requested a new connection
				default:
					// The channel is full, do nothing
					s.logger.Warn("channel is full, cannot request a new connection")
				}

				s.logger.Debugf("accepted incoming TCP connection from %s", tcpConn.RemoteAddr().String())

			default: // channel is full, discard the connection
				s.logger.Warnf("channel with listener %s is full, discarding TCP connection from %s", listener.Addr().String(), tcpConn.LocalAddr().String())
				conn.Close()
			}
		}
	}
}

func (s *WsTransport) handleLoop() {
	const setupBudget = 3000 * time.Millisecond
	g, ctx := s.gen, s.ctx
	for {
		select {
		case <-ctx.Done():
			return
		case localConn := <-s.localChannel:
			// Compute remaining budget from the original creation timestamp so
			// retries after a failed tunnel write never get a fresh 3 seconds.
			age := time.Duration(time.Now().UnixMilli()-localConn.timeCreated) * time.Millisecond
			remaining := setupBudget - age
			if remaining <= 0 {
				s.logger.Debugf("timeouted local connection: %d ms", age.Milliseconds())
				localConn.conn.Close()
				continue
			}
			// Absolute deadline for this local socket's entire setup phase.
			setupDeadline := time.Unix(0, localConn.timeCreated*int64(time.Millisecond)).Add(setupBudget)
			setupTimer := time.NewTimer(remaining)

		loop:
			for {
				select {
				case <-ctx.Done():
					setupTimer.Stop()
					localConn.conn.Close()
					return

				case <-setupTimer.C:
					age := time.Duration(time.Now().UnixMilli()-localConn.timeCreated) * time.Millisecond
					s.logger.Debugf("timeouted local connection: %d ms", age.Milliseconds())
					localConn.conn.Close()
					break loop

				case tunnelConnection := <-s.tunnelChannel:
					// Recheck expiry and cancellation before committing to this
					// tunnel; another attempt can only draw from the same budget.
					if ctx.Err() != nil {
						// drain timer so it can be GC'd
						if !setupTimer.Stop() {
							select {
							case <-setupTimer.C:
							default:
							}
						}
						localConn.conn.Close()
						tunnelConnection.conn.Close()
						return
					}
					select {
					case <-setupTimer.C:
						age := time.Duration(time.Now().UnixMilli()-localConn.timeCreated) * time.Millisecond
						s.logger.Debugf("timeouted local connection: %d ms", age.Milliseconds())
						localConn.conn.Close()
						tunnelConnection.conn.Close()
						break loop
					default:
					}

					close(tunnelConnection.ping)
					// Apply the remaining setup deadline to the underlying net.Conn
					// now, before acquiring the handover lock, so that any
					// in-progress keepAlive write is unblocked by the deadline
					// rather than leaving Lock() stuck indefinitely.
					tunnelConnection.conn.NetConn().SetDeadline(setupDeadline) //nolint:errcheck
					// Taken and deliberately never released: from here the
					// connection belongs to WSConnectionHandler, and the
					// keepAlive goroutine must never write another ping into the
					// middle of that data stream. It TryLocks and gives up, so
					// holding this forever is the handover. The mutex dies with
					// the per-connection struct, so nothing leaks - do not
					// "balance" it with an Unlock.
					tunnelConnection.mu.Lock()

					// Recheck after acquiring the lock: cancel or timer may have
					// fired while we were waiting.
					if ctx.Err() != nil {
						tunnelConnection.conn.NetConn().SetDeadline(time.Time{}) //nolint:errcheck
						tunnelConnection.conn.Close()
						if !setupTimer.Stop() {
							select {
							case <-setupTimer.C:
							default:
							}
						}
						localConn.conn.Close()
						return
					}
					select {
					case <-setupTimer.C:
						age := time.Duration(time.Now().UnixMilli()-localConn.timeCreated) * time.Millisecond
						s.logger.Debugf("timeouted local connection: %d ms", age.Milliseconds())
						tunnelConnection.conn.NetConn().SetDeadline(time.Time{}) //nolint:errcheck
						tunnelConnection.conn.Close()
						localConn.conn.Close()
						break loop
					default:
					}

					if err := tunnelConnection.conn.WriteMessage(network.TextMessage, []byte(localConn.remoteAddr)); err != nil {
						s.logger.Debugf("%v", err)                               // failed to send port number
						tunnelConnection.conn.NetConn().SetDeadline(time.Time{}) //nolint:errcheck
						tunnelConnection.conn.Close()
						// If the write failed due to a deadline (budget exhausted)
						// or cancellation, treat it as terminal for this socket.
						if ctx.Err() != nil {
							if !setupTimer.Stop() {
								select {
								case <-setupTimer.C:
								default:
								}
							}
							localConn.conn.Close()
							return
						}
						// Timer may have just fired; check before retrying.
						select {
						case <-setupTimer.C:
							age := time.Duration(time.Now().UnixMilli()-localConn.timeCreated) * time.Millisecond
							s.logger.Debugf("timeouted local connection: %d ms", age.Milliseconds())
							localConn.conn.Close()
							break loop
						default:
						}
						continue loop
					}

					// SUCCESS: clear the setup deadline before handing the
					// tunnel connection to WSConnectionHandler so data-path
					// I/O is not subject to the setup timeout.
					tunnelConnection.conn.NetConn().SetDeadline(time.Time{}) //nolint:errcheck
					setupTimer.Stop()
					// Handle data exchange between connections
					tunnelConn, userConn := tunnelConnection.conn, localConn.conn
					atomic.AddInt32(&s.activeFlows, 1)
					if !g.start(func() {
						defer atomic.AddInt32(&s.activeFlows, -1)
						defer g.release(tunnelConn)
						handlers.WSConnectionHandler(ctx, s.config.ProxyProtocol, tunnelConn, userConn, s.logger, s.usageMonitor, userConn.LocalAddr().(*net.TCPAddr).Port, s.config.Sniffer)
					}) {
						// Stopped: nobody will serve this flow.
						atomic.AddInt32(&s.activeFlows, -1)
						tunnelConn.Close()
						userConn.Close()
					}
					break loop
				}
			}
		}
	}
}

// rotateReplaceWait bounds how long an aging idle tunnel connection waits for
// its replacement to join the pool before it is closed anyway.
const rotateReplaceWait = 5 * time.Second

// retireIdleTunnel replaces one aging idle pool connection make-before-break:
// ask the client for a new one, wait (bounded) until the pool has grown, then
// close this one. It reports false if the connection was handed to a flow in the
// meantime, in which case it must be left alone. The hand-over lock is taken only
// for the final close, never while waiting, so a flow arriving meanwhile is not
// held up; once it holds the lock the keepAlive can never touch the connection.
func (s *WsTransport) retireIdleTunnel(ctx context.Context, tunnelCh chan TunnelChannel, reqCh chan struct{}, conn *TunnelChannel) bool {
	before := len(tunnelCh)
	select {
	case reqCh <- struct{}{}:
	default:
	}
	wait := time.NewTimer(rotateReplaceWait)
	defer wait.Stop()
	poll := time.NewTicker(200 * time.Millisecond)
	defer poll.Stop()
	for len(tunnelCh) <= before {
		select {
		case <-ctx.Done():
			return false
		case <-conn.ping:
			return false // handed to a flow
		case <-wait.C:
			return s.closeIdleTunnel(conn)
		case <-poll.C:
		}
	}
	return s.closeIdleTunnel(conn)
}

// closeIdleTunnel closes conn unless a flow has taken it (it then holds the
// hand-over lock for good).
func (s *WsTransport) closeIdleTunnel(conn *TunnelChannel) bool {
	if !conn.mu.TryLock() {
		return false
	}
	s.logger.Debug("idle tunnel connection reached its rotation age, closing")
	conn.conn.Close()
	return true
}

func (s *WsTransport) keepAlive(conn *TunnelChannel) {
	ctx, tunnelCh, reqCh := s.ctx, s.tunnelChannel, s.reqNewConnChan
	// Jittered like the control channel heartbeat: a pool of idle connections
	// all pinging on the same fixed period is a strong traffic fingerprint. The
	// payload deliberately stays a bare SG_Ping byte - clients match this frame
	// exactly, so padding it would break every client older than this change.
	pingTimer := time.NewTimer(utils.JitterDuration(s.config.Heartbeat))

	defer pingTimer.Stop()

	// An idle connection is retired before the CDN's max-age reset can land on
	// it: a write to a connection the edge already dropped still succeeds, so a
	// flow handed such a connection would silently black-hole until its setup
	// times out. A nil channel (rotation disabled) blocks forever in the select.
	var rotate <-chan time.Time
	if s.config.MaxConnAge > 0 {
		rotateTimer := time.NewTimer(rotateAge(s.config.MaxConnAge))
		defer rotateTimer.Stop()
		rotate = rotateTimer.C
	}

	for {
		select {
		case <-ctx.Done():
			conn.conn.Close()
			return
		case <-conn.ping:
			s.logger.Trace("ping channel closed")
			return
		case <-rotate:
			if s.retireIdleTunnel(ctx, tunnelCh, reqCh, conn) {
				return
			}
			rotate = nil
		case <-pingTimer.C:
			pingTimer.Reset(utils.JitterDuration(s.config.Heartbeat))

			// Try to acquire the lock without blocking
			locked := conn.mu.TryLock()
			if !locked {
				// If the lock is held by another operation, stop the pingSender
				s.logger.Trace("write operation in progress, stopping pingSender")
				return
			}

			if err := conn.conn.WriteMessage(network.BinaryMessage, []byte{utils.SG_Ping}); err != nil {
				conn.mu.Unlock()
				conn.conn.Close()
				return
			}
			conn.mu.Unlock()
			s.logger.Trace("ping sent to the client")
		}
	}
}
