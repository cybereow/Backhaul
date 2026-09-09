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
	config         *WsConfig
	parentctx      context.Context
	ctx            context.Context
	cancel         context.CancelFunc
	logger         *logrus.Logger
	tunnelChannel  chan TunnelChannel
	localChannel   chan LocalTCPConn
	reqNewConnChan chan struct{}
	controlChannel *network.WebSocketConn
	// controlMu guards controlChannel. The HTTP handler installs it, the
	// channel handler reads it on every heartbeat and Restart clears it, all
	// from different goroutines - unsynchronized that is a data race, and
	// Restart nilling the pointer while the handler dereferences it is a
	// crash.
	controlMu     sync.Mutex
	restartMutex  sync.Mutex
	usageMonitor  *web.Usage
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
	Fallback      string // decoy backend for non-tunnel requests (host:port), optional
	TLSEngine     string // "go" (default) or "openssl" for wss TLS termination
}

func NewWSServer(parentCtx context.Context, config *WsConfig, logger *logrus.Logger) *WsTransport {
	// Create a derived context from the parent context
	ctx, cancel := context.WithCancel(parentCtx)

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
	// for  webui
	if s.config.WebPort > 0 {
		go s.usageMonitor.Monitor()
	}

	s.config.TunnelStatus = fmt.Sprintf("Disconnected (%s)", s.config.Mode)

	go s.tunnelListener()

}
func (s *WsTransport) Restart() {
	if !s.restartMutex.TryLock() {
		s.logger.Warn("server restart already in progress, skipping restart attempt")
		return
	}
	defer s.restartMutex.Unlock()

	s.logger.Info("restarting server...")

	level := s.logger.Level
	s.logger.SetLevel(logrus.FatalLevel)

	if s.cancel != nil {
		s.cancel()
	}

	// Close control channel connection
	s.controlMu.Lock()
	if s.controlChannel != nil {
		s.controlChannel.Close()
	}
	s.controlChannel = nil
	s.controlMu.Unlock()

	time.Sleep(2 * time.Second)

	ctx, cancel := context.WithCancel(s.parentctx)
	s.ctx = ctx
	s.cancel = cancel

	// Re-initialize variables
	s.tunnelChannel = make(chan TunnelChannel, s.config.ChannelSize)
	s.localChannel = make(chan LocalTCPConn, s.config.ChannelSize)
	s.reqNewConnChan = make(chan struct{}, s.config.ChannelSize)
	s.usageMonitor = web.NewDataStore(fmt.Sprintf(":%v", s.config.WebPort), ctx, s.config.SnifferLog, s.config.Sniffer, &s.config.TunnelStatus, s.logger)
	s.config.TunnelStatus = ""

	// set the log level again
	s.logger.SetLevel(level)

	go s.Start()
}

// channelHandler drives one control channel. It takes the connection as an
// argument rather than reading s.controlChannel on every use: the field is
// shared with the HTTP handler and Restart, so a handler whose connection has
// died must not touch a pointer that by then may hold something else.
func (s *WsTransport) channelHandler(conn *network.WebSocketConn) {
	// A jittered timer (instead of a fixed-period ticker) so the heartbeat
	// cadence isn't perfectly periodic, which is an easy fingerprint for
	// traffic-pattern based DPI. wsmux/wssmux already do this.
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
					if s.cancel != nil {
						s.logger.Error("failed to read from channel connection. ", err)
						go s.Restart()
					}
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
				s.logger.Error("failed to send request new connection signal. ", err)
				go s.Restart()
				return
			}

		case <-heartbeatTimer.C:
			err := utils.WriteControlSignal(conn, utils.SG_HB)
			if err != nil {
				s.logger.Errorf("failed to send heartbeat signal. Error: %v.", err)
				go s.Restart()
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
				s.Restart()
				return

			default:
				s.logger.Errorf("unexpected response from channel: %v", msg)
				go s.Restart()
				return
			}

		}
	}
}

func (s *WsTransport) tunnelListener() {
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

			netConn, brw, _, err := ws.UpgradeHTTP(r, w)
			if err != nil {
				s.logger.Errorf("failed to upgrade connection from %s: %v", r.RemoteAddr, err)
				return
			}
			conn := network.NewWebSocketConn(netConn, ws.StateServerSide, brw.Reader)

			if r.URL.Path == channelPath {
				s.controlMu.Lock()
				if old := s.controlChannel; old != nil {
					s.controlMu.Unlock()
					s.logger.Warn("new control channel requested.")
					old.Close()
					conn.Close()
					go s.Restart()
					return
				}
				s.controlChannel = conn
				s.controlMu.Unlock()

				s.logger.Info("control channel established successfully")

				numCPU := runtime.NumCPU()
				if numCPU > 4 {
					numCPU = 4 // Max allowed handler is 4
				}

				go s.channelHandler(conn)
				go s.parsePortMappings()

				s.logger.Infof("starting %d handle loops on each CPU thread", numCPU)

				for i := 0; i < numCPU; i++ {
					go s.handleLoop()
				}

				s.config.TunnelStatus = fmt.Sprintf("Connected (%s)", s.config.Mode)

			} else if strings.HasPrefix(r.URL.Path, tunnelPathPrefix) {
				wsConn := TunnelChannel{
					conn: conn,
					ping: make(chan struct{}),
					mu:   &sync.Mutex{},
				}
				select {
				case s.tunnelChannel <- wsConn:
					go s.keepAlive(&wsConn)
					s.logger.Debugf("websocket connection accepted from %s", conn.RemoteAddr().String())
				default:
					s.logger.Warnf("websocket tunnel channel is full, closing connection from %s", conn.RemoteAddr().String())
					conn.Close()
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
			ln, err := network.NewTLSListener(s.config.TLSEngine, addr, certs, keys, 0, 0)
			if err != nil {
				s.logger.Fatalf("failed to create tls listener on %s: %v", addr, err)
			}
			if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
				s.logger.Fatalf("failed to listen on %s: %v", addr, err)
			}
		}()
	}

	<-s.ctx.Done()

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
					go s.localListener(localAddr, strconv.Itoa(port)) // Use port as the remoteAddr
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
					go s.localListener(localAddr, remoteAddr)
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
		go s.localListener(localAddr, remoteAddr)
	}
}

func (s *WsTransport) localListener(localAddr string, remoteAddr string) {
	portListener, err := net.Listen("tcp", localAddr)
	if err != nil {
		s.logger.Fatalf("failed to start listener on %s: %v", localAddr, err)
		return
	}

	//close local listener after context cancellation
	defer portListener.Close()

	s.logger.Infof("listener started successfully, listening on address: %s", portListener.Addr().String())

	go s.acceptLocalConn(portListener, remoteAddr)

	<-s.ctx.Done()
}

func (s *WsTransport) acceptLocalConn(listener net.Listener, remoteAddr string) {
	for {
		select {
		case <-s.ctx.Done():
			return

		default:
			s.logger.Debugf("waiting to accept incoming connection on %s", listener.Addr().String())
			conn := acceptWithBackoff(s.ctx, listener, s.logger)
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
			case s.localChannel <- LocalTCPConn{conn: conn, remoteAddr: remoteAddr, timeCreated: time.Now().UnixMilli()}:

				select {
				case s.reqNewConnChan <- struct{}{}:
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
	for {
		select {
		case <-s.ctx.Done():
			return
		case localConn := <-s.localChannel:
		loop:
			for {
				if time.Now().UnixMilli()-localConn.timeCreated > 3000 { // 3000ms
					s.logger.Debugf("timeouted local connection: %d ms", time.Now().UnixMilli()-localConn.timeCreated)
					localConn.conn.Close()
					break loop
				}

				select {
				case <-s.ctx.Done():
					return
				case tunnelConnection := <-s.tunnelChannel:
					close(tunnelConnection.ping)
					// Taken and deliberately never released: from here the
					// connection belongs to WSConnectionHandler, and the
					// keepAlive goroutine must never write another ping into the
					// middle of that data stream. It TryLocks and gives up, so
					// holding this forever is the handover. The mutex dies with
					// the per-connection struct, so nothing leaks - do not
					// "balance" it with an Unlock.
					tunnelConnection.mu.Lock()
					if err := tunnelConnection.conn.WriteMessage(network.TextMessage, []byte(localConn.remoteAddr)); err != nil {
						s.logger.Debugf("%v", err) // failed to send port number
						tunnelConnection.conn.Close()
						continue loop
					}
					// Handle data exchange between connections
					go handlers.WSConnectionHandler(s.ctx, s.config.ProxyProtocol, tunnelConnection.conn, localConn.conn, s.logger, s.usageMonitor, localConn.conn.LocalAddr().(*net.TCPAddr).Port, s.config.Sniffer)
					break loop
				}
			}
		}
	}
}

func (s *WsTransport) keepAlive(conn *TunnelChannel) {
	// Jittered like the control channel heartbeat: a pool of idle connections
	// all pinging on the same fixed period is a strong traffic fingerprint. The
	// payload deliberately stays a bare SG_Ping byte - clients match this frame
	// exactly, so padding it would break every client older than this change.
	pingTimer := time.NewTimer(utils.JitterDuration(s.config.Heartbeat))

	defer pingTimer.Stop()

	for {
		select {
		case <-s.ctx.Done():
			conn.conn.Close()
			return
		case <-conn.ping:
			s.logger.Trace("ping channel closed")
			return
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
