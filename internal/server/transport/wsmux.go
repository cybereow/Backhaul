package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gobwas/ws"
	"github.com/musix/backhaul/config" // for mode
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

	fallbackProxy http.Handler

	// controlMu guards controlChannel, handlersStarted and graceTimer. The
	// HTTP handler goroutine may be adopting a reattached control channel at
	// the same moment a dying channelHandler is clearing the old one.
	controlMu       sync.Mutex
	handlersStarted bool
	graceTimer      *time.Timer

	// events is a small ring buffer of disruption events (control-channel
	// losses, restarts, replacements) with timestamps, so an operator can see
	// *why* a tunnel dropped after the fact via the /diag endpoint instead of
	// having to catch it live in the logs.
	eventsMu sync.Mutex
	events   []transportEvent
}

// transportEvent is one recorded disruption: what happened, when, and any
// detail. Kept deliberately small - this is a diagnostic breadcrumb trail, not
// a metrics system.
type transportEvent struct {
	Time   time.Time `json:"time"`
	Kind   string    `json:"kind"`
	Detail string    `json:"detail,omitempty"`
}

// maxRecordedEvents caps the ring buffer; old events are dropped once full.
const maxRecordedEvents = 128

// recordEvent appends a disruption event, dropping the oldest once the ring is
// full. Safe to call from any goroutine.
func (s *WsMuxTransport) recordEvent(kind, detail string) {
	s.eventsMu.Lock()
	s.events = append(s.events, transportEvent{Time: time.Now(), Kind: kind, Detail: detail})
	if len(s.events) > maxRecordedEvents {
		s.events = s.events[len(s.events)-maxRecordedEvents:]
	}
	s.eventsMu.Unlock()
}

// snapshotEvents returns a copy of the recorded events, newest last.
func (s *WsMuxTransport) snapshotEvents() []transportEvent {
	s.eventsMu.Lock()
	defer s.eventsMu.Unlock()
	out := make([]transportEvent, len(s.events))
	copy(out, s.events)
	return out
}

// poolSnapshot reports the live pool grouped by CDN: how many sessions each CDN
// has and how many streams (flows) are currently running on them. Run during a
// transfer, it shows whether flows spread across the CDNs (good aggregation) or
// concentrate on one (the reason a many-flow workload wouldn't aggregate).
func (s *WsMuxTransport) poolSnapshot() map[string]interface{} {
	type cdnStat struct {
		CDN      string  `json:"cdn"`
		Sessions int     `json:"sessions"`
		Streams  int     `json:"streams"`
		RTTms    float64 `json:"rtt_ms,omitempty"`
	}

	s.sessionsMu.Lock()
	sessions := make([]*pooledSession, len(s.sessions))
	copy(sessions, s.sessions)
	s.sessionsMu.Unlock()

	byCDN := map[string]*cdnStat{}
	order := []string{}
	totalStreams := 0
	for _, ps := range sessions {
		st, ok := byCDN[ps.cdn]
		if !ok {
			st = &cdnStat{CDN: ps.cdn}
			byCDN[ps.cdn] = st
			order = append(order, ps.cdn)
		}
		st.Sessions++
		n := ps.session.NumStreams()
		st.Streams += n
		totalStreams += n
		if rtt := ps.rtt.Load(); rtt > 0 {
			st.RTTms = float64(rtt) / float64(time.Millisecond)
		}
	}

	// Sort CDNs by stream count, busiest first, so concentration is obvious.
	sort.SliceStable(order, func(i, j int) bool {
		return byCDN[order[i]].Streams > byCDN[order[j]].Streams
	})
	cdns := make([]*cdnStat, 0, len(order))
	for _, c := range order {
		cdns = append(cdns, byCDN[c])
	}

	return map[string]interface{}{
		"now":            time.Now(),
		"total_sessions": len(sessions),
		"distinct_cdns":  len(byCDN),
		"total_streams":  totalStreams,
		"per_cdn":        cdns,
	}
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

// controlGraceWindow is how long the pool is kept alive after the control
// channel drops while waiting for the client to reattach: long enough to ride
// out a CDN max-age reset plus a few dial retries, short enough that a client
// which really is gone doesn't leave a stale pool serving nothing.
// ponytail: a constant, not a knob - nothing to tune until a deployment needs
// a different window.
const controlGraceWindow = 30 * time.Second

// onControlLost handles a control channel that died on its own, as opposed to
// the client deliberately going away. Everything that actually carries traffic
// - the pool sessions, the port listeners, the handle loops - is independent of
// the control channel, so it all stays up and only the control channel is
// dropped. If the client hasn't reattached one within controlGraceWindow, fall
// back to the old behaviour and rebuild the whole transport.
func (s *WsMuxTransport) onControlLost(conn *network.WebSocketConn) {
	s.controlMu.Lock()
	if s.controlChannel != conn {
		// Already cleared, or the client has since reattached: this is a late
		// error from a connection nothing uses any more.
		s.controlMu.Unlock()
		conn.Close()
		return
	}
	s.controlChannel = nil

	if s.graceTimer != nil {
		s.graceTimer.Stop()
	}
	s.armControlGrace()
	s.controlMu.Unlock()

	conn.Close()
	s.controlMu.Lock()
	s.config.TunnelStatus = fmt.Sprintf("Reconnecting (%s)", s.config.Mode)
	s.controlMu.Unlock()
	s.recordEvent("control_lost", fmt.Sprintf("control channel dropped; holding pool up to %s for reattach (flows keep running)", controlGraceWindow))
	s.logger.Warnf("control channel lost, holding the pool for up to %s for the client to reattach", controlGraceWindow)
}

// armControlGrace (re)starts the grace timer that decides what to do when the
// control channel has not reattached. Caller holds controlMu. It is self-
// re-arming: the control channel carries no user data, only heartbeats and
// new-connection requests, so as long as the pool still has live sessions
// carrying flows there is nothing to gain from a restart - it would just drop
// every in-flight flow because a CDN was slow to reconnect a side channel.
// Under prolonged CDN flakiness (502/521 storms) that turned every reconnect
// delay into a full restart, which then cascaded to the client via SG_Closed.
// So restart only once the pool has actually drained (nothing left to
// preserve); until then keep holding and re-checking.
func (s *WsMuxTransport) armControlGrace() {
	s.graceTimer = time.AfterFunc(controlGraceWindow, s.onControlGraceExpired)
}

func (s *WsMuxTransport) onControlGraceExpired() {
	s.controlMu.Lock()
	if s.controlChannel != nil {
		// Reattached in the meantime; nothing to do.
		s.controlMu.Unlock()
		return
	}
	if live := atomic.LoadInt32(&s.sessionCounter); live > 0 {
		// Pool still carrying flows: hold, don't tear everything down.
		s.armControlGrace()
		s.controlMu.Unlock()
		s.logger.Warnf("control channel still not reattached, but %d pool session(s) alive; holding instead of restarting", live)
		s.recordEvent("control_hold", fmt.Sprintf("no control channel after %s but %d pool session(s) alive; holding (flows preserved)", controlGraceWindow, live))
		return
	}
	s.controlMu.Unlock()
	s.logger.Warn("control channel did not reattach and the pool is empty, restarting server")
	s.recordEvent("restart", fmt.Sprintf("control channel did not reattach within %s and pool is empty; full restart", controlGraceWindow))
	s.Restart()
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

func (s *WsMuxTransport) localListener(localAddr string, remoteAddr string) {
	// Force the socket buffers on the local ingress port (e.g. 6034), the same
	// way the tcp/tcpmux transports do. This port carries the user's traffic into
	// the tunnel; with a plain net.Listen it fell back to the OS default receive
	// buffer, which caps how fast the server can *read* an upload off a
	// high-RTT client connection (throughput ~= rcvbuf / RTT) - so upload was
	// throttled at ingress even though the tunnel legs themselves were tuned.
	listener, err := network.ListenWithBuffers("tcp", localAddr, s.config.SO_RCVBUF, s.config.SO_SNDBUF, 0, s.config.KeepAlive, !s.config.Nodelay)
	if err != nil {
		s.logger.Fatalf("failed to start listener on %s: %v", localAddr, err)
		return
	}

	//close local listener after context cancellation
	defer listener.Close()

	go s.acceptLocalConn(listener, remoteAddr)

	s.logger.Infof("listener started successfully, listening on address: %s", listener.Addr().String())

	<-s.ctx.Done()
}

func (s *WsMuxTransport) acceptLocalConn(listener net.Listener, remoteAddr string) {
	for {
		select {
		case <-s.ctx.Done():
			return

		default:
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
				s.logger.Debugf("accepted incoming TCP connection from %s", tcpConn.RemoteAddr().String())

				// +1 for stream counter
				atomic.AddInt32(&s.streamCounter, 1)

				if atomic.LoadInt32(&s.streamCounter) >= atomic.LoadInt32(&s.sessionCounter)*int32(s.config.MuxCon) {
					s.logger.Tracef("stream counter: %v, session counter: %v", atomic.LoadInt32(&s.streamCounter), atomic.LoadInt32(&s.sessionCounter))
					// Attempt to request a new connection
					select {
					case s.reqNewConnChan <- struct{}{}:
					default:
						s.logger.Warn("failed to request new connection. channel is full")
					}
				}

			default: // channel is full, discard the connection
				s.logger.Warnf("local listener channel is full, discarding TCP connection from %s", tcpConn.LocalAddr().String())
				conn.Close()
			}
		}
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
			// toward the lowest-latency CDN. Only on mux_version >= 2 (the peer
			// must be able to route the probe to its echoer) and only when a path
			// actually selects legs - striping, or the UDP flow path.
			if s.config.MuxVersion >= 2 && (s.config.StripeFactor > 1 || s.config.AcceptUDP || s.config.Speedtest) {
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

// pooledSession is one live pool connection plus the metadata leg selection
// scores it on: the CDN it arrived over (so a flow can be spread across distinct
// CDNs) and an EWMA round-trip estimate maintained by probeSessionRTT.
type pooledSession struct {
	session *smux.Session
	cdn     string       // CDN identity: the remote IP the pool connection arrived from
	rtt     atomic.Int64 // EWMA round-trip in nanoseconds; 0 until the first probe lands
}

// Leg-selection scoring. A leg's score is (open streams + 1) x its RTT in ms;
// the lowest score wins. This is capacity-weighted, not count-balanced: on a
// TCP-over-TCP-over-TLS leg a single stream's throughput ceiling is ~window/RTT,
// so RTT is a live proxy for how much a leg can carry. Weighting the placement
// cost by RTT makes a fast (low-RTT) CDN absorb proportionally more streams
// (~1/RTT) before its cost catches up to a slower CDN, while a slow CDN still
// takes a few - a weighted spread biased toward the good paths.
//
// This sits between two failure modes seen earlier. Even-count balancing (load
// the strongly dominant term, RTT a tiebreak) spread Ookla's parallel streams
// equally over every CDN, pouring half of them onto slow/high-RTT paths that
// each carried little, so the aggregate fell below the single best CDN. Pure
// additive RTT dominance did the opposite - it piled every stream onto one
// low-RTT session because load barely counted. The multiplicative form keeps
// load always significant (each stream multiplies the leg's cost) so it can
// never concentrate on one leg, yet still steers the bulk of the traffic toward
// the fastest CDNs instead of diluting it into the slow ones.
//
// unprobedRTTms is the neutral RTT charged to a session not yet probed (or on
// mux_version 1, where probing isn't possible), so a fresh session is neither
// unfairly preferred nor shunned before its first probe.
const (
	unprobedRTTms   = 40.0
	rttProbeEvery   = 5 * time.Second
	rttProbeTimeout = 10 * time.Second
)

// registerSession adds a pool session to the live registry and returns its
// wrapper so the caller can start probing it. unregisterSession removes it.
func (s *WsMuxTransport) registerSession(session *smux.Session) *pooledSession {
	ps := &pooledSession{session: session, cdn: cdnKey(session.RemoteAddr())}
	s.sessionsMu.Lock()
	s.sessions = append(s.sessions, ps)
	s.sessionsMu.Unlock()
	return ps
}

func (s *WsMuxTransport) unregisterSession(session *smux.Session) {
	s.sessionsMu.Lock()
	for i, ps := range s.sessions {
		if ps.session == session {
			s.sessions = append(s.sessions[:i], s.sessions[i+1:]...)
			break
		}
	}
	s.sessionsMu.Unlock()
}

// cdnKey reduces a pool connection's remote address to a CDN identity - the host
// (IP) without the ephemeral port - so two connections that arrived over the
// same CDN edge count as the same path for spreading purposes.
func cdnKey(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}
	return host
}

// legScore ranks a session for leg selection: lower is better. It combines live
// load (open streams on the session) with the measured RTT so selection favours
// the least-loaded, lowest-latency connection.
func legScore(ps *pooledSession) float64 {
	return legScoreValue(ps.session.NumStreams(), ps.rtt.Load())
}

// legScoreValue is the pure scoring math, split out from legScore so it can be
// tested without a live smux session. The cost of placing a stream on a leg is
// (load + 1) x RTT: the +1 gives an idle leg a nonzero cost so idle legs are
// still ranked by RTT (rather than all tying at zero and concentrating on the
// first one), and multiplying by RTT makes each additional stream cost more on a
// slower leg, so streams accrue on a leg in proportion to 1/RTT - the
// capacity-weighted spread described above. An unprobed session (rttNanos <= 0)
// is charged the neutral unprobedRTTms rather than 0, so a fresh session isn't
// falsely ranked as the fastest path.
func legScoreValue(load int, rttNanos int64) float64 {
	rttMs := unprobedRTTms
	if rttNanos > 0 {
		rttMs = float64(rttNanos) / float64(time.Millisecond)
	}
	return float64(load+1) * rttMs
}

// selectLegs picks the n best sessions for a flow: lowest score first, but
// spread across distinct CDNs before doubling up on any one. Pass 1 takes the
// best session from each distinct CDN (so a striped flow rides several CDNs, and
// a single-leg UDP flow lands on the best CDN); pass 2 fills any remaining slots
// by best score regardless of CDN, for when a flow wants more legs than there
// are CDNs. avail is sorted in place.
func selectLegs(avail []*pooledSession, n int, score func(*pooledSession) float64) []*pooledSession {
	sort.SliceStable(avail, func(i, j int) bool {
		return score(avail[i]) < score(avail[j])
	})

	chosen := make([]*pooledSession, 0, n)
	usedCDN := make(map[string]bool)
	for _, ps := range avail {
		if len(chosen) == n {
			break
		}
		if usedCDN[ps.cdn] {
			continue
		}
		usedCDN[ps.cdn] = true
		chosen = append(chosen, ps)
	}
	if len(chosen) < n {
		inChosen := make(map[*pooledSession]bool, len(chosen))
		for _, ps := range chosen {
			inChosen[ps] = true
		}
		for _, ps := range avail {
			if len(chosen) == n {
				break
			}
			if !inChosen[ps] {
				chosen = append(chosen, ps)
			}
		}
	}
	return chosen
}

// openStripedLegs opens one stream on each of n live sessions, chosen by
// selectLegs: load-balanced across the pool (so a many-flow workload spreads
// over every CDN and aggregates to the pool's full width) with RTT breaking
// ties toward the lowest-latency CDN. Used by both the striped TCP dispatcher
// (n = legsPerFlow) and the plain/UDP path (n = 1).
//
// It requires n distinct sessions and errors if fewer are live, rather than
// silently opening a narrower group: a reduced-width stripe means the two ends
// disagree on how many data shards a FEC flow has (the server sizes the encoder
// from the configured StripeFactor, the client from the leg count it received),
// so a partial group either fails to decode or truncates one direction. Callers
// treat the error as "pool not wide enough yet" - the striped dispatcher
// requeues and the promotion path stays plain - so the flow waits for the pool
// to grow instead of running mis-striped.
func (s *WsMuxTransport) openStripedLegs(n int) ([]*smux.Stream, error) {
	s.sessionsMu.Lock()
	avail := make([]*pooledSession, len(s.sessions))
	copy(avail, s.sessions)
	s.sessionsMu.Unlock()

	if len(avail) < n {
		return nil, fmt.Errorf("striping needs %d live pool session(s), only %d available", n, len(avail))
	}

	chosen := selectLegs(avail, n, legScore)
	streams := make([]*smux.Stream, 0, n)
	for _, ps := range chosen {
		stream, err := ps.session.OpenStream()
		if err != nil {
			for _, st := range streams {
				st.Close()
			}
			return nil, fmt.Errorf("failed to open stripe leg: %w", err)
		}
		streams = append(streams, stream)
	}
	return streams, nil
}

// probeSessionRTT keeps a session's RTT estimate current by opening a tiny ping
// stream every rttProbeEvery and timing the peer's echo, feeding the result into
// an EWMA. It runs only on mux_version >= 2 (flow kinds, so the peer can route
// the probe to its echoer) and only while striping is on, so a non-striped
// deployment pays nothing. It exits when the session closes or the transport
// shuts down.
func (s *WsMuxTransport) probeSessionRTT(ps *pooledSession) {
	ticker := time.NewTicker(rttProbeEvery)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ps.session.CloseChan():
			return
		case <-ticker.C:
			rtt, err := measureRTT(ps.session)
			if err != nil {
				// A transient probe failure (a busy session refusing a stream)
				// shouldn't discard a good estimate; just try again next tick.
				s.logger.Tracef("rtt probe on %s failed: %v", ps.cdn, err)
				continue
			}
			// EWMA (7/8 old, 1/8 new) so a single jittery sample doesn't swing
			// selection; the first sample seeds it directly.
			if old := ps.rtt.Load(); old > 0 {
				ps.rtt.Store((old*7 + rtt) / 8)
			} else {
				ps.rtt.Store(rtt)
			}
		}
	}
}

// measureRTT opens one ping stream, sends a nonce and times the echo. The stream
// carries no user data and is closed immediately after.
func measureRTT(session *smux.Session) (int64, error) {
	stream, err := session.OpenStream()
	if err != nil {
		return 0, err
	}
	defer stream.Close()
	if err := stream.SetDeadline(time.Now().Add(rttProbeTimeout)); err != nil {
		return 0, err
	}
	start := time.Now()
	if err := utils.SendFlowPing(stream, uint64(start.UnixNano())); err != nil {
		return 0, err
	}
	if _, err := utils.ReceiveFlowPing(stream); err != nil {
		return 0, err
	}
	return int64(time.Since(start)), nil
}

// speedtestResult is the JSON returned by the speedtest endpoint. In "best"
// scope the top-level CDN/*Mbps/*Bytes fields carry the single-session result;
// in "all" scope Total*Mbps carry the aggregate across every CDN run in
// parallel and PerCDN carries the per-connection breakdown.
type speedtestResult struct {
	Direction string  `json:"direction"`
	Seconds   int     `json:"seconds"`
	Scope     string  `json:"scope"`
	CDN       string  `json:"cdn,omitempty"`    // best scope: the pool connection the test ran over
	RTTms     float64 `json:"rtt_ms,omitempty"` // best scope: last measured RTT of that connection
	DownMbps  float64 `json:"down_mbps,omitempty"`
	UpMbps    float64 `json:"up_mbps,omitempty"`
	DownBytes int64   `json:"down_bytes,omitempty"`
	UpBytes   int64   `json:"up_bytes,omitempty"`

	TotalDownMbps float64       `json:"total_down_mbps,omitempty"` // all scope: aggregate across CDNs
	TotalUpMbps   float64       `json:"total_up_mbps,omitempty"`
	PerCDN        []perCDNSpeed `json:"per_cdn,omitempty"`

	Error string `json:"error,omitempty"`
}

// perCDNSpeed is one connection's contribution in an "all"-scope aggregate run.
type perCDNSpeed struct {
	CDN      string  `json:"cdn"`
	RTTms    float64 `json:"rtt_ms,omitempty"`
	DownMbps float64 `json:"down_mbps,omitempty"`
	UpMbps   float64 `json:"up_mbps,omitempty"`
}

// handleSpeedtestRequest runs a live throughput test over the pool and returns
// the result as JSON. Query params:
//   - dir=down|up|both (default both)
//   - seconds=1..30 (default 10)
//   - scope=best|all (default best): "best" rides one stream on the best
//     (CDN-aware) session - single-flow throughput over the chosen CDN; "all"
//     runs every distinct CDN in parallel and reports the aggregate - the whole
//     connection's combined throughput.
func (s *WsMuxTransport) handleSpeedtestRequest(w http.ResponseWriter, r *http.Request) {
	dir := r.URL.Query().Get("dir")
	switch dir {
	case "down", "up", "both":
	case "":
		dir = "both"
	default:
		writeJSON(w, http.StatusBadRequest, speedtestResult{Error: "dir must be down, up or both"})
		return
	}

	scope := r.URL.Query().Get("scope")
	switch scope {
	case "best", "all":
	case "":
		scope = "best"
	default:
		writeJSON(w, http.StatusBadRequest, speedtestResult{Error: "scope must be best or all"})
		return
	}

	seconds := 10
	if v := r.URL.Query().Get("seconds"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 30 {
			writeJSON(w, http.StatusBadRequest, speedtestResult{Error: "seconds must be an integer between 1 and 30"})
			return
		}
		seconds = n
	}

	res := s.runSpeedtest(dir, scope, seconds)
	status := http.StatusOK
	if res.Error != "" {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, res)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// runSpeedtest measures tunnel throughput. In "best" scope it runs one stream
// on the best (least load, least latency) session; in "all" scope it runs every
// distinct CDN in parallel and aggregates.
func (s *WsMuxTransport) runSpeedtest(dir, scope string, seconds int) speedtestResult {
	res := speedtestResult{Direction: dir, Seconds: seconds, Scope: scope}

	if s.config.MuxVersion < 2 {
		res.Error = "speedtest requires mux_version >= 2 on both ends"
		return res
	}

	s.sessionsMu.Lock()
	avail := make([]*pooledSession, len(s.sessions))
	copy(avail, s.sessions)
	s.sessionsMu.Unlock()
	if len(avail) == 0 {
		res.Error = "no active pool sessions (is the client connected?)"
		return res
	}

	dur := time.Duration(seconds) * time.Second
	if scope == "all" {
		return s.runSpeedtestAll(avail, dir, seconds, dur)
	}

	ps := selectLegs(avail, 1, legScore)[0]
	res.CDN = ps.cdn
	if rtt := ps.rtt.Load(); rtt > 0 {
		res.RTTms = float64(rtt) / float64(time.Millisecond)
	}

	if dir == "down" || dir == "both" {
		bytes, el, err := s.speedtestOnce(ps.session, utils.SpeedtestDownload, seconds, dur)
		if err != nil {
			res.Error = "download: " + err.Error()
			return res
		}
		res.DownBytes = bytes
		res.DownMbps = utils.SpeedtestMbps(bytes, el)
	}
	if dir == "up" || dir == "both" {
		bytes, el, err := s.speedtestOnce(ps.session, utils.SpeedtestUpload, seconds, dur)
		if err != nil {
			res.Error = "upload: " + err.Error()
			return res
		}
		res.UpBytes = bytes
		res.UpMbps = utils.SpeedtestMbps(bytes, el)
	}
	return res
}

// speedtestOnce runs one direction of the test on a fresh stream and returns the
// receiver-measured bytes and elapsed time. On download the server sources the
// data and the client sinks and reports back; on upload the client sources and
// the server sinks and measures directly.
func (s *WsMuxTransport) speedtestOnce(session *smux.Session, mode byte, seconds int, dur time.Duration) (int64, time.Duration, error) {
	stream, err := session.OpenStream()
	if err != nil {
		return 0, 0, err
	}
	defer stream.Close()

	if err := utils.SendFlowSpeedtest(stream, mode, uint32(seconds)); err != nil {
		return 0, 0, err
	}
	if mode == utils.SpeedtestDownload {
		if err := utils.SpeedtestSource(stream, dur); err != nil {
			return 0, 0, err
		}
		return utils.ReadSpeedtestReport(stream)
	}
	return utils.SpeedtestSink(stream)
}

// runSpeedtestAll measures the whole connection's throughput: it runs the test
// on the best session of every distinct CDN in parallel and sums the
// receiver-measured bytes over the test window. Because the runs are concurrent
// they contend for any shared bottleneck (e.g. one origin uplink behind several
// CDNs), so the aggregate honestly reflects what the pool can move at once
// rather than an inflated sum of isolated runs.
func (s *WsMuxTransport) runSpeedtestAll(avail []*pooledSession, dir string, seconds int, dur time.Duration) speedtestResult {
	res := speedtestResult{Direction: dir, Seconds: seconds, Scope: "all"}
	targets := bestPerCDN(avail, legScore)

	per := make([]perCDNSpeed, len(targets))
	for i, ps := range targets {
		per[i].CDN = ps.cdn
		if rtt := ps.rtt.Load(); rtt > 0 {
			per[i].RTTms = float64(rtt) / float64(time.Millisecond)
		}
	}

	// runPhase runs one direction on every target concurrently and returns each
	// target's receiver-measured byte count (index-aligned with targets).
	runPhase := func(mode byte) ([]int64, error) {
		bytes := make([]int64, len(targets))
		errs := make([]error, len(targets))
		var wg sync.WaitGroup
		for i, ps := range targets {
			wg.Add(1)
			go func(i int, ps *pooledSession) {
				defer wg.Done()
				b, _, err := s.speedtestOnce(ps.session, mode, seconds, dur)
				bytes[i] = b
				errs[i] = err
			}(i, ps)
		}
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				return bytes, fmt.Errorf("cdn %s: %w", targets[i].cdn, err)
			}
		}
		return bytes, nil
	}

	// Aggregate throughput is the summed bytes over the common test window, so
	// concurrent runs that share a bottleneck don't add up beyond it.
	if dir == "down" || dir == "both" {
		b, err := runPhase(utils.SpeedtestDownload)
		if err != nil {
			res.Error = "download: " + err.Error()
			return res
		}
		var sum int64
		for i, x := range b {
			per[i].DownMbps = utils.SpeedtestMbps(x, dur)
			sum += x
		}
		res.TotalDownMbps = utils.SpeedtestMbps(sum, dur)
	}
	if dir == "up" || dir == "both" {
		b, err := runPhase(utils.SpeedtestUpload)
		if err != nil {
			res.Error = "upload: " + err.Error()
			return res
		}
		var sum int64
		for i, x := range b {
			per[i].UpMbps = utils.SpeedtestMbps(x, dur)
			sum += x
		}
		res.TotalUpMbps = utils.SpeedtestMbps(sum, dur)
	}

	res.PerCDN = per
	return res
}

// bestPerCDN returns the best-scoring session for each distinct CDN, so an
// aggregate run uses one connection per path rather than several on the same one.
func bestPerCDN(avail []*pooledSession, score func(*pooledSession) float64) []*pooledSession {
	ordered := make([]*pooledSession, len(avail))
	copy(ordered, avail)
	sort.SliceStable(ordered, func(i, j int) bool {
		return score(ordered[i]) < score(ordered[j])
	})
	seen := make(map[string]bool)
	out := make([]*pooledSession, 0, len(ordered))
	for _, ps := range ordered {
		if seen[ps.cdn] {
			continue
		}
		seen[ps.cdn] = true
		out = append(out, ps)
	}
	return out
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

func (s *WsMuxTransport) shouldStripe(incomingConn LocalTCPConn) bool {
	if len(s.config.StripePorts) > 0 {
		remotePort := ""
		if _, port, err := net.SplitHostPort(incomingConn.remoteAddr); err == nil {
			remotePort = port
		} else {
			remotePort = incomingConn.remoteAddr
		}
		for _, p := range s.config.StripePorts {
			if p == remotePort {
				return true
			}
		}
		return false
	}
	return s.config.StripeFactor > 1
}

func (s *WsMuxTransport) acquirePlainSlot() bool {
	for {
		active := atomic.LoadInt32(&s.plainFlows)
		budget := atomic.LoadInt32(&s.sessionCounter) * int32(s.config.MuxCon)
		if budget < 1 {
			budget = 1 // always admit at least one flow while the pool warms up
		}
		if (active + 1) <= budget {
			if atomic.CompareAndSwapInt32(&s.plainFlows, active, active+1) {
				return true
			}
			continue // lost the CAS race, re-read and retry
		}
		// Budget full: ask for a replacement session so the budget grows.
		select {
		case s.reqNewConnChan <- struct{}{}:
		default:
		}
		select {
		case <-s.ctx.Done():
			return false
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (s *WsMuxTransport) dispatchPlain(incomingConn LocalTCPConn) {
	if time.Now().UnixMilli()-incomingConn.timeCreated > 3000 { // 3000ms
		s.logger.Debugf("timeouted local connection: %d ms", time.Now().UnixMilli()-incomingConn.timeCreated)
		incomingConn.conn.Close()
		atomic.AddInt32(&s.streamCounter, -1)
		return
	}

	if !s.acquirePlainSlot() {
		incomingConn.conn.Close()
		atomic.AddInt32(&s.streamCounter, -1)
		return
	}

	legs, err := s.openStripedLegs(1)
	if err != nil {
		atomic.AddInt32(&s.plainFlows, -1)
		s.logger.Tracef("plain dispatch: %v, retrying shortly", err)
		time.Sleep(100 * time.Millisecond)
		incomingConn.timeCreated = time.Now().UnixMilli()
		s.requeueOrDrop(incomingConn)
		return
	}

	stream := legs[0]

	var flowID uint64
	promotable := s.config.MuxVersion >= 2 && s.config.PromoteBytes > 0

	if s.config.MuxVersion >= 2 {
		// A non-zero flowID signals the client this flow is promotable and must
		// be run through the promotable pump; flowID 0 means a plain flow.
		if promotable {
			flowID = uint64(time.Now().UnixNano())
		}
		if err := utils.SendFlowPlain(stream, flowID, incomingConn.remoteAddr); err != nil {
			atomic.AddInt32(&s.plainFlows, -1)
			stream.Close()
			s.requeueOrDrop(incomingConn)
			return
		}
	} else {
		if err := utils.SendBinaryString(stream, incomingConn.remoteAddr); err != nil {
			atomic.AddInt32(&s.plainFlows, -1)
			stream.Close()
			s.requeueOrDrop(incomingConn)
			return
		}
	}

	defer atomic.AddInt32(&s.plainFlows, -1)
	defer atomic.AddInt32(&s.streamCounter, -1)
	if promotable {
		// dispatchPromotable blocks until the flow (and any mid-stream
		// promotion) completes, so the counters above are released only when
		// the flow is truly done.
		s.dispatchPromotable(incomingConn.conn, stream, flowID, incomingConn.remoteAddr)
	} else {
		handlers.TCPConnectionHandler(s.ctx, s.config.ProxyProtocol, incomingConn.conn, stream, s.logger, s.usageMonitor, incomingConn.conn.LocalAddr().(*net.TCPAddr).Port, s.config.Sniffer)
	}
}

// dispatchStriped opens the striped legs for one incoming connection, sends the
// per-leg headers, and pumps the flow. streamCounter (incremented in
// localListener when the connection was accepted) is decremented exactly once
// for every connection that leaves this pipeline - dropped here, or handed to a
// handler that later finishes - mirroring handleSession on the non-striped path.
// A connection requeued onto localChannel keeps its count, since it passes
// through here again.
func (s *WsMuxTransport) dispatchStriped(incomingConn LocalTCPConn) {
	if time.Now().UnixMilli()-incomingConn.timeCreated > 3000 { // 3000ms
		s.logger.Debugf("timeouted local connection: %d ms", time.Now().UnixMilli()-incomingConn.timeCreated)
		incomingConn.conn.Close()
		atomic.AddInt32(&s.streamCounter, -1)
		return
	}

	// Bound concurrent striped flows to the pool's stream budget
	// (sessionCounter*MuxCon): each flow opens StripeFactor legs and drives a
	// local dial, so running more flows than the sessions can carry just floods
	// the pool and the local service. The bound scales with the live pool - as
	// load makes streamCounter request more sessions, more flows are admitted -
	// mirroring the non-striped path's per-session MuxCon cap. In the common
	// under-budget case the slot is taken immediately (no added latency); only a
	// genuinely saturated pool makes a new flow wait instead of piling on.
	if !s.acquireStripedSlot() {
		// ctx cancelled while waiting for a slot
		incomingConn.conn.Close()
		atomic.AddInt32(&s.streamCounter, -1)
		return
	}
	// The admission slot is released explicitly on each exit path rather than
	// via a single defer: a setup that fails and requeues must give the slot
	// back *before* the backoff sleep and requeue, otherwise it shrinks capacity
	// for everyone else while doing nothing.

	legs, err := s.openStripedLegs(s.legsPerFlow())
	if err != nil {
		atomic.AddInt32(&s.stripedFlows, -1) // release before backoff + requeue
		s.logger.Tracef("striped dispatch: %v, retrying shortly", err)
		time.Sleep(100 * time.Millisecond)
		// Refresh the creation time: a requeued conn keeps its original
		// timestamp otherwise, so the 3s setup-timeout check at the top would
		// almost always drop it on the retry, making the "retry" effectively
		// dead.
		incomingConn.timeCreated = time.Now().UnixMilli()
		s.requeueOrDrop(incomingConn)
		return
	}

	gid := atomic.AddUint32(&s.stripeGroupID, 1)
	for i, stream := range legs {
		var err error
		if s.config.MuxVersion >= 2 {
			err = utils.SendFlowStriped(stream, gid, uint8(i), uint8(len(legs)), uint8(s.config.StripeParity), incomingConn.remoteAddr)
		} else {
			err = utils.SendStripeHeader(stream, gid, uint8(i), uint8(len(legs)), uint8(s.config.StripeParity), incomingConn.remoteAddr)
		}
		if err != nil {
			atomic.AddInt32(&s.stripedFlows, -1) // release before requeue
			s.logger.Tracef("failed to send stripe header: %v", err)
			for _, st := range legs {
				st.Close()
			}
			incomingConn.timeCreated = time.Now().UnixMilli()
			s.requeueOrDrop(incomingConn)
			return
		}
	}

	conns := make([]net.Conn, len(legs))
	for i, st := range legs {
		conns[i] = st
	}
	var stripedConn net.Conn
	if s.config.StripeParity > 0 {
		fecConn, err := striping.NewFEC(conns, striping.DefaultChunkSize, s.config.StripeFactor, s.config.StripeParity)
		if err != nil {
			atomic.AddInt32(&s.stripedFlows, -1)
			s.logger.Errorf("striped dispatch: %v", err)
			for _, st := range legs {
				st.Close()
			}
			atomic.AddInt32(&s.streamCounter, -1)
			incomingConn.conn.Close()
			return
		}
		stripedConn = fecConn
	} else {
		stripedConn = striping.New(conns, striping.DefaultChunkSize)
	}

	defer atomic.AddInt32(&s.stripedFlows, -1) // hold the slot for the flow's lifetime
	defer atomic.AddInt32(&s.streamCounter, -1)
	handlers.TCPConnectionHandler(s.ctx, s.config.ProxyProtocol, incomingConn.conn, stripedConn, s.logger, s.usageMonitor, incomingConn.conn.LocalAddr().(*net.TCPAddr).Port, s.config.Sniffer)
}

// legsPerFlow is how many pool legs one striped flow opens: the plain
// stripe factor, plus any FEC parity legs on top.
func (s *WsMuxTransport) legsPerFlow() int {
	return s.config.StripeFactor + s.config.StripeParity
}

// acquireStripedSlot reserves one in-flight striped-flow slot, blocking (with a
// short poll) while the pool is at its stream budget so a burst of new flows
// can't open more legs than the sessions can carry. The budget is
// sessionCounter*MuxCon streams and each flow uses StripeFactor legs, so at most
// sessionCounter*MuxCon/StripeFactor flows run at once; the budget grows as the
// pool does. Returns false only if the transport is shutting down.
func (s *WsMuxTransport) acquireStripedSlot() bool {
	sf := int32(s.legsPerFlow())
	if sf < 1 {
		sf = 1
	}
	for {
		active := atomic.LoadInt32(&s.stripedFlows)
		budget := atomic.LoadInt32(&s.sessionCounter) * int32(s.config.MuxCon)
		if budget < sf {
			budget = sf // always admit at least one flow while the pool warms up
		}
		if (active+1)*sf <= budget {
			if atomic.CompareAndSwapInt32(&s.stripedFlows, active, active+1) {
				return true
			}
			continue // lost the CAS race, re-read and retry
		}
		// At budget: ask the client to dial another pool session so the budget
		// can grow. Under striping the usual growth trigger in localListener
		// (streamCounter >= sessionCounter*MuxCon) rarely fires - streamCounter
		// counts logical flows, but each flow consumes StripeFactor streams, so
		// the slot cap is hit long before that threshold and the pool would
		// otherwise never grow to meet striped demand.
		select {
		case s.reqNewConnChan <- struct{}{}:
		default:
		}
		select {
		case <-s.ctx.Done():
			return false
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// requeueOrDrop puts a connection whose striped setup could not complete back on
// localChannel to be retried, or closes it (and releases its streamCounter slot)
// if the channel is full.
func (s *WsMuxTransport) requeueOrDrop(incomingConn LocalTCPConn) {
	select {
	case s.localChannel <- incomingConn:
	default:
		incomingConn.conn.Close()
		atomic.AddInt32(&s.streamCounter, -1)
	}
}

func (s *WsMuxTransport) handleSession(session *smux.Session) {
	counter := make(chan struct{}, s.config.MuxCon)
	defer session.Close() // runs after retireSession below has drained the session
	defer close(counter)

	// Retire this session before it gets old enough for the CDN's own max-age
	// reset to land on it. The age is jittered because the initial pool is
	// dialled all at once - on a fixed age every connection would rotate in the
	// same second, which is both a reconnect storm and a nice periodic
	// signature. A nil channel (rotation disabled) blocks forever in the select.
	var rotate <-chan time.Time
	if s.config.MaxConnAge > 0 {
		rotateTimer := time.NewTimer(utils.JitterDuration(s.config.MaxConnAge))
		defer rotateTimer.Stop()
		rotate = rotateTimer.C
	}
	// replaced closes once a replacement pool connection has actually been
	// admitted. Nil until rotation starts, so the select ignores it.
	var replaced chan struct{}

	for {
		// +1 for mux connection counter
		counter <- struct{}{}

		select {
		case <-s.ctx.Done():
			return

		case <-rotate:
			<-counter // hand back the slot reserved above; no connection used it
			rotate = nil

			// Make before break: order the replacement, but keep serving on this
			// connection until it is actually up. That is the point of waiting -
			// if the client cannot dial (edge IP blackholed, CDN refusing the
			// upgrade) an aging connection still carries traffic until the CDN
			// resets it, while a closed one carries nothing.
			replaced = make(chan struct{})
			go func(ch chan struct{}) {
				if s.awaitReplacement(session) {
					close(ch)
				}
			}(replaced)
			continue

		case <-replaced:
			<-counter // hand back the slot reserved above; no connection used it
			atomic.AddInt32(&s.sessionCounter, -1)
			s.retireSession(session)
			return

		case incomingConn := <-s.localChannel:
			if time.Now().UnixMilli()-incomingConn.timeCreated > 3000 { // 3000ms
				s.logger.Debugf("timeouted local connection: %d ms", time.Now().UnixMilli()-incomingConn.timeCreated)
				incomingConn.conn.Close()

				// Decrement the counter
				atomic.AddInt32(&s.streamCounter, -1)
				<-counter
				continue
			}

			stream, err := session.OpenStream()
			if err != nil {
				s.handleSessionError(&incomingConn, err)
				return
			}

			var flowID uint64
			var promotable bool

			if s.config.MuxVersion >= 2 && s.config.PromoteBytes > 0 {
				promotable = true
				flowID = uint64(time.Now().UnixNano())
				if err := utils.SendFlowPlain(stream, flowID, incomingConn.remoteAddr); err != nil {
					s.logger.Tracef("failed to send plain flow header: %v", err)
					stream.Close()
					select {
					case s.localChannel <- incomingConn:
					default:
						incomingConn.conn.Close()
						atomic.AddInt32(&s.streamCounter, -1)
					}
					<-counter
					continue
				}
			} else {
				// Send the target port over the tunnel connection (legacy mode)
				if err := utils.SendBinaryString(stream, incomingConn.remoteAddr); err != nil {
					s.logger.Tracef("failed to send address over stream: %v", err)
					// Close the stream that never served traffic - leaving it open
					// leaks the smux stream. Requeue non-blocking (a full
					// localChannel would otherwise park this goroutine forever and
					// permanently burn a MuxCon slot), dropping the conn if full.
					stream.Close()
					select {
					case s.localChannel <- incomingConn:
					default:
						incomingConn.conn.Close()
						atomic.AddInt32(&s.streamCounter, -1)
					}
					<-counter // release the mux slot reserved at the top of the loop
					continue
				}
			}

			// Handle data exchange between connections
			go func() {
				if promotable {
					s.dispatchPromotable(incomingConn.conn, stream, flowID, incomingConn.remoteAddr)
				} else {
					handlers.TCPConnectionHandler(s.ctx, s.config.ProxyProtocol, incomingConn.conn, stream, s.logger, s.usageMonitor, incomingConn.conn.LocalAddr().(*net.TCPAddr).Port, s.config.Sniffer)
				}
				atomic.AddInt32(&s.streamCounter, -1)
				<-counter // read signal from the channel
			}()
		}
	}
}

// rotateRetryInterval is how long rotation waits before re-checking for the
// replacement connection it asked for. Deliberately unhurried: the connection
// is only aging, and the client may be unable to dial at all for minutes.
const rotateRetryInterval = 30 * time.Second

// requestReplacement asks the client to bring up one more pool connection.
func (s *WsMuxTransport) requestReplacement() {
	select {
	case s.reqNewConnChan <- struct{}{}:
	default:
		s.logger.Warn("failed to request a replacement connection for rotation. channel is full")
	}
}

// retireSession takes a pool session out of service before it is old enough
// for a CDN/LB max-age reset to kill it mid-flow, waiting for the streams
// already running on it to finish. Callers only get here once a replacement
// connection has actually joined the pool (see requestReplacement), so this
// never shrinks the pool. The caller closes the session once this returns.
//
// Draining is the point: a long-lived flow - an SSH session, a large download -
// is pinned to one connection because smux cannot migrate a live stream, so
// cutting the connection at rotation time would cut the flow. Instead the
// retiring connection takes no new streams and stays up until its last one
// ends. That buys such a flow the whole window up to the CDN's hard limit; the
// limit itself is not something client-side code can extend.
func (s *WsMuxTransport) retireSession(session *smux.Session) {
	s.logger.Debugf("retiring pool session at max_conn_age, %d live stream(s) to drain", session.NumStreams())

	// ponytail: poll for drain rather than wiring per-stream completion
	// signalling; a 1s tick is plenty for a connection on its way out.
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		if session.IsClosed() {
			return
		}
		if session.NumStreams() == 0 {
			s.logger.Debug("retired pool session drained, closing")
			return
		}
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// rotateStripedSession is the striped path's equivalent of the rotation branch
// in handleSession: at max_conn_age the session is pulled out of the leg pool
// so no new stripe legs land on it, then drained and closed. The sessionCounter
// decrement is left to the CloseChan watcher registered in handleLoop.
func (s *WsMuxTransport) rotateStripedSession(session *smux.Session) {
	rotateTimer := time.NewTimer(utils.JitterDuration(s.config.MaxConnAge))
	defer rotateTimer.Stop()

	select {
	case <-s.ctx.Done():
		return
	case <-session.CloseChan():
		return
	case <-rotateTimer.C:
	}

	// Make before break. Unlike the non-striped path this can block: the session
	// keeps serving legs from the registry until unregisterSession below, so
	// waiting here costs no capacity.
	if !s.awaitReplacement(session) {
		return
	}

	s.unregisterSession(session)
	s.retireSession(session)
	session.Close()
}

// awaitReplacement asks for a replacement pool connection and waits until one
// has actually been admitted, re-asking on every retry because the client's
// tunnelDialer abandons a failed dial for good. Returns false if the context
// ended or the session died while waiting - in both cases there is nothing left
// to rotate. It never gives up otherwise: retiring a connection the pool has no
// replacement for would leave less capacity than before rotation started.
func (s *WsMuxTransport) awaitReplacement(session *smux.Session) bool {
	// Mark first, ask second: a replacement admitted from here on counts.
	mark := atomic.LoadInt32(&s.admittedSessions)
	s.requestReplacement()

	// ponytail: poll the counter instead of signalling admissions to whoever is
	// waiting. Rotation is not latency-sensitive - a second either way is noise
	// against a max_conn_age measured in minutes.
	poll := time.NewTicker(time.Second)
	defer poll.Stop()
	reask := time.NewTicker(utils.JitterDuration(rotateRetryInterval))
	defer reask.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return false
		case <-session.CloseChan():
			return false
		case <-reask.C:
			s.logger.Debugf("rotation deferred: replacement pool connection is not up, keeping the aging one in service (re-asking every ~%s)", rotateRetryInterval)
			s.requestReplacement()
		case <-poll.C:
		}
		if atomic.LoadInt32(&s.admittedSessions) > mark {
			return true
		}
	}
}

func (s *WsMuxTransport) handleSessionError(incomingConn *LocalTCPConn, err error) {
	s.logger.Tracef("failed to handle session: %v", err)

	// decrease session value
	atomic.AddInt32(&s.sessionCounter, -1)

	// Put local connection back to local channel (non-blocking): a blocking
	// send on a full localChannel would park this goroutine forever, and the
	// caller returns right after this - leaking the session and its counter slot.
	select {
	case s.localChannel <- *incomingConn:
	default:
		incomingConn.conn.Close()
		atomic.AddInt32(&s.streamCounter, -1)
	}

	// Attempt to request a new connection
	select {
	case s.reqNewConnChan <- struct{}{}:
	default:
		s.logger.Warn("request new connection channel is full")
	}
}

func (s *WsMuxTransport) dispatchPromotable(appConn net.Conn, plainStream net.Conn, flowID uint64, remoteAddr string) {
	swapper := handlers.PromotablePump(s.ctx, s.config.ProxyProtocol, appConn, plainStream, s.logger, s.usageMonitor, appConn.LocalAddr().(*net.TCPAddr).Port, s.config.Sniffer)

	if swapper == nil {
		return // failed proxy protocol
	}

	go func() {
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-swapper.DoneWait(): // Need a wait channel
				return
			case <-time.After(100 * time.Millisecond):
				if swapper.UpBytes() >= s.config.PromoteBytes {
					if s.shouldPromote() {
						s.promoteFlow(flowID, remoteAddr, swapper, plainStream)
					}
					// Whether or not it promoted, stop checking: a flow that
					// stayed plain because the pool was busy keeps running plain
					// (already spread across CDNs by load-balancing) for its life.
					return
				}
			}
		}
	}()

	// Block until the flow (and any promotion) is fully done, so the caller's
	// pool-slot accounting is released only when the flow actually finishes.
	<-swapper.DoneWait()
}

// shouldPromote decides whether a flow that has crossed promote_bytes should
// migrate to a striped group or stay plain. Striping one flow across legsPerFlow
// legs helps a single heavy flow reach several CDNs it otherwise couldn't. But
// when many flows are already active, plain load-balancing is already spreading
// them across every CDN, and promoting each one (each grabbing legsPerFlow legs)
// over-subscribes the pool and couples the legs under in-order reassembly -
// which collapses aggregate throughput instead of raising it (the "climbs then
// crashes mid-test" upload symptom). So only promote while few enough flows are
// active that their striped groups still fit the pool on distinct CDNs; beyond
// that, staying plain aggregates better.
func (s *WsMuxTransport) shouldPromote() bool {
	s.sessionsMu.Lock()
	cdns := make(map[string]struct{}, len(s.sessions))
	for _, ps := range s.sessions {
		cdns[ps.cdn] = struct{}{}
	}
	distinct := len(cdns)
	s.sessionsMu.Unlock()

	legs := s.legsPerFlow()
	if legs < 1 {
		legs = 1
	}
	budget := int32(distinct / legs)
	if budget < 1 {
		budget = 1 // always let a single heavy flow promote, even with one CDN
	}
	return atomic.LoadInt32(&s.plainFlows) <= budget
}

func (s *WsMuxTransport) promoteFlow(flowID uint64, remoteAddr string, swapper *handlers.PumpSwapper, plainStream net.Conn) {
	// 2. Open legs
	legs, err := s.openStripedLegs(s.legsPerFlow())
	if err != nil {
		s.logger.Tracef("failed to open striped legs for promotion: %v", err)
		return // abort, flow continues on plain
	}

	gid := atomic.AddUint32(&s.stripeGroupID, 1)
	for i, stream := range legs {
		if err := utils.SendFlowPromote(stream, flowID, gid, uint8(i), uint8(len(legs)), uint8(s.config.StripeParity)); err != nil {
			s.logger.Tracef("failed to send promote header: %v", err)
			for _, st := range legs {
				st.Close()
			}
			return
		}
	}

	conns := make([]net.Conn, len(legs))
	for i, st := range legs {
		conns[i] = st
	}

	var serverStriped net.Conn
	if s.config.StripeParity > 0 {
		fecConn, err := striping.NewFEC(conns, striping.DefaultChunkSize, s.config.StripeFactor, s.config.StripeParity)
		if err != nil {
			s.logger.Errorf("promotion striped dispatch: %v", err)
			for _, st := range legs {
				st.Close()
			}
			return
		}
		serverStriped = fecConn
	} else {
		serverStriped = striping.New(conns, striping.DefaultChunkSize)
	}

	own := swapper.FreezeUp() // server is sending to Client (download direction for user)

	// Write own on leg 0 of the new stripe groups
	if err := utils.WriteCount(legs[0], own); err != nil {
		serverStriped.Close()
		return
	}

	// 5. Server reads peer off leg 0
	peer, err := utils.ReadCount(legs[0])
	if err != nil {
		serverStriped.Close()
		return
	}

	// 7. Resume: switch swapper
	swapper.Install(serverStriped, peer)
}
