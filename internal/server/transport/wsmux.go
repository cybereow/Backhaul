package transport

import (
	"context"
	"fmt"
	"io"
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

// restartJoinTimeout bounds how long Restart waits for the previous generation's
// workers to end before it publishes the next one. Every blocking point of a
// worker is woken by cancellation or by closing a socket the generation owns, so
// this is a safety valve, not an expected wait; a var so tests can shorten it.
var restartJoinTimeout = 30 * time.Second

// wsGeneration is everything one run of the transport owns: the context its
// workers run under, every worker goroutine, and every socket or session they
// use - queued, admitted, draining or handshaking alike. It is deliberately
// separate from s.sessions, which only says which sessions may be *picked* for a
// flow (rotation unregisters a session while it still carries streams).
//
// Restart stops a generation, waits for its workers, and only then publishes the
// next one, so a worker of generation N can never observe or mutate the state of
// generation N+1: the transport fields it reads (ctx, counters, channels) are
// republished only once it has returned.
//
// Ownership is registered before use: a worker is counted (start/enter) before
// it runs and a socket is owned right after it exists, before smux or admission
// wraps it. Once stopped, a generation refuses both, so nothing can slip in
// behind the teardown.
//
// A nil *wsGeneration is valid and tracks nothing (goroutines just run, sockets
// are not registered).
type wsGeneration struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu      sync.Mutex
	stopped bool
	owned   map[io.Closer]struct{}

	// rotate is the rotation decision gate (capacity 1, see awaitReplacement):
	// held only while one aging session waits for its own replacement and is
	// taken out of eligibility, never while it drains. Owned by the generation so
	// a Restart starts with a free gate whatever became of the old one.
	rotate chan struct{}
}

func newWsGeneration(parent context.Context) *wsGeneration {
	ctx, cancel := context.WithCancel(parent)
	g := &wsGeneration{ctx: ctx, cancel: cancel, owned: make(map[io.Closer]struct{}), rotate: make(chan struct{}, 1)}
	// However the context ends - Restart, or the parent (process shutdown) being
	// cancelled - the owned sockets are closed: ctx alone cannot wake a blocked
	// read or accept.
	context.AfterFunc(ctx, g.stop)
	return g
}

// start runs f as a worker of g. It reports false, without running f, once g is
// stopped. Workers may start further workers: a parent that is still running
// keeps the count above zero, so this never races the join.
func (g *wsGeneration) start(f func()) bool {
	if !g.enter() {
		return false
	}
	go func() {
		defer g.exit()
		f()
	}()
	return true
}

// isStopped reports whether g has been stopped (a nil generation never is).
func (g *wsGeneration) isStopped() bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.stopped
}

// enter/exit bracket work that already has its own goroutine (an HTTP handler).
func (g *wsGeneration) enter() bool {
	if g == nil {
		return true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopped {
		return false
	}
	g.wg.Add(1)
	return true
}

func (g *wsGeneration) exit() {
	if g != nil {
		g.wg.Done()
	}
}

// own registers c to be closed when g stops. If g is already stopped c is closed
// here instead and false is returned, so the caller just abandons it.
func (g *wsGeneration) own(c io.Closer) bool {
	if g == nil {
		return true
	}
	g.mu.Lock()
	stopped := g.stopped
	if !stopped {
		g.owned[c] = struct{}{}
	}
	g.mu.Unlock()
	if stopped {
		c.Close()
		return false
	}
	return true
}

// ownedSessions copies out the smux sessions g owns (queued, admitted or
// draining), for a diagnostics snapshot: a short lock-held copy, nothing retained.
func (g *wsGeneration) ownedSessions() []*smux.Session {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]*smux.Session, 0, len(g.owned))
	for c := range g.owned {
		if sess, ok := c.(*smux.Session); ok {
			out = append(out, sess)
		}
	}
	return out
}

// release forgets c once its owner has closed it itself.
func (g *wsGeneration) release(c io.Closer) {
	if g == nil {
		return
	}
	g.mu.Lock()
	delete(g.owned, c)
	g.mu.Unlock()
}

// stop ends the generation's ownership window and wakes every worker: the
// context is cancelled and every owned socket is closed, which is what unblocks
// reads, accepts and writes that ctx alone cannot. Idempotent.
func (g *wsGeneration) stop() {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.stopped = true
	owned := g.owned
	g.owned = nil
	g.mu.Unlock()
	g.cancel()
	for c := range owned {
		c.Close()
	}
}

// join waits for every worker of a stopped generation, for at most timeout.
func (g *wsGeneration) join(timeout time.Duration) bool {
	if g == nil {
		return true
	}
	done := make(chan struct{})
	go func() {
		g.wg.Wait()
		close(done)
	}()
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-done:
		return true
	case <-t.C:
		return false
	}
}

// tunnelSession is a pool session waiting for admission, with what its upgrade
// negotiated. halfClose is set only when mux_half_close is on and the client
// offered the halfclose-v1 capability (a client that did not is rejected before
// the upgrade), so FlowPlainHC is never sent to a peer that cannot read it.
type tunnelSession struct {
	session   *smux.Session
	halfClose bool
}

type WsMuxTransport struct {
	config     *WsMuxConfig
	smuxConfig *smux.Config
	parentctx  context.Context
	// ctx and cancel belong to gen and are republished with it by Restart.
	ctx    context.Context
	cancel context.CancelFunc
	// gen is the current generation (nil = untracked, see wsGeneration).
	gen            *wsGeneration
	logger         *logrus.Logger
	tunnelChannel  chan tunnelSession
	localChannel   chan LocalTCPConn
	reqNewConnChan chan struct{}
	// setupSlots is the setup-worker permit pool: dispatchLoop takes a permit
	// before it dequeues a connection, so the number of workers in setup is
	// bounded by it and localChannel stays the only queue (see setupAttempt).
	setupSlots chan struct{}
	// setupsActive counts the setups a worker currently owns (permit held). The
	// dispatch loop also parks holding one free permit while it waits for a
	// connection, so len(setupSlots) is not this number.
	setupsActive int32
	// growth* coalesce admission-driven requests for more pool sessions (see
	// requestGrowth). Guarded by growthMu, which is never held across I/O.
	growthMu       sync.Mutex
	growthAsked    bool
	growthMark     int32
	growthAt       time.Time
	growthGap      time.Duration // 0 = growthReaskGap
	controlChannel *network.WebSocketConn
	usageMonitor   *web.Usage
	restartMutex   sync.Mutex
	streamCounter  int32
	sessionCounter int32
	// admittedSessions counts every pool session ever admitted and is never
	// decremented. requestGrowth uses it to tell that its last ask was answered.
	// It is not evidence of a rotation successor: that must be a live registered
	// session (see awaitReplacement).
	admittedSessions int32
	// rotatePoll is how often a rotation waiting for its replacement re-checks
	// the registry; 0 = rotatePollEvery. A field so tests need not wait seconds.
	rotatePoll   time.Duration
	stripedFlows int32 // in-flight striped flows, bounded by the pool's stream budget
	plainFlows   int32

	// sessions is a live registry of pool sessions the striped dispatcher (and
	// the single-leg UDP path) picks legs from. Each entry carries the CDN it
	// arrived over and a live RTT estimate so leg selection can steer toward the
	// lowest-latency, least-loaded connection and spread a flow across distinct
	// CDNs. The non-striped TCP path never touches this.
	sessionsMu    sync.Mutex
	sessions      []*pooledSession
	stripeGroupID uint32
	// plainSelectMu guards only single-leg (plain) flow *selection* and the
	// pendingOpens reservations (plan 014): a pick reserves its session under this
	// lock, so a burst of concurrent flows scores against each other's reserved
	// load and spreads across CDNs instead of piling onto one connection (whose
	// single-connection upload ceiling would cap the aggregate). The lock is never
	// held across OpenStream - which can block up to smux's own open timeout - so
	// one stalled open cannot stall every other flow's placement.
	plainSelectMu sync.Mutex

	fallbackProxy http.Handler

	// controlMu guards controlChannel, handlersStarted, graceTimer, graceEpoch
	// and restartClaim. The HTTP handler goroutine may be adopting a reattached
	// control channel at the same moment a dying channelHandler is clearing the
	// old one, or the grace timer is deciding to restart: adoptControl and
	// onControlGraceExpired arbitrate that under this lock. It is never held
	// across I/O, a join or Restart.
	controlMu       sync.Mutex
	handlersStarted bool
	graceTimer      *time.Timer
	// graceEpoch identifies the current control-loss period. It advances on every
	// loss, adoption, restart claim and Restart, so a timer callback (which
	// captured the epoch it was armed for) that outlives its loss is inert.
	graceEpoch uint64
	// restartClaim is the generation the grace callback committed to restarting;
	// adoptControl refuses control channels for it so none is accepted into a
	// generation that is about to be torn down.
	restartClaim *wsGeneration
	// graceRevalidateHook, if set, runs in onControlGraceExpired between the
	// live-session count and the revalidation. Test seam only.
	graceRevalidateHook func()
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
	WSFraming            bool          // mux_ws_framing: standards-framed legs, strict (a peer that does not offer backhaul-mux-v1 is rejected); false = legacy raw
	HalfClose            bool          // mux_half_close: strict (an upgrade that does not offer the halfclose-v1 capability is rejected); non-promotable plain flows then use FlowPlainHC. Requires MuxVersion >= 2.
}

func NewWSMuxServer(parentCtx context.Context, config *WsMuxConfig, logger *logrus.Logger) *WsMuxTransport {
	// Create the first generation; its context derives from the parent context
	gen := newWsGeneration(parentCtx)
	ctx, cancel := gen.ctx, gen.cancel

	if config.WSFraming {
		logger.Infof("wsmux framing: standards-framed (%s)", network.MuxSubprotocol)
	} else {
		logger.Info("wsmux framing: legacy raw (mux_ws_framing=false)")
	}

	// Build the decoy fallback proxy once, if configured. A bad address is
	// fatal here rather than silently disabling camouflage at runtime.
	fallbackProxy, err := network.NewFallbackProxy(config.Fallback)
	if err != nil {
		logger.Fatalf("invalid fallback address %q: %v", config.Fallback, err)
	}

	// Initialize the TcpTransport struct
	server := &WsMuxTransport{
		gen: gen,
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
		tunnelChannel:  make(chan tunnelSession, config.ChannelSize),
		localChannel:   make(chan LocalTCPConn, config.ChannelSize),
		reqNewConnChan: make(chan struct{}, config.ChannelSize),
		setupSlots:     newSetupSlots(config.ChannelSize),
		streamCounter:  0,
		sessionCounter: 0,
		controlChannel: nil, // will be set when a control connection is established
		usageMonitor:   web.NewDataStore(fmt.Sprintf(":%v", config.WebPort), ctx, config.SnifferLog, config.Sniffer, &config.TunnelStatus, logger),
		fallbackProxy:  fallbackProxy,
	}

	return server
}

func (s *WsMuxTransport) Start() {
	g := s.gen
	// for  webui
	if s.config.WebPort > 0 {
		// A worker, so that Restart waits for the web port to be released before
		// the next generation's monitor tries to bind it.
		g.start(s.usageMonitor.Monitor)
	}

	s.setTunnelStatus(fmt.Sprintf("Disconnected (%s)", s.config.Mode))

	g.start(func() { s.tunnelListener(g) })

}

// setTunnelStatus publishes the status string under controlMu, the lock
// onControlLost already holds when it writes it.
func (s *WsMuxTransport) setTunnelStatus(v string) {
	s.controlMu.Lock()
	s.config.TunnelStatus = v
	s.controlMu.Unlock()
}

// requestRestart asks for a Restart from outside the generation. Restart joins
// every worker of the generation it stops, so a worker calling it inline would
// be waiting for itself (the control handler used to); every worker requests
// through here instead.
func (s *WsMuxTransport) requestRestart() {
	go s.Restart()
}

// Restart tears the current generation down completely and starts a fresh one.
// Requests are serialized (a second one while one runs is dropped, as before).
//
//  1. stop: cancel the context and close every socket and session the
//     generation owns - queued, admitted, draining - which wakes reads, accepts
//     and writes that ctx cannot;
//  2. join: wait for every worker to return - no sleep stands in for this;
//  3. publish: only now replace ctx, counters, channels and registries, so
//     nothing from the old generation can touch the new one.
//
// If the workers do not end within restartJoinTimeout nothing is published (a
// stuck worker could still mutate the new state) and the attempt is retried.
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

	old := s.gen
	if old == nil && s.cancel != nil {
		s.cancel() // untracked transport: nothing to join
	}
	old.stop()

	// A pending grace timer must not restart a generation that is already being
	// replaced. Timer.Stop does not join a callback that is already running, so
	// the epoch is advanced too: that callback finds its epoch gone (and its
	// generation stopped) and does nothing.
	s.controlMu.Lock()
	s.invalidateGraceLocked()
	s.controlMu.Unlock()

	joined := old.join(restartJoinTimeout)

	// set the log level again
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
	//
	// Close the control channel and reset the reattach state, so the next
	// control channel to arrive counts as a first one and starts the pool
	// machinery again.
	s.controlMu.Lock()
	s.invalidateGraceLocked()
	s.restartClaim = nil // the claimed generation is gone
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

	// Re-initialize variables
	s.gen, s.ctx, s.cancel = g, g.ctx, g.cancel
	s.tunnelChannel = make(chan tunnelSession, s.config.ChannelSize)
	s.localChannel = make(chan LocalTCPConn, s.config.ChannelSize)
	s.reqNewConnChan = make(chan struct{}, s.config.ChannelSize)
	s.setupSlots = newSetupSlots(s.config.ChannelSize)
	s.resetGrowth()
	s.usageMonitor = web.NewDataStore(fmt.Sprintf(":%v", s.config.WebPort), g.ctx, s.config.SnifferLog, s.config.Sniffer, &s.config.TunnelStatus, s.logger)
	s.setTunnelStatus("")
	atomic.StoreInt32(&s.streamCounter, 0)
	atomic.StoreInt32(&s.sessionCounter, 0)
	atomic.StoreInt32(&s.admittedSessions, 0)
	// Reset the in-flight flow counts too. Left stale, acquireStripedSlot
	// would see active*StripeFactor already over the budget of a fresh, empty
	// pool and busy-wait (or hang) until leftover goroutines from the previous
	// generation happened to decrement it.
	atomic.StoreInt32(&s.stripedFlows, 0)
	atomic.StoreInt32(&s.plainFlows, 0)

	s.sessionsMu.Lock()
	s.sessions = nil
	s.sessionsMu.Unlock()

	s.Start()
}

// drainQueues closes the sessions and user connections a stopped generation left
// queued in its channels. Only called once every worker of that generation has
// returned, so nothing sends or receives concurrently.
func (s *WsMuxTransport) drainQueues() {
	for {
		select {
		case ts := <-s.tunnelChannel:
			ts.session.Close()
			continue
		case lc := <-s.localChannel:
			lc.conn.Close()
			continue
		default:
		}
		return
	}
}

// channelHandler drives one control channel. It takes the connection as an
// argument rather than reading s.controlChannel on every use: once a dropped
// channel can be replaced without restarting the transport, a handler for a
// dead connection must never touch the shared pointer that now holds its
// successor.
func (s *WsMuxTransport) channelHandler(g *wsGeneration, conn *network.WebSocketConn) {
	ctx, reqNewConn := g.ctx, s.reqNewConnChan
	// A control connection this handler is done with is closed (by
	// onControlLost, or by the generation stopping); forget it so a flapping
	// control channel does not accumulate dead connections in the owned set.
	defer g.release(conn)

	// hctx also ends when this handler returns, so the reader below can never be
	// left parked on a send nobody will receive.
	hctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// A jittered timer (instead of a fixed-period ticker) so the heartbeat
	// cadence isn't perfectly periodic, which is an easy fingerprint for
	// traffic-pattern based DPI.
	heartbeatTimer := time.NewTimer(utils.JitterDuration(s.config.Heartbeat))
	defer heartbeatTimer.Stop()

	// Channel to receive the message or error
	messageChan := make(chan byte, 10)

	// Separate goroutine to continuously listen for messages. A worker of the
	// generation, so Restart waits for it; the generation closing the socket is
	// what wakes its read.
	g.start(func() {
		for {
			select {
			case <-ctx.Done():
				return

			default:
				_, msg, err := conn.ReadMessage()
				// Exit if there's an error
				if err != nil {
					if ctx.Err() != nil {
						// The transport is being stopped and closed this socket
						// itself; there is nothing to hold or reattach.
						return
					}
					s.logger.Warn("control channel read failed. ", err)
					// The control channel carries no user data - only
					// heartbeats and new-connection requests - so losing it
					// must not take the pool, and every flow running on it,
					// down as well. Hold everything and wait for a reattach.
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
		}
	})

	for {
		select {
		case <-ctx.Done():
			// Best effort and bounded: the peer may be gone, and an unbounded
			// write would keep this handler alive past cancellation.
			_ = conn.SetWriteDeadline(time.Now().Add(controlCloseWriteTimeout))
			_ = utils.WriteControlSignal(conn, utils.SG_Closed)
			return
		case <-reqNewConn:
			err := utils.WriteControlSignal(conn, utils.SG_Chan)
			if err != nil {
				s.logger.Warn("failed to send request new connection signal. ", err)
				go s.onControlLost(g, conn)
				return
			}

		case <-heartbeatTimer.C:
			err := utils.WriteControlSignal(conn, utils.SG_HB)
			if err != nil {
				s.logger.Warnf("failed to send heartbeat signal. Error: %v.", err)
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
				s.recordEvent("restart", "control channel closed by client; full restart (all flows dropped)")
				// Not inline: the restart joins this very handler.
				s.requestRestart()
				return

			default:
				s.logger.Errorf("unexpected response from channel: %v", msg)
				s.recordEvent("restart", fmt.Sprintf("unexpected control signal %v; full restart (all flows dropped)", msg))
				s.requestRestart()
				return
			}

		}
	}
}

// controlCloseWriteTimeout bounds the best-effort SG_Closed write sent while the
// transport is being cancelled.
const controlCloseWriteTimeout = 2 * time.Second

// tunnelShutdownTimeout bounds the graceful part of the HTTP server's shutdown;
// after it the server is closed outright.
const tunnelShutdownTimeout = 5 * time.Second

func (s *WsMuxTransport) tunnelListener(g *wsGeneration) {
	ctx := g.ctx
	addr := s.config.BindAddr
	basePath := network.NormalizeBasePath(s.config.Path)
	channelPath := basePath + "/channel"
	tunnelPathPrefix := basePath + "/tunnel"
	speedtestPath := basePath + "/speedtest"
	diagPath := basePath + "/diag"
	poolPath := basePath + "/pool"
	// This listener is a worker of g, started after Restart republished the
	// channels, so it reads them once here (see poolViews).
	views := s.poolViewsOf(g)

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
					// Additive: the same best-effort ownership snapshot as /pool.
					"diagnostics": s.poolDiagnostics(views),
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
				writeJSON(w, http.StatusOK, s.poolSnapshot(views))
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

			// Strict framing: a peer that does not offer the subprotocol would
			// speak raw smux after the upgrade, which this side no longer
			// reads. Refuse it up front, after the authorization checks above
			// so unauthorized and decoy behaviour is unchanged, and say how to
			// fix it.
			if s.config.WSFraming && !network.OffersMuxSubprotocol(r.Header) {
				s.logger.Warnf("rejecting %s upgrade from %s: it does not offer the %s subprotocol (this server has mux_ws_framing on); upgrade the client, or set mux_ws_framing=false on both ends", r.URL.Path, r.RemoteAddr, network.MuxSubprotocol)
				http.Error(w, "standards-framed mux required: upgrade the client, or set mux_ws_framing=false on both ends", http.StatusBadRequest)
				return
			}

			// Half-close (plan 024) is the same kind of strict, per-upgrade gate,
			// checked after the framing one so each mismatch reports its own fix.
			// It also runs after authorization, so unauthorized and decoy
			// behaviour is unchanged. The header is separate from the subprotocol.
			if s.config.HalfClose && !network.OffersCapability(r.Header, network.CapHalfCloseV1) {
				s.logger.Warnf("rejecting %s upgrade from %s: it does not offer the %s capability (this server has mux_half_close on); upgrade the client, or set mux_half_close=false", r.URL.Path, r.RemoteAddr, network.CapHalfCloseV1)
				http.Error(w, "half-close capability required: upgrade the client, or set mux_half_close=false on the server", http.StatusBadRequest)
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

			upgrader := ws.HTTPUpgrader{}
			if s.config.WSFraming {
				// Echo the token in the 101 response.
				upgrader.Protocol = func(p string) bool { return p == network.MuxSubprotocol }
			}
			netConn, brw, hs, err := upgrader.Upgrade(r, w)
			if err != nil {
				s.logger.Errorf("failed to upgrade connection from %s: %v", r.RemoteAddr, err)
				return
			}
			if s.config.WSFraming && hs.Protocol != network.MuxSubprotocol {
				// Cannot happen after the check above; never continue in a mode
				// the peer was not told about.
				s.logger.Errorf("upgrade from %s did not select %s, closing", r.RemoteAddr, network.MuxSubprotocol)
				netConn.Close()
				return
			}
			conn := network.NewWebSocketConn(netConn, ws.StateServerSide, brw.Reader)

			if r.URL.Path == channelPath {
				// Owned from the moment it exists. If the generation was stopped
				// meanwhile, own has already closed it.
				if !g.own(conn) {
					return
				}
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
				// for. The first control channel starts the pool machinery; one
				// arriving after a drop is a reattach: the handle loops, port
				// listeners and pool sessions are all still running, and
				// starting them again would double every listener.
				stale, first, ok := s.adoptControl(g, conn)
				if !ok {
					// The grace timer already committed this generation to a
					// restart; the client redials into the next one.
					s.logger.Warn("control channel refused: this generation is being restarted")
					conn.Close()
					g.release(conn)
					return
				}
				if stale != nil {
					s.logger.Warn("control channel replaced while the previous one was still registered")
					s.recordEvent("control_replaced", fmt.Sprintf("new control channel from %s adopted, stale one dropped", conn.RemoteAddr()))
				}

				// Closed outside controlMu: closing a TLS connection writes.
				if stale != nil {
					stale.Close()
					g.release(stale)
				}

				if !g.start(func() { s.channelHandler(g, conn) }) {
					return
				}

				if !first {
					s.logger.Info("control channel reattached successfully, pool preserved")
					return
				}

				s.logger.Info("control channel established successfully")

				numCPU := runtime.NumCPU()
				if numCPU > 4 {
					numCPU = 4 // Max allowed handler is 4
				}

				g.start(func() { s.parsePortMappings(g) })

				s.logger.Infof("starting %d handle loops on each CPU thread", numCPU)

				for i := 0; i < numCPU; i++ {
					g.start(func() { s.handleLoop(g) })
				}

				g.start(func() { s.dispatchLoop(g) })

			} else if strings.HasPrefix(r.URL.Path, tunnelPathPrefix) {
				// Track the raw socket before smux wraps it, so a constructor
				// failure or a stop mid-handshake cannot strand it.
				if !g.own(netConn) {
					return
				}
				// Raw (legacy) mode gives smux the byte stream itself; framed mode
				// wraps it in binary WebSocket messages. Both are built on conn, never
				// on the raw netConn from ws.UpgradeHTTP: the upgrade reads the request
				// through a bufio.Reader, so any bytes the client pipelined behind its
				// handshake are already off the socket and sitting in that buffer.
				// NewWebSocketConn folds them back in front of the connection; the bare
				// netConn cannot see them, and handing it to smux would start the mux
				// session mid-frame with the stream's first bytes missing. Closing
				// either view closes netConn.
				var leg io.ReadWriteCloser = conn.NetConn()
				if s.config.WSFraming {
					leg = conn.Stream()
				}
				session, err := smux.Client(leg, s.smuxConfig)
				if err != nil {
					s.logger.Errorf("failed to create MUX session for connection %s: %v", conn.RemoteAddr().String(), err)
					conn.Close()
					g.release(netConn)
					return
				}
				// From here the session owns the socket (closing it closes the
				// conn) and stays owned while queued, admitted or draining - the
				// selection registry (s.sessions) is not the ownership record.
				if !g.own(session) {
					return
				}
				g.release(netConn)
				select {
				case s.tunnelChannel <- tunnelSession{session: session, halfClose: s.config.HalfClose}: // ok
				default:
					s.logger.Warnf("tunnel listener channel is full, discarding TCP connection from %s", conn.LocalAddr().String())
					// Close the smux session, not just the raw conn: a bare
					// conn.Close() leaves the session's goroutines and buffers
					// leaked. session.Close() also closes the underlying conn.
					session.Close()
					conn.Close()
					g.release(session)
				}
			}
		}),
	}

	if s.config.Mode == config.WSMUX {
		g.start(func() {
			s.logger.Infof("%s server starting, listening on %s", s.config.Mode, addr)
			s.controlMu.Lock()
			noControl := s.controlChannel == nil
			s.controlMu.Unlock()
			if noControl {
				s.logger.Infof("waiting for %s control channel connection", s.config.Mode)
			}
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				s.logger.Fatalf("failed to listen on %s: %v", addr, err)
			}
		})
	} else {
		g.start(func() {
			engine := s.config.TLSEngine
			if engine == "" {
				engine = network.TLSEngineGo
			}
			s.logger.Infof("%s server starting, listening on %s (tls engine: %s)", s.config.Mode, addr, engine)
			s.controlMu.Lock()
			noControl := s.controlChannel == nil
			s.controlMu.Unlock()
			if noControl {
				s.logger.Infof("waiting for %s control channel connection", s.config.Mode)
			}
			certs, keys := network.ResolveCertPairs(s.config.TLSCertFile, s.config.TLSKeyFile, s.config.TLSCerts, s.config.TLSKeys)
			sndBuf, sndForce := s.tunnelLegSendBuf()
			ln, err := network.NewTLSListener(s.config.TLSEngine, addr, certs, keys, s.config.SO_RCVBUF, sndBuf, sndForce)
			if err != nil {
				s.logger.Fatalf("failed to create tls listener on %s: %v", addr, err)
			}
			if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
				s.logger.Fatalf("failed to listen on %s: %v", addr, err)
			}
		})
	}

	<-ctx.Done()

	// close connection (the generation owns it too; this is just first)
	s.controlMu.Lock()
	ctl := s.controlChannel
	s.controlMu.Unlock()
	if ctl != nil {
		ctl.Close()
	}

	// Gracefully shutdown the server, but not forever: Shutdown does not wait for
	// hijacked tunnel sockets (the generation closed those), only for plain
	// requests such as a proxied fallback, and a stuck one must not hold a
	// restart. The listener is closed before Shutdown even starts waiting.
	s.logger.Infof("shutting down the websocket server on %s", addr)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), tunnelShutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		s.logger.Errorf("Failed to gracefully shutdown the server: %v", err)
		server.Close()
	}
}

// tunnelLegSendBuf is the SO_SNDBUF forced on the server's accepted tunnel
// legs (the wssmux TLS listener). The server -> client direction - the user's
// *upload* - is sent out of these sockets. Left to kernel autotuning their send
// window is bounded by net.ipv4.tcp_wmem[2] (4 MB by default), which on a
// high-RTT tunnel caps upload at ~4 MB / RTT (~357 Mbps at 94 ms) while the
// download direction runs unthrottled under the far larger tcp_rmem ceiling -
// the "fast download, throttled upload" asymmetry.
//
// ApplyTCPTuning raises tcp_wmem[2] to lift that ceiling, but it does so with
// `sysctl -w`, which silently fails in the unprivileged/containerized server
// deployments this tunnel commonly runs in - leaving upload pinned at ~357 Mbps
// even though the download side is fine. Forcing an explicit SO_SNDBUF here
// (via SO_SNDBUFFORCE when privileged; see setSendBuf) makes upload throughput
// deterministic and independent of whether that sysctl took effect.
//
// It is sized to the smux session receive window (MaxReceiveBuffer): that is
// the in-flight budget the *download* direction already sustains at line rate,
// so matching it on the send side restores symmetry rather than guessing a BDP.
// Only the handful of pool connections land on this listener, so a fixed buffer
// costs no memory on the many short-lived user connections - those arrive on the
// separate local port listeners, which are left on autotuning.
//
// The second return value marks this as a *derived default*, applied force-only:
// setting SO_SNDBUF pins the socket out of autotuning, and without CAP_NET_ADMIN
// it is clamped to net.core.wmem_max, so if the FORCE path is unavailable the
// listener leaves autotuning on rather than pinning a possibly-tiny buffer that
// would undershoot the tcp_wmem[2] window autotuning already reaches (see
// setSendBufForce). An explicit so_sndbuf in the config wins and keeps the
// ordinary clamped-fallback behavior, since the operator asked for a fixed size.
func (s *WsMuxTransport) tunnelLegSendBuf() (size int, force bool) {
	if s.config.SO_SNDBUF > 0 {
		return s.config.SO_SNDBUF, false
	}
	return s.config.MaxReceiveBuffer, true
}

func (s *WsMuxTransport) parsePortMappings(g *wsGeneration) {
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
					s.startPortListeners(g, localAddr, strconv.Itoa(port)) // Use port as the remoteAddr
					if !sleepCtx(g.ctx, time.Millisecond) {                // for wide port ranges
						return
					}
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
					s.startPortListeners(g, localAddr, remoteAddr)
					if !sleepCtx(g.ctx, time.Millisecond) { // for wide port ranges
						return
					}
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
		s.startPortListeners(g, localAddr, remoteAddr)
	}
}

// startPortListeners brings up the TCP listener for a mapped port and, when
// accept_udp is enabled on a mux_version >= 2 tunnel, a UDP listener on the same
// port so both transports are forwarded (needed for e.g. L2TP/IPsec, which uses
// UDP 500/1701/4500).
func (s *WsMuxTransport) startPortListeners(g *wsGeneration, localAddr, remoteAddr string) {
	g.start(func() { s.localListener(g, localAddr, remoteAddr) })
	if s.config.AcceptUDP && s.config.MuxVersion >= 2 {
		g.start(func() { s.udpListener(g, localAddr, remoteAddr) })
	}
}

func (s *WsMuxTransport) handleLoop(g *wsGeneration) {
	ctx, tunnelCh := g.ctx, s.tunnelChannel
	for {
		select {
		case <-ctx.Done():
			return

		case ts := <-tunnelCh:
			session := ts.session
			// +1 for session counter
			atomic.AddInt32(&s.sessionCounter, 1)
			atomic.AddInt32(&s.admittedSessions, 1)

			ps := s.registerSession(session, ts.halfClose)
			// The close watcher settles the session's counter and registry entry
			// exactly once, however the session ends (peer, rotation, or the
			// generation closing it). It is a worker of the generation, so Restart
			// waits for it.
			if !g.start(func() {
				<-session.CloseChan()
				s.unregisterSession(session)
				atomic.AddInt32(&s.sessionCounter, -1)
				g.release(session)
			}) {
				// Stopped between dequeue and admission: undo, and close the
				// session (the generation would have, had it seen it).
				s.unregisterSession(session)
				atomic.AddInt32(&s.sessionCounter, -1)
				session.Close()
				return
			}
			// Keep this session's RTT estimate fresh so leg selection can steer
			// toward the lowest-latency CDN. Requires mux_version >= 2 (the peer
			// must be able to route the probe to its echoer, which it does on the
			// FlowPing kind regardless of mode). Every leg-selecting path benefits:
			// striping, the UDP flow path, and the plain single-leg path, which now
			// ranks by legScore too so it spreads across the fast CDNs rather than
			// blindly round-robining onto the slow tail.
			if s.config.MuxVersion >= 2 {
				g.start(func() { s.probeSessionRTT(g, ps) })
			}
			if s.config.MaxConnAge > 0 {
				g.start(func() { s.rotateStripedSession(g, session) })
			}
		}
	}
}

// dispatchLoop takes queued local connections and starts one setup worker for
// each - striped or plain according to the port. It is the only consumer of
// localChannel, and it takes a setup permit (setupSlots) *before* it dequeues, so
// at most cap(setupSlots) workers are ever in setup; everything beyond that waits
// in localChannel, whose size is the only queue. No goroutine is spawned just to
// wait for a permit.
//
// ponytail: the ceiling is the already-normalized ChannelSize rather than a new
// setting; a dedicated knob only if that proves too loose for a deployment.
func (s *WsMuxTransport) dispatchLoop(g *wsGeneration) {
	ctx, localCh, slots := g.ctx, s.localChannel, s.setupSlots
	// Whatever is still queued when the generation stops is closed here, not left
	// for a timeout: nothing will serve it. (Restart drains again once every
	// worker has ended, which also covers a send that races this.)
	defer s.closeQueuedLocal(localCh)
	for {
		select {
		case <-ctx.Done():
			return
		case slots <- struct{}{}:
		}
		select {
		case <-ctx.Done():
			<-slots
			return
		case incomingConn := <-localCh:
			striped := s.shouldStripe(incomingConn)
			if !g.start(func() { s.runSetup(g, slots, incomingConn, striped) }) {
				// Stopped: nobody will serve this connection.
				<-slots
				incomingConn.conn.Close()
				atomic.AddInt32(&s.streamCounter, -1)
			}
		}
	}
}
