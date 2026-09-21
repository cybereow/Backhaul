package transport

import (
	"context"
	"fmt"
	"io"
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

// restartJoinTimeout bounds how long Restart waits for the previous generation's
// workers to end before it publishes the next one. Every blocking point of a
// worker is woken by cancellation or by closing a socket the generation owns, so
// this is a safety valve, not an expected wait; a var so tests can shorten it.
var restartJoinTimeout = 30 * time.Second

// wsGeneration is everything one run of a transport owns: the context its
// workers run under, every worker goroutine, and every socket or session they
// use. Restart stops a generation, waits for its workers, and only then
// publishes the next one, so a worker of generation N can never observe or
// mutate the state of generation N+1 - the transport fields it reads (ctx,
// counters, channels, maps) are republished only once it has returned.
//
// Ownership is registered before use: a worker is counted (start) before its
// goroutine runs and a socket is owned right after it is dialed, before smux or
// admission wraps it. Once stopped, a generation refuses both, so nothing can
// slip in behind the teardown.
//
// A nil *wsGeneration is valid and tracks nothing (goroutines just run, sockets
// are not registered); tests that drive one worker directly rely on that.
type wsGeneration struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu      sync.Mutex
	stopped bool
	owned   map[io.Closer]struct{}
}

func newWsGeneration(parent context.Context) *wsGeneration {
	ctx, cancel := context.WithCancel(parent)
	g := &wsGeneration{ctx: ctx, cancel: cancel, owned: make(map[io.Closer]struct{})}
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

// enter/exit bracket work that already has its own goroutine.
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
	controlChannel *network.WebSocketConn
	// controlMu guards controlChannel: it is now swapped in place on a
	// reconnect, so the dialer, a dying channelHandler and Restart can all
	// touch it at once.
	controlMu       sync.Mutex
	usageMonitor    *web.Usage
	restartMutex    sync.Mutex
	poolConnections int32
	// pendingDials counts pool dials that are in flight (dialing, not yet
	// established). The proactive refill in poolMaintainer subtracts it from the
	// deficit so a slow CDN handshake isn't dialed over and over each tick while
	// the first attempt is still connecting.
	pendingDials    int32
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

	// Identity of the group, fixed by the first leg. Every later leg must match
	// it, so legs from unrelated flows that happen to share a numeric groupID
	// (or a buggy/hostile peer) cannot be filed into the wrong group.
	flowKind        byte   // utils.FlowStriped or utils.FlowPromote
	promotionFlowID uint64 // only meaningful for utils.FlowPromote
}

// addStripeLeg validates one incoming leg against its group (creating the group
// on the first leg) and files it. On rejection the stream is closed and (nil,
// false) is returned; a valid pending group is left untouched. When the leg
// completes the group it is removed from the map and returned with true.
//
// All header bounds are checked before anything is allocated or indexed, and
// the stream is only ever closed after stripeGroupsMu is released.
func (c *WsMuxTransport) addStripeLeg(stream *smux.Stream, kind byte, promoFlowID uint64, groupID uint32, index, total, parity uint8, remoteAddr string) (*stripeGroup, bool) {
	if total == 0 || int(index) >= int(total) || int(parity) >= int(total) {
		c.logger.Errorf("invalid stripe header: group=%d index=%d total=%d parity=%d", groupID, index, total, parity)
		stream.Close()
		return nil, false
	}

	c.stripeGroupsMu.Lock()
	g, ok := c.stripeGroups[groupID]
	if !ok {
		g = &stripeGroup{
			streams:         make([]*smux.Stream, total),
			remaining:       int(total),
			remoteAddr:      remoteAddr,
			parity:          parity,
			flowKind:        kind,
			promotionFlowID: promoFlowID,
		}
		// The timer is bound to this group instance, not just its numeric ID, so
		// it can never abort a later group that reuses the ID.
		g.timer = time.AfterFunc(10*time.Second, func() {
			c.abortStripeGroup(groupID, g)
		})
		c.stripeGroups[groupID] = g
	}

	var reason string
	switch {
	case g.flowKind != kind:
		reason = "flow kind mismatch"
	case kind == utils.FlowPromote && g.promotionFlowID != promoFlowID:
		reason = "promotion flow mismatch"
	case len(g.streams) != int(total) || g.parity != parity:
		reason = "group shape mismatch"
	case g.remoteAddr != remoteAddr:
		reason = "destination mismatch"
	case g.streams[index] != nil:
		reason = "duplicate leg"
	}
	if reason != "" {
		c.stripeGroupsMu.Unlock()
		c.logger.Warnf("rejecting stripe leg %d for group %d: %s", index, groupID, reason)
		stream.Close()
		return nil, false
	}

	g.streams[index] = stream
	g.remaining--
	complete := g.remaining == 0
	if complete {
		delete(c.stripeGroups, groupID)
	}
	c.stripeGroupsMu.Unlock()
	return g, complete
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
	WSFraming            bool // mux_ws_framing: standards-framed legs, fail loudly if the server does not confirm backhaul-mux-v1; false = legacy raw
}

func NewWSMuxClient(parentCtx context.Context, config *WsMuxConfig, logger *logrus.Logger) *WsMuxTransport {
	if config.WSFraming {
		logger.Infof("wsmux framing: standards-framed (%s)", network.MuxSubprotocol)
	} else {
		logger.Info("wsmux framing: legacy raw (mux_ws_framing=false)")
	}
	// Create the first generation; its context derives from the parent context
	gen := newWsGeneration(parentCtx)
	ctx, cancel := gen.ctx, gen.cancel

	// Initialize the TcpTransport struct
	client := &WsMuxTransport{
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

// dialOptions is what every upgrade this client makes - the control channel and
// each tunnel leg - adds to the plain handshake: in framed mode the subprotocol
// offer, so a mismatch with the server shows up at the very first dial and never
// as a corrupted stream later. Legacy mode sends no subprotocol. Every upgrade
// also offers the halfclose-v1 capability (plan 024): a server that does not
// know it ignores the header, one with mux_half_close=true requires it.
func (c *WsMuxTransport) dialOptions() []network.DialOption {
	opts := []network.DialOption{network.WithHalfCloseOffer()}
	if c.config.WSFraming {
		opts = append(opts, network.WithMuxFraming())
	}
	return opts
}

func (c *WsMuxTransport) Start() {
	g := c.gen
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
func (c *WsMuxTransport) requestRestart() {
	go c.Restart()
}

// Restart tears the current generation down completely and starts a fresh one.
// Requests are serialized (a second one while one runs is dropped, as before).
//
//  1. stop: cancel the context and close every socket and session the
//     generation owns, which wakes reads and accepts that ctx cannot;
//  2. join: wait for every worker to return - no sleep stands in for this;
//  3. publish: only now replace ctx, counters, channels and maps, so nothing
//     from the old generation can touch the new one.
//
// If the workers do not end within restartJoinTimeout nothing is published (a
// stuck worker could still mutate the new state) and the attempt is retried.
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

	// No worker of the old generation is left, so the control channel pointer
	// and every other field below are ours alone.
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
	atomic.StoreInt32(&c.pendingDials, 0)
	atomic.StoreInt32(&c.loadConnections, 0)
	c.controlFlow = make(chan struct{}, 100)

	c.closeStripeGroups()
	c.promotableFlowsMu.Lock()
	c.promotableFlows = make(map[uint64]*handlers.PumpSwapper)
	c.promotableFlowsMu.Unlock()

	c.Start()
}

// closeStripeGroups drops every pending (incomplete) stripe group and closes the
// legs that had arrived.
func (c *WsMuxTransport) closeStripeGroups() {
	c.stripeGroupsMu.Lock()
	groups := c.stripeGroups
	c.stripeGroups = make(map[uint32]*stripeGroup)
	c.stripeGroupsMu.Unlock()

	for _, g := range groups {
		g.timer.Stop()
		for _, st := range g.streams {
			if st != nil {
				st.Close()
			}
		}
	}
}

func (c *WsMuxTransport) channelDialer() {
	// The generation this worker belongs to, captured once: Restart republishes
	// c.ctx/c.gen only after this worker has returned.
	g, ctx := c.gen, c.ctx

	c.logger.Infof("attempting to establish a new %s control channel connection", c.config.Mode)

	for {
		select {
		case <-ctx.Done():
			return
		default:

			ep := c.nextEndpoint()
			tunnelWSConn, err := network.WebSocketDialer(ctx, ep.addr, ep.edgeIP, network.NormalizeBasePath(c.config.Path)+"/channel", c.config.DialTimeOut, c.config.KeepAlive, true, c.config.Token, c.userAgent, c.config.Mode, 3, 0, 0, 0, c.config.TLSVerify, c.dialOptions()...)
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

// poolRefillPerTick caps how many pool connections the proactive refill dials
// in a single tick. The refill runs once a second, so this bounds the reconnect
// rate to at most this many dials/sec - enough to rebuild a drained pool in a
// few seconds, gentle enough not to storm a flaky CDN that is already returning
// 502/521 (which is often *why* the pool drained in the first place).
const poolRefillPerTick = 4

// poolRefillCount decides how many new pool dials to launch this tick to keep
// the pool at its configured floor. target is connection_pool; live is the
// established pool connections; pending is dials still in flight. Split out as a
// pure function so the deficit logic is unit-testable without driving real
// dials. It never returns more than perTick, and never counts a connection
// twice (in-flight dials are subtracted from the deficit), so a slow CDN
// handshake is waited on rather than piled onto.
func poolRefillCount(target, live, pending, perTick int) int {
	deficit := target - live - pending
	if deficit <= 0 {
		return 0
	}
	if deficit > perTick {
		return perTick
	}
	return deficit
}

func (c *WsMuxTransport) poolMaintainer() {
	g, ctx := c.gen, c.ctx

	// Stagger the initial pool fill instead of firing every dial at once -
	// a burst of ConnPoolSize near-simultaneous TLS handshakes to the same
	// host is a distinctive connection pattern real browser traffic doesn't
	// produce.
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

			// Proactively keep the pool topped up to its configured floor.
			// Pool connections die on their own - a CDN max-age reset, an idle
			// phone whose tunnelled flows went quiet long enough for the edge to
			// drop them - and nothing else rebuilt them: the initial fill runs
			// once and the dynamic sizing below only *grows* on load. So after an
			// idle spell the pool could sit well below connection_pool, and a
			// resumed flow would land on a thin (or empty) pool and stall until
			// load happened to trigger growth - the "works, then after another
			// idle it doesn't, until I reconnect" symptom. Refill the deficit
			// gently, counting in-flight dials so a slow handshake isn't dialed
			// repeatedly.
			//
			// Target the configured floor (ConnPoolSize), NOT newPoolSize: during
			// a dial outage poolConnectionsAvg sits at 0, so the load check below
			// increments newPoolSize every 10s even with no traffic. Chasing that
			// inflated target here would, on recovery, open hundreds of surplus
			// sessions at poolRefillPerTick/sec and overload the CDN that just
			// came back. Growth above the floor stays the load path's job (it
			// dials its own connection when it grows).
			refill := poolRefillCount(c.config.ConnPoolSize, int(atomic.LoadInt32(&c.poolConnections)), int(atomic.LoadInt32(&c.pendingDials)), poolRefillPerTick)
			for i := 0; i < refill; i++ {
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
func (c *WsMuxTransport) channelHandler(conn *network.WebSocketConn) {
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
				select {
				case <-c.controlFlow: // Do nothing

				default:
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

// controlReconnectWindow bounds how long a dropped control channel is re-dialled
// while the pool is kept alive. It matches the server's grace window: past that
// the server gives up on the reattach and rebuilds its own side anyway, so there
// is nothing left to preserve and a full restart is the honest fallback.
const controlReconnectWindow = 30 * time.Second

// controlCloseWriteTimeout bounds the best-effort SG_Closed write sent while
// the transport is being cancelled.
const controlCloseWriteTimeout = 2 * time.Second

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
func (c *WsMuxTransport) reconnectControl(old *network.WebSocketConn, oldDone <-chan struct{}) {
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
		conn, err := network.WebSocketDialer(ctx, ep.addr, ep.edgeIP, network.NormalizeBasePath(c.config.Path)+"/channel", c.config.DialTimeOut, c.config.KeepAlive, true, c.config.Token, c.userAgent, c.config.Mode, 1, 0, 0, 0, c.config.TLSVerify, c.dialOptions()...)
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

		c.logger.Errorf("control channel re-dial: %v", err)

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

func (c *WsMuxTransport) tunnelDialer() {
	g, ctx := c.gen, c.ctx

	// Count this attempt as in flight only for the dialing phase, so the
	// proactive refill (poolMaintainer) doesn't re-dial a connection that is
	// still handshaking. It is handed off to poolConnections once established.
	atomic.AddInt32(&c.pendingDials, 1)

	ep := c.nextEndpoint()
	c.logger.Debugf("initiating new %s tunnel connection to address %s", c.config.Mode, ep.addr)

	// Dial to the tunnel server
	tunnelWSConn, err := network.WebSocketDialer(ctx, ep.addr, ep.edgeIP, network.NormalizeBasePath(c.config.Path)+"/tunnel", c.config.DialTimeOut, c.config.KeepAlive, c.config.Nodelay, c.config.Token, c.userAgent, c.config.Mode, 3, c.config.SO_RCVBUF, c.config.SO_SNDBUF, c.config.MSS, c.config.TLSVerify, c.dialOptions()...)
	if err != nil {
		atomic.AddInt32(&c.pendingDials, -1)
		c.logger.Errorf("tunnel server dialer: %v", err)

		return
	}

	// Own the socket before anything else touches it. If the generation was
	// stopped while this dial was in flight, own has already closed it.
	if !g.own(tunnelWSConn) {
		atomic.AddInt32(&c.pendingDials, -1)
		return
	}

	// Increment active connections counter, then drop the in-flight mark: the
	// connection is now a live pool member tracked by poolConnections.
	atomic.AddInt32(&c.poolConnections, 1)
	atomic.AddInt32(&c.pendingDials, -1)

	c.handleSession(tunnelWSConn)
}

func (c *WsMuxTransport) handleSession(tunnelConn *network.WebSocketConn) {
	g, ctx := c.gen, c.ctx

	// The one place a pool connection leaves the pool counter, whichever way
	// this returns: constructor failure, session error, or cancellation.
	defer func() {
		atomic.AddInt32(&c.poolConnections, -1)
		g.release(tunnelConn)
	}()

	// SMUX server. Raw (legacy) mode runs smux on the socket itself; framed mode
	// on binary WebSocket messages over it (the dial already confirmed the server
	// speaks that).
	var leg io.ReadWriteCloser = tunnelConn.NetConn()
	if c.config.WSFraming {
		leg = tunnelConn.Stream()
	}
	session, err := smux.Server(leg, c.smuxConfig)
	if err != nil {
		c.logger.Errorf("failed to create mux session: %v", err)
		// The socket has no other owner that would close it before restart.
		tunnelConn.Close()
		return
	}
	// The session now owns the socket; closing it (Restart does, through the
	// generation) is what wakes AcceptStream, which ctx cannot.
	if !g.own(session) {
		return
	}
	defer func() {
		session.Close()
		g.release(session)
	}()

	// run starts a per-stream worker in this generation. Past the stop it runs
	// nothing and the stream is closed instead.
	run := func(stream net.Conn, f func()) {
		if !g.start(f) {
			stream.Close()
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			stream, err := session.AcceptStream()
			if err != nil {
				c.logger.Debug("session is closed: ", err)
				return
			}
			c.setupStream(stream, tunnelConn.RemoteAddr().String(), run)
		}
	}
}

// setupHeaderTimeout bounds how long a peer may take to send a new stream's
// initial header: the configured dial timeout (cmd normalizes it to at least one
// second; the fallback only covers a hand-built config).
func (c *WsMuxTransport) setupHeaderTimeout() time.Duration {
	if c.config.DialTimeOut > 0 {
		return c.config.DialTimeOut
	}
	return 10 * time.Second
}

// setupStream reads and validates the initial header of one accepted stream and
// hands it to a worker (run). It is deliberately serial - it runs on the accept
// loop, so a stream that never finishes its header cannot pile up blocked parser
// goroutines - and bounded: the read deadline is setupHeaderTimeout, and closing
// the session (generation stop) wakes it earlier. A slow header therefore delays
// the streams behind it for at most that long; it is bounded, not free.
//
// The header deadline is cleared before the stream reaches a worker, so it can
// never linger into the flow's data transfer. An unknown flow kind is a protocol
// error and closes the stream; it is never reinterpreted as a legacy header.
func (c *WsMuxTransport) setupStream(stream *smux.Stream, remote string, run func(net.Conn, func())) {
	_ = stream.SetReadDeadline(time.Now().Add(c.setupHeaderTimeout()))
	ready := func(f func()) {
		_ = stream.SetReadDeadline(time.Time{})
		run(stream, f)
	}
	bad := func(what string, err error) {
		c.logger.Errorf("%s from stream connection %s: %v", what, remote, err)
		stream.Close()
	}
	striped := func() {
		groupID, index, total, parity, remoteAddr, err := utils.ReceiveStripeHeader(stream)
		if err != nil {
			bad("failed to read stripe header", err)
			return
		}
		ready(func() { c.handleStripedStream(stream, groupID, index, total, parity, remoteAddr) })
	}

	if c.config.MuxVersion < 2 {
		if c.config.StripeFactor > 1 {
			striped()
			return
		}
		remoteAddr, err := utils.ReceiveBinaryString(stream)
		if err != nil {
			bad("unable to get port", err)
			return
		}
		ready(func() { c.localDialer(stream, remoteAddr) })
		return
	}

	kind, err := utils.ReadFlowKind(stream)
	if err != nil {
		bad("unable to read flow kind", err)
		return
	}
	switch kind {
	case utils.FlowPlainHC:
		// Plain flow wrapped in the half-close envelope (plan 024), sent only
		// because this client offered halfclose-v1. Never promotable: a non-zero
		// flowID is a protocol violation.
		flowID, remoteAddr, err := utils.ReceiveFlowPlainHC(stream)
		if err == nil && flowID != 0 {
			err = fmt.Errorf("non-zero flow id %d on a half-close flow", flowID)
		}
		if err != nil {
			bad("bad half-close plain flow header", err)
			return
		}
		ready(func() { c.localDialer(handlers.NewHalfCloseConn(stream), remoteAddr) })
	case utils.FlowPlain:
		flowID, remoteAddr, err := utils.ReceiveFlowPlain(stream)
		if err != nil {
			bad("unable to read plain flow header", err)
			return
		}
		// A non-zero flowID marks the flow as promotable: run it through the
		// promotable pump so a later FlowPromote can migrate it mid-stream. flowID
		// 0 is a plain flow.
		if flowID != 0 {
			ready(func() { c.localDialerPlain(stream, flowID, remoteAddr) })
		} else {
			ready(func() { c.localDialer(stream, remoteAddr) })
		}
	case utils.FlowStriped:
		striped()
	case utils.FlowPromote:
		flowID, groupID, index, total, parity, err := utils.ReceiveFlowPromote(stream)
		if err != nil {
			bad("failed to read promote header", err)
			return
		}
		ready(func() { c.handlePromoteStream(stream, flowID, groupID, index, total, parity) })
	case utils.FlowUDP:
		remoteAddr, err := utils.ReceiveFlowUDP(stream)
		if err != nil {
			bad("unable to read udp flow header", err)
			return
		}
		ready(func() { c.localDialerUDP(stream, remoteAddr) })
	case utils.FlowPing:
		ready(func() { c.handlePingStream(stream) }) // reads its 8-byte nonce under its own deadline
	case utils.FlowSpeedtest:
		mode, seconds, err := utils.ReceiveFlowSpeedtest(stream)
		if err != nil {
			bad("speedtest header read failed", err)
			return
		}
		ready(func() { c.handleSpeedtestStream(stream, mode, seconds) })
	default:
		bad("unknown flow kind", fmt.Errorf("0x%02x", kind))
	}
}

// handleStripedStream files a newly accepted stream, whose stripe header
// setupStream already read, under its group. Once every leg the server promised
// (total) has shown up, the group is assembled into a single striping.Conn and
// handed to localDialer exactly like a plain stream would be.
func (c *WsMuxTransport) handleStripedStream(stream *smux.Stream, groupID uint32, index, total, parity uint8, remoteAddr string) {
	g, complete := c.addStripeLeg(stream, utils.FlowStriped, 0, groupID, index, total, parity, remoteAddr)
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
		c.startLocalDialer(fecConn, g.remoteAddr)
		return
	}
	c.startLocalDialer(striping.New(conns, striping.DefaultChunkSize), g.remoteAddr)
}

// startLocalDialer hands an assembled striped connection to a localDialer worker
// of the current generation, or closes it (and so its legs) if the generation is
// already stopped. Called only from a worker, so c.gen is that worker's own.
func (c *WsMuxTransport) startLocalDialer(conn net.Conn, remoteAddr string) {
	if !c.gen.start(func() { c.localDialer(conn, remoteAddr) }) {
		conn.Close()
	}
}

// abortStripeGroup gives up on a group that never received all of its legs
// within the timeout (e.g. one pool connection died mid-handshake) and
// closes whatever legs did arrive. It only acts if the map still holds this
// exact group instance: a group that completed (or was replaced by a newer one
// reusing the same numeric ID) is left alone.
func (c *WsMuxTransport) abortStripeGroup(groupID uint32, g *stripeGroup) {
	c.stripeGroupsMu.Lock()
	cur, ok := c.stripeGroups[groupID]
	ok = ok && cur == g
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
	ctx := c.ctx // this worker's generation, read once
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

	localConnection, err := network.TcpDialer(ctx, resolvedAddr, "", c.config.DialTimeOut, c.config.KeepAlive, true, 1, recvBuf, sendBuf, 0)
	if err != nil {
		c.logger.Errorf("local dialer: %v", err)
		stream.Close()
		return
	}

	c.logger.Debugf("connected to local address %s successfully", remoteAddr)

	handlers.TCPConnectionHandler(ctx, false, stream, localConnection, c.logger, c.usageMonitor, int(port), c.config.Sniffer)
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

// handleSpeedtestStream answers a server-initiated tunnel speed test. The server
// picks the direction: on a download it sources the data and the client sinks it
// and reports back the receiver-measured bytes/elapsed; on an upload the client
// sources for the requested duration and the server measures. It carries no user
// data and is torn down when the test ends.
func (c *WsMuxTransport) handleSpeedtestStream(stream net.Conn, mode byte, seconds uint32) {
	defer stream.Close()
	dur := time.Duration(seconds) * time.Second
	switch mode {
	case utils.SpeedtestDownload:
		bytes, el, err := utils.SpeedtestSink(stream)
		if err != nil {
			c.logger.Tracef("speedtest download sink ended: %v", err)
			return
		}
		if err := utils.WriteSpeedtestReport(stream, bytes, el); err != nil {
			c.logger.Tracef("speedtest report write failed: %v", err)
		}
	case utils.SpeedtestUpload:
		if err := utils.SpeedtestSource(stream, dur); err != nil {
			c.logger.Tracef("speedtest upload source ended: %v", err)
		}
	default:
		c.logger.Tracef("speedtest: unknown mode %d", mode)
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
	ctx := c.ctx // this worker's generation, read once
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

	localConnection, err := network.TcpDialer(ctx, resolvedAddr, "", c.config.DialTimeOut, c.config.KeepAlive, true, 1, recvBuf, sendBuf, 0)
	if err != nil {
		c.logger.Errorf("local dialer: %v", err)
		stream.Close()
		return
	}

	swapper := handlers.PromotablePump(ctx, false, localConnection, stream, c.logger, c.usageMonitor, port, c.config.Sniffer)
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

func (c *WsMuxTransport) handlePromoteStream(stream *smux.Stream, flowID uint64, groupID uint32, index, total, parity uint8) {
	ctx := c.ctx // this worker's generation, read once
	c.promotableFlowsMu.Lock()
	swapper, ok := c.promotableFlows[flowID]
	c.promotableFlowsMu.Unlock()

	if !ok {
		c.logger.Errorf("promotion leg for unknown flow %d", flowID)
		stream.Close()
		return
	}

	// remoteAddr is not needed for promotion, so it is always "" here.
	g, complete := c.addStripeLeg(stream, utils.FlowPromote, flowID, groupID, index, total, parity, "")
	if !complete {
		return
	}

	g.timer.Stop()
	conns := make([]net.Conn, len(g.streams))
	for i, st := range g.streams {
		conns[i] = st
	}

	// Freeze, exchange the byte counts on raw leg 0 (no striped reader exists
	// yet), then build the wrapper and install it. A failure before the freeze
	// leaves the flow plain; a later one aborts it (see PumpSwapper.Promote).
	err := swapper.Promote(ctx, conns, func() (net.Conn, error) {
		if g.parity > 0 {
			dataShards := len(conns) - int(g.parity)
			return striping.NewFEC(conns, striping.DefaultChunkSize, dataShards, int(g.parity))
		}
		return striping.New(conns, striping.DefaultChunkSize), nil
	})
	if err != nil {
		c.logger.Warnf("promotion of flow %d failed: %v", flowID, err)
		return
	}
	c.logger.Debugf("flow %d promoted to %d striped legs", flowID, len(conns))
}
