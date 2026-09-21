package transport

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/musix/backhaul/internal/utils"
	"github.com/musix/backhaul/internal/utils/handlers"
	"github.com/musix/backhaul/internal/utils/network"
	"github.com/musix/backhaul/internal/utils/striping"
	"github.com/xtaci/smux"
)

// sleepCtx waits d, or until ctx ends; it reports whether the full wait elapsed.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (s *WsMuxTransport) localListener(g *wsGeneration, localAddr string, remoteAddr string) {
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

	// Owned from the moment it exists, so the generation closes it (which is what
	// wakes the accept loop) even if this worker is slow to notice the stop.
	if !g.own(listener) {
		return
	}
	//close local listener after context cancellation
	defer func() {
		listener.Close()
		g.release(listener)
	}()

	if !g.start(func() { s.acceptLocalConn(g, listener, remoteAddr) }) {
		return
	}

	s.logger.Infof("listener started successfully, listening on address: %s", listener.Addr().String())

	<-g.ctx.Done()
}

func (s *WsMuxTransport) acceptLocalConn(g *wsGeneration, listener net.Listener, remoteAddr string) {
	ctx, localCh := g.ctx, s.localChannel
	for {
		select {
		case <-ctx.Done():
			return

		default:
			conn := acceptWithBackoff(ctx, listener, s.logger)
			if conn == nil {
				return
			}
			if ctx.Err() != nil {
				// Accepted in the instant the generation stopped: nothing will
				// serve it, and the new generation must not inherit it.
				conn.Close()
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
			case localCh <- LocalTCPConn{conn: conn, remoteAddr: remoteAddr, timeCreated: acceptStamp(time.Now())}:
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

// Setup limits. A local connection has setupTimeout, counted from the moment it
// was accepted, to be handed to a data pump: queueing, admission waits, retries
// and header writes all spend that one budget, and no retry refreshes it.
//
// The pinned smux (v1.5.27) OpenStream cannot be cancelled and can itself block
// for up to its internal 30s open timeout. So the *local socket* is closed on
// time, but the worker (and its setup permit) stays owned until OpenStream
// returns, and a stream that arrives late is closed, never used. Nothing here
// closes a shared pool session to cancel one flow's setup.
const (
	setupTimeout          = 3 * time.Second
	admissionPollInterval = 10 * time.Millisecond
	setupRetryBackoff     = 100 * time.Millisecond
)

// growthReaskGap is how long admission waits before asking for more pool
// sessions again when the previous request produced none (a failed dial).
// WsMuxTransport.growthGap overrides it (tests shorten it).
const growthReaskGap = time.Second

// acceptEpoch anchors LocalTCPConn.timeCreated for this transport. The field is a
// millisecond count that reads like a Unix time, but it is computed from the
// monotonic clock (elapsed since acceptEpoch, added to acceptEpoch's wall time),
// so its age never follows a wall-clock step. A plain UnixMilli stamp did: on a
// host whose clock stepped, queued and retrying connections aged - and were closed -
// up to a second early or late.
var acceptEpoch = time.Now()

// acceptStamp is the timeCreated value for a connection accepted at t (which must
// come from time.Now, so that it carries a monotonic reading).
func acceptStamp(t time.Time) int64 {
	return acceptEpoch.UnixMilli() + t.Sub(acceptEpoch).Milliseconds()
}

// stampAge is how long ago acceptStamp(t) was taken, on the monotonic clock.
func stampAge(stamp int64) time.Duration {
	return time.Since(acceptEpoch) - time.Duration(stamp-acceptEpoch.UnixMilli())*time.Millisecond
}

// newSetupSlots builds the setup-permit pool: a finite ceiling of n permits.
func newSetupSlots(n int) chan struct{} {
	if n < 1 {
		n = 1
	}
	return make(chan struct{}, n)
}

// requestGrowth asks the client for one more pool session, coalescing demand:
// any number of waiting setups share one request, repeated only once a session
// has been admitted since the last one (the ask was answered, and the budget may
// still be short) or growthReaskGap has passed (the dial probably failed).
func (s *WsMuxTransport) requestGrowth() {
	admitted := atomic.LoadInt32(&s.admittedSessions)
	now := time.Now()
	s.growthMu.Lock()
	gap := s.growthGap
	if gap <= 0 {
		gap = growthReaskGap
	}
	if s.growthAsked && s.growthMark == admitted && now.Sub(s.growthAt) < gap {
		s.growthMu.Unlock()
		return
	}
	s.growthAsked, s.growthMark, s.growthAt = true, admitted, now
	s.growthMu.Unlock()
	select {
	case s.reqNewConnChan <- struct{}{}:
	default:
	}
}

// resetGrowth forgets the last request; Restart calls it once no worker is left.
func (s *WsMuxTransport) resetGrowth() {
	s.growthMu.Lock()
	s.growthAsked = false
	s.growthMu.Unlock()
}

// setupAttempt is one local connection's setup, owned by exactly one worker from
// dequeue until it hands the connection to a data pump or drops it. It carries
// the absolute expiry, the setup permit and the guard that closes the local
// socket on expiry or generation stop.
type setupAttempt struct {
	s      *WsMuxTransport
	g      *wsGeneration
	lc     LocalTCPConn
	expiry time.Time

	slots  chan struct{}
	permit sync.Once

	// guard closes lc.conn at expiry or when the generation stops. It is settled
	// (stopped and joined) before the connection is handed to a data pump, so it
	// can never close a flow that succeeded.
	guard   sync.Once
	fired   atomic.Bool
	timer   *time.Timer
	stopCtx func() bool
}

func (s *WsMuxTransport) newSetupAttempt(g *wsGeneration, slots chan struct{}, lc LocalTCPConn) *setupAttempt {
	// The accepted stamp is monotonic-based (acceptStamp), and is converted once,
	// here, into a deadline on the monotonic clock: every later check and timer
	// then measures elapsed time, so a wall-clock step (NTP, a VM resume) cannot
	// shorten or stretch the budget of a connection - queued or mid-setup.
	left := setupTimeout - stampAge(lc.timeCreated)
	a := &setupAttempt{s: s, g: g, lc: lc, slots: slots, expiry: time.Now().Add(left)}
	atomic.AddInt32(&s.setupsActive, 1)
	fire := func() {
		a.guard.Do(func() {
			a.fired.Store(true)
			lc.conn.Close()
		})
	}
	a.timer = time.AfterFunc(time.Until(a.expiry), fire)
	a.stopCtx = context.AfterFunc(g.ctx, fire)
	return a
}

// releasePermit gives the setup permit back, once however often it is called.
func (a *setupAttempt) releasePermit() {
	a.permit.Do(func() {
		atomic.AddInt32(&a.s.setupsActive, -1)
		<-a.slots
	})
}

// settle stops the guard and waits for a callback that already started, so when
// it returns the local socket is either closed (true) or will never be closed by
// the guard (false).
func (a *setupAttempt) settle() bool {
	a.timer.Stop()
	a.stopCtx()
	a.guard.Do(func() {}) // claims the guard if it has not run; otherwise waits for it
	return a.fired.Load()
}

// done reports that setup must stop: expired, or the generation ended.
func (a *setupAttempt) done() bool {
	return a.fired.Load() || a.g.ctx.Err() != nil || !time.Now().Before(a.expiry)
}

// wait sleeps d, cut short by expiry or generation stop; it reports whether
// setup may continue afterwards.
func (a *setupAttempt) wait(d time.Duration) bool {
	if rem := time.Until(a.expiry); rem < d {
		d = rem
	}
	if d > 0 && !sleepCtx(a.g.ctx, d) {
		return false
	}
	return !a.done()
}

// handoff ends the guard right before the connection goes to a data pump. False
// means the guard already closed it: the caller must undo its own state and drop.
func (a *setupAttempt) handoff() bool {
	return !a.settle()
}

// drop ends a setup that did not produce a flow: the local socket is closed and
// its streamCounter slot released. Called exactly once, by the owning worker.
func (a *setupAttempt) drop() {
	if !time.Now().Before(a.expiry) {
		a.s.logger.Debugf("timeouted local connection: setup budget of %s used up", setupTimeout)
	}
	a.settle()
	a.lc.conn.Close()
	atomic.AddInt32(&a.s.streamCounter, -1)
}

// runSetup is the worker dispatchLoop starts for one dequeued connection. The
// permit it was started with is released when setup ends or, on success, when
// the flow starts - not when its data transfer ends.
func (s *WsMuxTransport) runSetup(g *wsGeneration, slots chan struct{}, lc LocalTCPConn, striped bool) {
	a := s.newSetupAttempt(g, slots, lc)
	defer a.releasePermit()
	if striped {
		s.dispatchStriped(a)
	} else {
		s.dispatchPlain(a)
	}
}

// closeQueuedLocal closes the connections still queued on ch and releases their
// streamCounter slots.
func (s *WsMuxTransport) closeQueuedLocal(ch chan LocalTCPConn) {
	for {
		select {
		case lc := <-ch:
			lc.conn.Close()
			atomic.AddInt32(&s.streamCounter, -1)
		default:
			return
		}
	}
}

// acquirePlainSlot reserves one plain-flow admission slot. While the pool's
// budget is full it asks (coalesced) for a bigger pool and polls, until the
// attempt expires or the generation stops.
func (s *WsMuxTransport) acquirePlainSlot(a *setupAttempt) bool {
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
		s.requestGrowth()
		if !a.wait(admissionPollInterval) {
			return false
		}
	}
}

func (s *WsMuxTransport) dispatchPlain(a *setupAttempt) {
	g, incomingConn := a.g, a.lc
	for {
		if a.done() {
			a.drop()
			return
		}
		if !s.acquirePlainSlot(a) {
			a.drop()
			return
		}

		stream, ps, err := s.openPlainLegPS()
		if err == nil && a.done() {
			// Expired (or stopped) while the open was in flight: the local socket
			// is already closed; the stream that finally arrived is not used.
			stream.Close()
			atomic.AddInt32(&s.plainFlows, -1)
			a.drop()
			return
		}
		if err != nil {
			atomic.AddInt32(&s.plainFlows, -1)
			s.logger.Tracef("plain dispatch: %v, retrying shortly", err)
			s.requestGrowth() // no live session to open on
			if !a.wait(setupRetryBackoff) {
				a.drop()
				return
			}
			continue
		}

		var flowID uint64
		// Promotion migrates a heavy plain flow onto a striped group, so it only
		// makes sense when a group is actually wider than one leg. In pure-plain mode
		// (StripeFactor 1, no parity) legsPerFlow is 1, so "promotion" would just wrap
		// the flow in single-leg striping framing and stall it through the promote
		// handshake for no aggregation gain - keep it plain. Per-port striping (a
		// plain port in a striped deployment) still has legsPerFlow > 1 and promotes.
		promotable := s.config.MuxVersion >= 2 && s.config.PromoteBytes > 0 && s.legsPerFlow() > 1

		// Half-close envelope (plan 024): only a plain, non-promotable flow on a
		// session that negotiated halfclose-v1. Promotable flows keep FlowPlain and
		// its legacy full-close semantics.
		halfClose := s.config.MuxVersion >= 2 && !promotable && ps.halfClose
		var streamConn net.Conn = stream

		// The header write shares the setup's expiry, and the deadline is cleared
		// before the stream carries data.
		_ = stream.SetWriteDeadline(a.expiry)
		if s.config.MuxVersion >= 2 {
			// A non-zero flowID signals the client this flow is promotable and must
			// be run through the promotable pump; flowID 0 means a plain flow.
			if promotable {
				flowID = uint64(time.Now().UnixNano())
			}
			send := utils.SendFlowPlain
			if halfClose {
				send = utils.SendFlowPlainHC
				streamConn = handlers.NewHalfCloseConn(stream)
			}
			err = send(stream, flowID, incomingConn.remoteAddr)
		} else {
			err = utils.SendBinaryString(stream, incomingConn.remoteAddr)
		}
		if err != nil {
			atomic.AddInt32(&s.plainFlows, -1)
			stream.Close()
			if !a.wait(setupRetryBackoff) {
				a.drop()
				return
			}
			continue
		}
		_ = stream.SetWriteDeadline(time.Time{})

		if !a.handoff() {
			// The guard already closed the local socket at expiry.
			atomic.AddInt32(&s.plainFlows, -1)
			stream.Close()
			a.drop()
			return
		}
		a.releasePermit() // the flow is active: its data transfer holds no setup permit

		defer atomic.AddInt32(&s.plainFlows, -1)
		defer atomic.AddInt32(&s.streamCounter, -1)
		if promotable {
			// dispatchPromotable blocks until the flow (and any mid-stream
			// promotion) completes, so the counters above are released only when
			// the flow is truly done.
			s.dispatchPromotable(g, incomingConn.conn, stream, flowID, incomingConn.remoteAddr)
		} else {
			handlers.TCPConnectionHandler(g.ctx, s.config.ProxyProtocol, incomingConn.conn, streamConn, s.logger, s.usageMonitor, incomingConn.conn.LocalAddr().(*net.TCPAddr).Port, s.config.Sniffer)
		}
		return
	}
}

// dispatchStriped opens the striped legs for one incoming connection, sends the
// per-leg headers, and pumps the flow. streamCounter (incremented in
// localListener when the connection was accepted) is decremented exactly once
// for every connection that leaves this pipeline - dropped here, or handed to a
// handler that later finishes. A failed attempt is retried by this same worker
// until the attempt's absolute expiry; the accepted time is never refreshed.
func (s *WsMuxTransport) dispatchStriped(a *setupAttempt) {
	g, incomingConn := a.g, a.lc
	for {
		if a.done() {
			a.drop()
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
		if !s.acquireStripedSlot(a) {
			a.drop()
			return
		}
		// The admission slot is released explicitly on each exit path rather than
		// via a single defer: a setup that fails must give the slot back *before*
		// the backoff sleep, otherwise it shrinks capacity for everyone else while
		// doing nothing.

		legs, err := s.openStripedLegs(s.legsPerFlow())
		if err == nil && a.done() {
			// Expired (or stopped) while the opens were in flight: the local socket
			// is already closed; the legs that finally arrived are not used.
			closeLegs(legs)
			atomic.AddInt32(&s.stripedFlows, -1)
			a.drop()
			return
		}
		if err != nil {
			atomic.AddInt32(&s.stripedFlows, -1) // release before backoff
			s.logger.Tracef("striped dispatch: %v, retrying shortly", err)
			// Too few live sessions for a whole group is a width deficit, not a
			// full budget: ask for capacity here too (coalesced, not per leg).
			s.requestGrowth()
			if !a.wait(setupRetryBackoff) {
				a.drop()
				return
			}
			continue
		}

		gid := atomic.AddUint32(&s.stripeGroupID, 1)
		var sendErr error
		for i, stream := range legs {
			_ = stream.SetWriteDeadline(a.expiry)
			if s.config.MuxVersion >= 2 {
				sendErr = utils.SendFlowStriped(stream, gid, uint8(i), uint8(len(legs)), uint8(s.config.StripeParity), incomingConn.remoteAddr)
			} else {
				sendErr = utils.SendStripeHeader(stream, gid, uint8(i), uint8(len(legs)), uint8(s.config.StripeParity), incomingConn.remoteAddr)
			}
			if sendErr != nil {
				break
			}
		}
		if sendErr != nil {
			atomic.AddInt32(&s.stripedFlows, -1) // release before backoff
			s.logger.Tracef("failed to send stripe header: %v", sendErr)
			closeLegs(legs)
			if !a.wait(setupRetryBackoff) {
				a.drop()
				return
			}
			continue
		}
		for _, stream := range legs {
			_ = stream.SetWriteDeadline(time.Time{})
		}

		if !a.handoff() {
			// The guard already closed the local socket at expiry.
			atomic.AddInt32(&s.stripedFlows, -1)
			closeLegs(legs)
			a.drop()
			return
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
				closeLegs(legs)
				a.drop()
				return
			}
			stripedConn = fecConn
		} else {
			stripedConn = striping.New(conns, striping.DefaultChunkSize)
		}
		a.releasePermit() // the flow is active: its data transfer holds no setup permit

		defer atomic.AddInt32(&s.stripedFlows, -1) // hold the slot for the flow's lifetime
		defer atomic.AddInt32(&s.streamCounter, -1)
		handlers.TCPConnectionHandler(g.ctx, s.config.ProxyProtocol, incomingConn.conn, stripedConn, s.logger, s.usageMonitor, incomingConn.conn.LocalAddr().(*net.TCPAddr).Port, s.config.Sniffer)
		return
	}
}

func closeLegs(legs []*smux.Stream) {
	for _, st := range legs {
		st.Close()
	}
}

// legsPerFlow is how many pool legs one striped flow opens: the plain
// stripe factor, plus any FEC parity legs on top.
func (s *WsMuxTransport) legsPerFlow() int {
	return s.config.StripeFactor + s.config.StripeParity
}

// acquireStripedSlot reserves one in-flight striped-flow slot, waiting (with a
// short poll) while the pool is at its stream budget so a burst of new flows
// can't open more legs than the sessions can carry. The budget is
// sessionCounter*MuxCon streams and each flow uses StripeFactor legs, so at most
// sessionCounter*MuxCon/StripeFactor flows run at once; the budget grows as the
// pool does. Returns false when the attempt expired or the generation stopped.
func (s *WsMuxTransport) acquireStripedSlot(a *setupAttempt) bool {
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
		s.requestGrowth()
		if !a.wait(admissionPollInterval) {
			return false
		}
	}
}

func (s *WsMuxTransport) dispatchPromotable(g *wsGeneration, appConn net.Conn, plainStream net.Conn, flowID uint64, remoteAddr string) {
	swapper := handlers.PromotablePump(g.ctx, s.config.ProxyProtocol, appConn, plainStream, s.logger, s.usageMonitor, appConn.LocalAddr().(*net.TCPAddr).Port, s.config.Sniffer)

	if swapper == nil {
		return // failed proxy protocol
	}

	// A worker of the generation: it may be inside promoteFlow (blocking reads on
	// legs the generation owns) when a restart begins, and the restart waits for it.
	g.start(func() {
		for {
			select {
			case <-g.ctx.Done():
				return
			case <-swapper.DoneWait(): // Need a wait channel
				return
			case <-time.After(100 * time.Millisecond):
				if swapper.UpBytes() >= s.config.PromoteBytes {
					if s.shouldPromote() {
						s.promoteFlow(g.ctx, flowID, swapper)
					}
					// Whether or not it promoted, stop checking: a flow that
					// stayed plain because the pool was busy keeps running plain
					// (already spread across CDNs by load-balancing) for its life.
					return
				}
			}
		}
	})

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

func (s *WsMuxTransport) promoteFlow(ctx context.Context, flowID uint64, swapper *handlers.PumpSwapper) {
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

	// Freeze, exchange the byte counts on raw leg 0 (no striped reader exists
	// yet), then build the wrapper and install it. A failure before the freeze
	// leaves the flow plain; a later one aborts it (see PumpSwapper.Promote).
	err = swapper.Promote(ctx, conns, func() (net.Conn, error) {
		if s.config.StripeParity > 0 {
			return striping.NewFEC(conns, striping.DefaultChunkSize, s.config.StripeFactor, s.config.StripeParity)
		}
		return striping.New(conns, striping.DefaultChunkSize), nil
	})
	if err != nil {
		s.logger.Warnf("promotion of flow %d failed: %v", flowID, err)
		return
	}
	s.logger.Debugf("flow %d promoted to %d striped legs", flowID, len(conns))
}
