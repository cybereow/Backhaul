package transport

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/musix/backhaul/config"
	"github.com/musix/backhaul/internal/utils/network"
	"github.com/sirupsen/logrus"
	"github.com/xtaci/smux"
)

// livePoolSession returns a real, open smux session registered on s, plus a
// cleanup. Mirrors the pool: the server opens streams, the peer accepts them.
func livePoolSession(t *testing.T, s *WsMuxTransport) func() {
	t.Helper()
	srvConn, cliConn := net.Pipe()
	session, err := smux.Client(srvConn, smux.DefaultConfig())
	if err != nil {
		t.Fatalf("smux.Client: %v", err)
	}
	peer, err := smux.Server(cliConn, smux.DefaultConfig())
	if err != nil {
		t.Fatalf("smux.Server: %v", err)
	}
	s.sessions = append(s.sessions, &pooledSession{session: session, cdn: "test"})
	return func() {
		session.Close()
		peer.Close()
		srvConn.Close()
		cliConn.Close()
	}
}

// TestOpenPlainLegSpreads guards the upload-aggregation fix: a burst of plain
// (single-leg) flows must spread across every pool session, not pile onto one.
// Dispatching each flow to the single lowest-score session concentrated a burst
// of them on one connection (they all read the same pre-OpenStream load and
// picked the same session), whose single-connection upload ceiling then capped
// the aggregate. openPlainLeg serializes the score-pick-open sequence so each
// flow's OpenStream raises its session's score before the next flow scores;
// with the sessions here equally scored (unprobed) that yields an even spread.
func TestOpenPlainLegSpreads(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	s := &WsMuxTransport{logger: logger}

	const nSessions = 4
	for i := 0; i < nSessions; i++ {
		defer livePoolSession(t, s)()
	}

	const nFlows = 12 // an exact multiple of nSessions: even spread => 3 each
	streams := make([]*smux.Stream, 0, nFlows)
	for i := 0; i < nFlows; i++ {
		st, err := s.openPlainLeg()
		if err != nil {
			t.Fatalf("openPlainLeg %d: %v", i, err)
		}
		streams = append(streams, st)
	}
	defer func() {
		for _, st := range streams {
			st.Close()
		}
	}()

	for i, ps := range s.sessions {
		if got := ps.session.NumStreams(); got != nFlows/nSessions {
			t.Errorf("session %d carries %d streams, want an even %d (flows must spread, not concentrate)", i, got, nFlows/nSessions)
		}
	}
}

// TestOpenPlainLegPrefersFastCDN confirms the spread stays latency-aware: given
// sessions of differing RTT at equal load, the next plain flow lands on the
// lower-RTT one, so the placement favours the fast CDNs (and leaves the slow
// tail unused) instead of round-robining blindly. rtt is set directly here;
// in production probeSessionRTT keeps it current on the plain path too.
func TestOpenPlainLegPrefersFastCDN(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	s := &WsMuxTransport{logger: logger}

	defer livePoolSession(t, s)() // s.sessions[0]
	defer livePoolSession(t, s)() // s.sessions[1]
	s.sessions[0].rtt.Store(int64(90 * time.Millisecond))
	s.sessions[1].rtt.Store(int64(15 * time.Millisecond)) // the fast CDN

	st, err := s.openPlainLeg()
	if err != nil {
		t.Fatalf("openPlainLeg: %v", err)
	}
	defer st.Close()

	if s.sessions[1].session.NumStreams() != 1 || s.sessions[0].session.NumStreams() != 0 {
		t.Errorf("the first plain flow should land on the lower-RTT session, got fast=%d slow=%d",
			s.sessions[1].session.NumStreams(), s.sessions[0].session.NumStreams())
	}
}

// TestControlGraceHoldsWhilePoolAlive verifies the core fix: when the control
// channel has not reattached but the pool still carries a live session (and the
// hold is within the cap), the grace handler holds (re-arms) instead of
// restarting the whole transport - which would drop every in-flight flow just
// because a CDN was slow to reconnect the side channel.
func TestControlGraceHoldsWhilePoolAlive(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	s := &WsMuxTransport{logger: logger}

	cleanup := livePoolSession(t, s) // one genuinely-open pool session
	defer cleanup()
	s.graceStart = time.Now() // just lost the control channel; well within the cap
	// controlChannel is nil (lost) and not reattached.

	s.onControlGraceExpired(nil, 0) // the initial epoch of an untracked transport

	// It must have recorded a hold (not a restart) and re-armed the timer.
	ev := s.snapshotEvents()
	if len(ev) == 0 {
		t.Fatal("expected a recorded event")
	}
	last := ev[len(ev)-1]
	if last.Kind != "control_hold" {
		t.Fatalf("expected a control_hold event, got kind %q (%+v)", last.Kind, ev)
	}
	for _, e := range ev {
		if e.Kind == "restart" {
			t.Errorf("must not restart while the pool is alive, but recorded: %+v", e)
		}
	}

	s.controlMu.Lock()
	armed := s.graceTimer != nil
	if s.graceTimer != nil {
		s.graceTimer.Stop() // don't leave a 30s timer running in the test process
	}
	s.controlMu.Unlock()
	if !armed {
		t.Error("grace timer should be re-armed while holding")
	}
}

// TestGraceShouldHold guards the hold/restart decision, including the cap that
// keeps a silently-dropped client (sessions never report closed, e.g. mux
// keepalive disabled) from being held "up" forever.
func TestGraceShouldHold(t *testing.T) {
	if !graceShouldHold(1, time.Second) {
		t.Error("a live session within the cap should hold")
	}
	if graceShouldHold(0, time.Second) {
		t.Error("no live sessions should not hold (restart to rebuild)")
	}
	if graceShouldHold(3, maxControlGraceHold+time.Second) {
		t.Error("past the cap it must stop holding even with live sessions, so a stale pool is rebuilt")
	}
}

// TestLiveSessionCount confirms the grace decision counts real open sessions,
// not the lagging sessionCounter: an open session counts, a closed one does not.
func TestLiveSessionCount(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	s := &WsMuxTransport{logger: logger}

	cleanup := livePoolSession(t, s)
	defer cleanup()
	if got := s.liveSessionCount(); got != 1 {
		t.Fatalf("expected 1 live session, got %d", got)
	}

	// Close it: it must no longer count as live.
	s.sessions[0].session.Close()
	if got := s.liveSessionCount(); got != 0 {
		t.Fatalf("a closed session must not count as live, got %d", got)
	}
}

// --- grace expiry versus control reattachment (plan 012) -------------------
//
// The tests below drive onControlGraceExpired directly with the (generation,
// epoch) its timer would have captured, and pause it between "decided to
// restart" and "revalidate" with graceRevalidateHook. Every ordering is forced
// with channels; nothing waits on the real 30 second window.

// newGraceRace returns a transport with a tracked generation and a control
// channel already adopted. Its parent context is cancelled, so a Restart that
// does run tears the generation down and then abandons instead of starting a new
// one (which needs a full transport): "g is stopped" then means "Restart ran".
func newGraceRace(t *testing.T) (*WsMuxTransport, *wsGeneration, *network.WebSocketConn) {
	t.Helper()
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	g := newWsGeneration(context.Background())
	t.Cleanup(g.stop)
	s := &WsMuxTransport{
		logger:    logger,
		config:    &WsMuxConfig{Mode: config.WSMUX},
		parentctx: parent,
		gen:       g,
		ctx:       g.ctx,
	}
	t.Cleanup(func() {
		s.controlMu.Lock()
		s.invalidateGraceLocked() // don't leave a 30s timer running in the test process
		s.controlMu.Unlock()
	})
	c1 := graceConn(t)
	if _, first, ok := s.adoptControl(g, c1); !ok || !first {
		t.Fatalf("initial adoption: first=%v ok=%v", first, ok)
	}
	return s, g, c1
}

// graceConn is a control connection nothing reads or writes; only its identity
// and Close matter to the grace logic.
func graceConn(t *testing.T) *network.WebSocketConn {
	t.Helper()
	a, b := net.Pipe()
	t.Cleanup(func() { a.Close(); b.Close() })
	return network.NewWebSocketConn(a, ws.StateServerSide, nil)
}

func graceEpoch(s *WsMuxTransport) uint64 {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	return s.graceEpoch
}

func graceEvents(s *WsMuxTransport, kind string) int {
	n := 0
	for _, e := range s.snapshotEvents() {
		if e.Kind == kind {
			n++
		}
	}
	return n
}

// pauseGrace makes the next expiry callback stop after it has gathered the live
// count, and reports when it has (gathered) and lets it go (resume).
func pauseGrace(s *WsMuxTransport) (gathered <-chan struct{}, resume func()) {
	in, out := make(chan struct{}), make(chan struct{})
	s.graceRevalidateHook = func() { close(in); <-out }
	return in, func() { close(out) }
}

// runGrace runs one expiry callback and reports when it has returned.
func runGrace(s *WsMuxTransport, g *wsGeneration, epoch uint64) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.onControlGraceExpired(g, epoch)
	}()
	return done
}

// TestControlGraceReattachWins: the callback has decided to restart (empty pool)
// and paused; the client reattaches; the callback resumes. The adoption won, so
// the callback must not claim a restart and the recovered channel stays current.
func TestControlGraceReattachWins(t *testing.T) {
	s, g, c1 := newGraceRace(t)
	s.onControlLost(g, c1)
	epoch := graceEpoch(s)
	gathered, resume := pauseGrace(s)

	done := runGrace(s, g, epoch)
	lcWaitClosed(t, "the callback to reach its revalidation point", gathered)

	c2 := graceConn(t)
	if _, first, ok := s.adoptControl(g, c2); !ok || first {
		t.Fatalf("reattach: first=%v ok=%v, want an adopted reattach", first, ok)
	}
	resume()
	lcWaitClosed(t, "the callback to return", done)

	if g.isStopped() || graceEvents(s, "restart") != 0 {
		t.Fatal("the callback restarted a transport whose control channel had just reattached")
	}
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	if s.controlChannel != c2 {
		t.Fatal("the reattached control channel is no longer current")
	}
	if s.restartClaim != nil || s.graceTimer != nil {
		t.Fatalf("stale claim/timer left behind: claim=%v timer=%v", s.restartClaim != nil, s.graceTimer != nil)
	}
	if want := "Connected (" + string(config.WSMUX) + ")"; s.config.TunnelStatus != want {
		t.Fatalf("status = %q, want %q", s.config.TunnelStatus, want)
	}
}

// TestControlGraceRestartClaimWins: the callback claims the restart first. That
// happens exactly once for the epoch, and a control channel arriving afterwards is
// refused rather than adopted into the generation that is being torn down.
func TestControlGraceRestartClaimWins(t *testing.T) {
	s, g, c1 := newGraceRace(t)
	s.onControlLost(g, c1)
	epoch := graceEpoch(s)

	lcWaitClosed(t, "the callback to return", runGrace(s, g, epoch))
	if !g.isStopped() || graceEvents(s, "restart") != 1 {
		t.Fatalf("expected exactly one restart of the generation: stopped=%v restarts=%d", g.isStopped(), graceEvents(s, "restart"))
	}

	// A repeated callback for the same epoch is inert: no second claim.
	lcWaitClosed(t, "the repeated callback to return", runGrace(s, g, epoch))
	if n := graceEvents(s, "restart"); n != 1 {
		t.Fatalf("a repeated callback for the same epoch claimed again: %d restarts", n)
	}

	if _, _, ok := s.adoptControl(g, graceConn(t)); ok {
		t.Fatal("a control channel was adopted into a generation whose restart was claimed")
	}
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	if s.controlChannel != nil {
		t.Fatal("the refused control channel became current")
	}
}

// TestControlGraceClaimRefusesAdoptionBeforeStop: the claim alone, before Restart
// has stopped the generation, already closes the generation to adoption.
func TestControlGraceClaimRefusesAdoptionBeforeStop(t *testing.T) {
	s, g, c1 := newGraceRace(t)
	s.onControlLost(g, c1)
	s.controlMu.Lock()
	s.restartClaim = g
	s.controlMu.Unlock()
	if g.isStopped() {
		t.Fatal("test setup: generation must not be stopped yet")
	}
	if _, _, ok := s.adoptControl(g, graceConn(t)); ok {
		t.Fatal("adopted into a generation with a restart claim")
	}
	// A different generation is not affected by the claim.
	if _, _, ok := s.adoptControl(newWsGeneration(context.Background()), graceConn(t)); !ok {
		t.Fatal("a claim on one generation refused adoption into another")
	}
}

// TestControlGraceOldEpoch: a callback from loss 1 that runs late, after the
// channel reattached and was lost again (loss 2), must neither restart nor extend
// loss 1's grace into loss 2: the pending timer and the epoch stay loss 2's.
func TestControlGraceOldEpoch(t *testing.T) {
	s, g, c1 := newGraceRace(t)
	s.onControlLost(g, c1)
	oldEpoch := graceEpoch(s)
	gathered, resume := pauseGrace(s)

	done := runGrace(s, g, oldEpoch)
	lcWaitClosed(t, "the callback to reach its revalidation point", gathered)

	c2 := graceConn(t)
	if _, _, ok := s.adoptControl(g, c2); !ok {
		t.Fatal("reattach refused")
	}
	s.onControlLost(g, c2) // loss 2
	s.controlMu.Lock()
	timer2, epoch2 := s.graceTimer, s.graceEpoch
	s.controlMu.Unlock()
	if timer2 == nil || epoch2 == oldEpoch {
		t.Fatal("loss 2 did not start its own epoch and timer")
	}

	resume()
	lcWaitClosed(t, "the callback to return", done)

	if g.isStopped() || graceEvents(s, "restart") != 0 || graceEvents(s, "control_hold") != 0 {
		t.Fatalf("the old-epoch callback acted: stopped=%v events=%+v", g.isStopped(), s.snapshotEvents())
	}
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	if s.graceTimer != timer2 || s.graceEpoch != epoch2 {
		t.Fatal("the old-epoch callback replaced loss 2's timer or epoch")
	}
}

// TestControlGraceHoldRearmsSameEpoch: holding re-arms the timer for the same
// epoch and does not restart the loss clock, so the 90s cap counts from the
// original loss however many times the window is re-armed.
func TestControlGraceHoldRearmsSameEpoch(t *testing.T) {
	s, g, c1 := newGraceRace(t)
	defer livePoolSession(t, s)()
	s.onControlLost(g, c1)
	s.controlMu.Lock()
	epoch, start := s.graceEpoch, s.graceStart
	first := s.graceTimer
	s.controlMu.Unlock()

	lcWaitClosed(t, "the callback to return", runGrace(s, g, epoch))

	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	if graceEvents(s, "control_hold") != 1 || graceEvents(s, "restart") != 0 {
		t.Fatalf("expected a hold: %+v", s.snapshotEvents())
	}
	if s.graceEpoch != epoch || !s.graceStart.Equal(start) {
		t.Fatal("re-arming changed the loss epoch or restarted the loss clock")
	}
	if s.graceTimer == nil || s.graceTimer == first {
		t.Fatal("the hold did not re-arm a fresh timer")
	}
}

// TestControlGraceHoldCapStillRestarts: the 90s cap is unchanged - a loss held
// past it restarts even with a live session.
func TestControlGraceHoldCapStillRestarts(t *testing.T) {
	s, g, c1 := newGraceRace(t)
	defer livePoolSession(t, s)()
	s.onControlLost(g, c1)
	s.controlMu.Lock()
	epoch := s.graceEpoch
	s.graceStart = time.Now().Add(-maxControlGraceHold - time.Second)
	s.controlMu.Unlock()

	lcWaitClosed(t, "the callback to return", runGrace(s, g, epoch))
	if !g.isStopped() || graceEvents(s, "restart") != 1 {
		t.Fatalf("a hold past the cap must restart: stopped=%v events=%+v", g.isStopped(), s.snapshotEvents())
	}
}

// gateCloseConn blocks Close until released, so a test can hold onControlLost at
// the point after it has dropped controlMu.
type gateCloseConn struct {
	net.Conn
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *gateCloseConn) Close() error {
	c.once.Do(func() { close(c.entered) })
	<-c.release
	return c.Conn.Close()
}

// TestControlGraceLateReconnectingStatus: onControlLost is still closing the
// dead socket when the client reattaches. Its Reconnecting status must not land
// after (and overwrite) the Connected one the reattach published.
func TestControlGraceLateReconnectingStatus(t *testing.T) {
	s, g, _ := newGraceRace(t)
	a, b := net.Pipe()
	t.Cleanup(func() { a.Close(); b.Close() })
	gate := &gateCloseConn{Conn: a, entered: make(chan struct{}), release: make(chan struct{})}
	dying := network.NewWebSocketConn(gate, ws.StateServerSide, nil)
	if _, _, ok := s.adoptControl(g, dying); !ok { // replaces c1 with the connection about to die
		t.Fatal("adoption refused")
	}

	lost := make(chan struct{})
	go func() {
		defer close(lost)
		s.onControlLost(g, dying)
	}()
	lcWaitClosed(t, "onControlLost to reach the socket close", gate.entered)

	if _, _, ok := s.adoptControl(g, graceConn(t)); !ok {
		t.Fatal("reattach refused")
	}
	close(gate.release)
	lcWaitClosed(t, "onControlLost to return", lost)

	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	if want := "Connected (" + string(config.WSMUX) + ")"; s.config.TunnelStatus != want {
		t.Fatalf("status = %q after a late loss report, want %q", s.config.TunnelStatus, want)
	}
	if s.controlChannel == nil || s.controlChannel == dying {
		t.Fatal("the reattached control channel is not current")
	}
}

// TestControlGraceStaleLossIsInert: a loss report for a connection that is no
// longer registered changes nothing (no epoch, no timer, no status).
func TestControlGraceStaleLossIsInert(t *testing.T) {
	s, g, c1 := newGraceRace(t)
	c2 := graceConn(t)
	if _, _, ok := s.adoptControl(g, c2); !ok {
		t.Fatal("reattach refused")
	}
	epoch := graceEpoch(s)
	s.onControlLost(g, c1) // c1 was replaced
	if graceEpoch(s) != epoch {
		t.Fatal("a stale loss advanced the epoch")
	}
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	if s.controlChannel != c2 || s.graceTimer != nil || !strings.HasPrefix(s.config.TunnelStatus, "Connected") {
		t.Fatalf("a stale loss disturbed the current channel: current=%v timer=%v status=%q", s.controlChannel == c2, s.graceTimer != nil, s.config.TunnelStatus)
	}
}

// TestControlGraceReattachKeepsFlow is the end-to-end form of the race: a real
// transport with a pool session and a running flow. The control channel drops,
// the grace callback decides to restart (the hold is past the cap) and pauses,
// the client reattaches, and the callback resumes. The flow must carry exactly
// the bytes it is sent afterwards, on the same pool session, in the same
// generation.
func TestControlGraceReattachKeepsFlow(t *testing.T) {
	h := newLCHarness(t)
	ctl := h.control()
	peer := h.pool()
	lcWaitFor(t, "an admitted session", func() bool { return h.sessions() == 1 })
	user := h.user()
	lcEcho(t, user, "before")
	gen := h.s.gen

	ctl.Close()
	lcWaitFor(t, "the server to notice the lost control channel", func() bool {
		h.s.controlMu.Lock()
		defer h.s.controlMu.Unlock()
		return h.s.controlChannel == nil && h.s.graceTimer != nil
	})
	h.s.controlMu.Lock()
	epoch := h.s.graceEpoch
	h.s.graceStart = time.Now().Add(-maxControlGraceHold - time.Second) // past the cap: restart is the decision
	h.s.controlMu.Unlock()

	gathered, resume := pauseGrace(h.s)
	done := runGrace(h.s, gen, epoch)
	lcWaitClosed(t, "the callback to reach its revalidation point", gathered)

	h.control() // the client reattaches while the callback is paused
	lcWaitFor(t, "the control channel to be reattached", func() bool {
		h.s.controlMu.Lock()
		defer h.s.controlMu.Unlock()
		return h.s.controlChannel != nil
	})
	resume()
	lcWaitClosed(t, "the callback to return", done)

	payload := strings.Repeat("0123456789abcdef", 64)
	lcEcho(t, user, payload)
	select {
	case <-peer.readClosed:
		t.Fatal("the pool session was closed although the control channel had reattached")
	default:
	}
	if h.s.gen != gen || gen.isStopped() || graceEvents(h.s, "restart") != 0 {
		t.Fatal("the grace callback restarted the transport after the reattach")
	}
	if n := h.sessions(); n != 1 {
		t.Fatalf("session counter is %d, want 1", n)
	}
}
