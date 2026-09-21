package transport

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/xtaci/smux"
)

// placementTimeout bounds every wait in these tests. It is generous next to the
// work (microseconds over net.Pipe) and only ever elapses when placement is
// actually wedged, which is the failure these tests exist to catch.
const placementTimeout = 5 * time.Second

// gatedConn wraps the smux-facing end of a net.Pipe. Once armed it blocks every
// Write until the gate is released or the conn is closed, which parks a real
// smux OpenStream at its SYN write - the exact point a stalled pool connection
// wedges an opener in production.
type gatedConn struct {
	net.Conn
	gate    chan struct{} // nil: never blocks
	entered chan struct{} // closed when the first Write hits the gate
	once    sync.Once
	done    chan struct{}
	closeMu sync.Once
}

func (c *gatedConn) Write(p []byte) (int, error) {
	if c.gate != nil {
		c.once.Do(func() { close(c.entered) })
		select {
		case <-c.gate:
		case <-c.done:
			return 0, io.ErrClosedPipe
		}
	}
	return c.Conn.Write(p)
}

func (c *gatedConn) Close() error {
	c.closeMu.Do(func() { close(c.done) })
	return c.Conn.Close()
}

// gatedSession is a registered pool session backed by real smux on both ends.
type gatedSession struct {
	ps *pooledSession
	c  *gatedConn
}

func (g *gatedSession) release() { close(g.c.gate) }

// addGatedSession registers a real smux pool session on s. When blocked is
// true its writes stall until release(). rtt (0 = unprobed) feeds legScore.
func addGatedSession(t *testing.T, s *WsMuxTransport, blocked bool, rtt time.Duration) *gatedSession {
	t.Helper()
	srvEnd, cliEnd := net.Pipe()
	gc := &gatedConn{Conn: srvEnd, entered: make(chan struct{}), done: make(chan struct{})}
	if blocked {
		gc.gate = make(chan struct{})
	}
	cfg := smux.DefaultConfig()
	cfg.KeepAliveDisabled = true // no background NOP writes racing the gate
	session, err := smux.Client(gc, cfg)
	if err != nil {
		t.Fatalf("smux.Client: %v", err)
	}
	peer, err := smux.Server(cliEnd, cfg)
	if err != nil {
		t.Fatalf("smux.Server: %v", err)
	}
	ps := &pooledSession{session: session, cdn: "test"}
	ps.rtt.Store(int64(rtt))
	s.sessionsMu.Lock()
	s.sessions = append(s.sessions, ps)
	s.sessionsMu.Unlock()
	t.Cleanup(func() {
		session.Close()
		peer.Close()
		gc.Close()
		cliEnd.Close()
	})
	return &gatedSession{ps: ps, c: gc}
}

func newPlacementTransport() *WsMuxTransport {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	return &WsMuxTransport{logger: logger}
}

type openResult struct {
	stream *smux.Stream
	ps     *pooledSession
	err    error
}

// startOpen runs openPlainLegPS on its own goroutine and reports the outcome on
// the returned channel (buffered, so an abandoned opener never leaks).
func startOpen(s *WsMuxTransport) <-chan openResult {
	ch := make(chan openResult, 1)
	go func() {
		st, ps, err := s.openPlainLegPS()
		ch <- openResult{st, ps, err}
	}()
	return ch
}

func mustRecv[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(placementTimeout):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}

// TestOpenPlainLegBlockedSession is the regression for one stalled OpenStream
// serializing every plain-flow placement. Session A's SYN write is parked; the
// first opener is proven to have reached that barrier; a second opener must
// then place on the healthy session B and finish while the first is still stuck.
func TestOpenPlainLegBlockedSession(t *testing.T) {
	s := newPlacementTransport()
	// A is faster, so with equal load the first opener picks it; once A carries
	// one (pending) stream its score doubles past B's and the next opener must
	// go to B.
	a := addGatedSession(t, s, true, 10*time.Millisecond)
	b := addGatedSession(t, s, false, 15*time.Millisecond)

	first := startOpen(s)
	mustRecv(t, a.c.entered, "the first opener to reach session A's blocked SYN write")

	second := startOpen(s)
	got := mustRecv(t, second, "the second opener to finish while the first is blocked")
	if got.err != nil {
		t.Fatalf("second open: %v", got.err)
	}
	defer got.stream.Close()
	if got.ps != b.ps {
		t.Fatalf("second open landed on the wrong session, want the healthy one")
	}

	select {
	case r := <-first:
		t.Fatalf("first opener returned while session A is still blocked: %+v", r)
	default:
	}

	// Unblock A: the first opener completes on A.
	a.release()
	r := mustRecv(t, first, "the first opener to finish after A is released")
	if r.err != nil {
		t.Fatalf("first open after release: %v", r.err)
	}
	defer r.stream.Close()
	if r.ps != a.ps {
		t.Fatalf("first open should have stayed on session A")
	}
}

// pendingOf reads a session's reservation count the way production does.
func pendingOf(s *WsMuxTransport, ps *pooledSession) int {
	s.plainSelectMu.Lock()
	defer s.plainSelectMu.Unlock()
	return ps.pendingOpens
}

// waitPending blocks until the reservation total over sessions reaches want. It
// polls an observable condition (never a fixed delay) and is bounded.
func waitPending(t *testing.T, s *WsMuxTransport, want int, sessions ...*gatedSession) {
	t.Helper()
	deadline := time.After(placementTimeout)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		total := 0
		for _, g := range sessions {
			total += pendingOf(s, g.ps)
		}
		if total == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %d pending reservations, have %d", want, total)
		case <-tick.C:
		}
	}
}

func assertNoPending(t *testing.T, s *WsMuxTransport, sessions ...*gatedSession) {
	t.Helper()
	for i, g := range sessions {
		if got := pendingOf(s, g.ps); got != 0 {
			t.Errorf("session %d still holds %d pending reservation(s), want exactly 0", i, got)
		}
	}
}

// TestOpenPlainLegBlockedSessionReservations pins the reservation lifecycle in
// the blocked-session scenario: a blocked opener holds exactly one reservation on
// its session (which is what steers the next flow away), and every reservation
// is gone once the opens complete.
func TestOpenPlainLegBlockedSessionReservations(t *testing.T) {
	s := newPlacementTransport()
	a := addGatedSession(t, s, true, 10*time.Millisecond)
	b := addGatedSession(t, s, false, 15*time.Millisecond)

	first := startOpen(s)
	mustRecv(t, a.c.entered, "the first opener to reach session A's blocked SYN write")
	if got := pendingOf(s, a.ps); got != 1 {
		t.Fatalf("blocked opener should hold exactly 1 reservation on A, got %d", got)
	}

	second := mustRecv(t, startOpen(s), "the second opener")
	if second.err != nil {
		t.Fatalf("second open: %v", second.err)
	}
	defer second.stream.Close()
	if got := pendingOf(s, a.ps); got != 1 {
		t.Errorf("A's reservation must survive the second open, got %d", got)
	}
	assertNoPending(t, s, b) // success path settles

	a.release()
	r := mustRecv(t, first, "the first opener after release")
	if r.err != nil {
		t.Fatalf("first open: %v", r.err)
	}
	defer r.stream.Close() // late return: the caller still owns and closes it
	assertNoPending(t, s, a, b)
}

// TestOpenPlainLegReservationsSettle covers the other outcomes: the reservation
// must return to exactly zero on success, when there is no live session, when the
// only candidate's session closes under a blocked open, and when a failed open
// is retried onto another session.
func TestOpenPlainLegReservationsSettle(t *testing.T) {
	t.Run("session closes while opening, no other candidate", func(t *testing.T) {
		s := newPlacementTransport()
		a := addGatedSession(t, s, true, 0)

		first := startOpen(s)
		mustRecv(t, a.c.entered, "the opener to reach the blocked SYN write")
		a.ps.session.Close()

		r := mustRecv(t, first, "the failed open")
		if r.err == nil {
			r.stream.Close()
			t.Fatal("open on a closed session must fail")
		}
		assertNoPending(t, s, a)
	})

	t.Run("failed open is retried on another session", func(t *testing.T) {
		s := newPlacementTransport()
		a := addGatedSession(t, s, true, 10*time.Millisecond) // picked first
		b := addGatedSession(t, s, false, 15*time.Millisecond)

		first := startOpen(s)
		mustRecv(t, a.c.entered, "the opener to reach A's blocked SYN write")
		a.ps.session.Close()

		r := mustRecv(t, first, "the retried open")
		if r.err != nil {
			t.Fatalf("retry on the healthy session failed: %v", r.err)
		}
		defer r.stream.Close()
		if r.ps != b.ps {
			t.Fatal("retry must land on the healthy session, not the failed one")
		}
		assertNoPending(t, s, a, b)
	})

	t.Run("no live session", func(t *testing.T) {
		s := newPlacementTransport()
		a := addGatedSession(t, s, false, 0)
		a.ps.session.Close()
		if _, _, err := s.openPlainLegPS(); err == nil {
			t.Fatal("open with no live session must fail")
		}
		assertNoPending(t, s, a)
	})

	t.Run("success", func(t *testing.T) {
		s := newPlacementTransport()
		a := addGatedSession(t, s, false, 0)
		st, ps, err := s.openPlainLegPS()
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer st.Close()
		if ps != a.ps {
			t.Fatal("wrong session")
		}
		assertNoPending(t, s, a)
	})
}

// TestOpenPlainLegConcurrentBurstSpreads proves the reservation keeps a
// simultaneous burst from concentrating even though no lock is held across the
// open. Every session's SYN write is parked, so nothing registers in NumStreams:
// the spread can only come from reservations. 12 concurrent openers over 4
// equally-scored sessions must reserve exactly 3 each before any of them returns.
func TestOpenPlainLegConcurrentBurstSpreads(t *testing.T) {
	s := newPlacementTransport()
	const nSessions, nFlows = 4, 12
	sessions := make([]*gatedSession, nSessions)
	for i := range sessions {
		sessions[i] = addGatedSession(t, s, true, 0) // equal (unprobed) score
	}

	results := make([]<-chan openResult, nFlows)
	for i := range results {
		results[i] = startOpen(s)
	}
	waitPending(t, s, nFlows, sessions...)
	for i, g := range sessions {
		if got := pendingOf(s, g.ps); got != nFlows/nSessions {
			t.Errorf("session %d holds %d reservations, want an even %d", i, got, nFlows/nSessions)
		}
	}

	for _, g := range sessions {
		g.release()
	}
	for i, ch := range results {
		r := mustRecv(t, ch, "a burst opener to finish")
		if r.err != nil {
			t.Fatalf("open %d: %v", i, r.err)
		}
		defer r.stream.Close()
	}
	for i, g := range sessions {
		if got := g.ps.session.NumStreams(); got != nFlows/nSessions {
			t.Errorf("session %d carries %d streams, want %d", i, got, nFlows/nSessions)
		}
	}
	assertNoPending(t, s, sessions...)
}
