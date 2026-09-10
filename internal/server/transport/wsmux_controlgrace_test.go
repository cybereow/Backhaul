package transport

import (
	"io"
	"net"
	"testing"
	"time"

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

// TestOpenPlainLegRoundRobin guards the upload-aggregation fix: plain (single-
// leg) flows must spread evenly across every pool session, not pile onto one.
// The old central selection took the single lowest-score session, so a burst of
// concurrent plain flows - all reading the same near-zero load on an unprobed
// pool - concentrated on one connection, whose single-connection upload ceiling
// then capped the aggregate. Round-robin placement restores the even spread the
// old per-session work-stealing loop had.
func TestOpenPlainLegRoundRobin(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	s := &WsMuxTransport{logger: logger}

	const nSessions = 4
	for i := 0; i < nSessions; i++ {
		defer livePoolSession(t, s)()
	}

	const nFlows = 12 // an exact multiple of nSessions: perfect round-robin => 3 each
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

	s.onControlGraceExpired()

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
