package transport

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"
)

// A connection whose socket has timed out twice in a row without an
// acknowledgement, or whose RTT probe went unanswered, is stalled: it is charged
// so heavily at placement that anything else is picked first. It is itself again
// as soon as neither sign shows.
func TestStalledSessionIsChargedUntilItGetsThrough(t *testing.T) {
	feed := newCapFeed()
	hosts := map[string]*hostCapacity{}
	ok, bad := capSession(feed, "a"), capSession(feed, "a")
	sessions := []*pooledSession{ok, bad}

	feed.step(sessions, hosts)
	if ok.slowness() != 1 || bad.slowness() != 1 {
		t.Fatalf("healthy sessions are charged: %v %v", ok.slowness(), bad.slowness())
	}

	// One timeout is ordinary loss; two in a row is a connection gone silent.
	feed.get(bad.conn).Backoff = 1
	feed.step(sessions, hosts)
	if bad.slowness() != 1 {
		t.Fatal("a single retransmission timeout marked the connection as stalled")
	}
	feed.get(bad.conn).Backoff = stallBackoff
	feed.step(sessions, hosts)
	if bad.slowness() < stallPenalty || ok.slowness() != 1 {
		t.Fatalf("stalled session charged %v, healthy one %v", bad.slowness(), ok.slowness())
	}
	// With 30 streams the healthy one still costs less than the stalled one idle.
	if legScoreValue(30, 0)*ok.slowness() >= legScoreValue(0, 0)*bad.slowness() {
		t.Fatal("a stalled connection can still be the cheapest to place a flow on")
	}
	since := bad.stalledSince.Load()
	feed.step(sessions, hosts)
	if bad.stalledSince.Load() != since {
		t.Fatal("the start of the stall moved while it lasted")
	}
	if got := bad.stalledFor(feed.now); got != time.Second {
		t.Fatalf("stalled for %v, want 1s", got)
	}

	feed.get(bad.conn).Backoff = 0
	feed.step(sessions, hosts)
	if bad.slowness() != 1 || bad.stalledFor(feed.now) != 0 {
		t.Fatal("the connection stayed marked after it got through again")
	}

	// The other sign: an unanswered probe, with a socket that looks fine.
	bad.probeFails.Store(1)
	feed.step(sessions, hosts)
	if bad.slowness() < stallPenalty {
		t.Fatal("an unanswered RTT probe did not mark the connection as stalled")
	}
	bad.probeFails.Store(0)
	feed.step(sessions, hosts)
	if bad.slowness() != 1 {
		t.Fatal("an answered probe did not clear the mark")
	}
}

// A flow on a connection that stops getting through is not left to wait it out:
// new flows are placed elsewhere at once, and after stallMoveAfter the flow is
// resumed on another connection from its replay state, with every byte intact.
func TestWSMuxFlowLeavesAStalledSession(t *testing.T) {
	tg := echoTarget(t)
	h := newLCHarness(t, toTarget(tg.Addr().String()), func(c *WsMuxConfig) {
		c.MaxConnAge = time.Hour
		c.MaxDrain = time.Minute
		c.ResumeWindow = 20 * time.Second
	})
	startResumeClient(t, h, 4)

	user := h.user().(*net.TCPConn)
	_ = user.SetDeadline(time.Now().Add(30 * time.Second))
	echo := func(step string, msg []byte) {
		t.Helper()
		if _, err := user.Write(msg); err != nil {
			t.Fatalf("%s: %v", step, err)
		}
		got := make([]byte, len(msg))
		if _, err := io.ReadFull(user, got); err != nil || !bytes.Equal(got, msg) {
			t.Fatalf("%s: echo failed: %v", step, err)
		}
	}
	echo("before the stall", promoPayload(200*1024, 3))
	f := soleFlow(t, h)
	stalled := f.session()

	h.s.sessionsMu.Lock()
	sessions := append([]*pooledSession(nil), h.s.sessions...)
	h.s.sessionsMu.Unlock()
	var ps *pooledSession
	for _, x := range sessions {
		if x.session == stalled {
			ps = x
		}
	}
	if ps == nil {
		t.Fatal("the flow's session is not in the registry")
	}

	// The connection has just gone silent: nothing new is placed on it.
	now := time.Now()
	ps.probeFails.Store(1)
	ps.noteHealth(now, false)
	for i := 0; i < 12; i++ {
		st, on, err := h.s.openPlainLegPS()
		if err != nil {
			t.Fatal(err)
		}
		if on.session == stalled {
			t.Fatal("a new stream was placed on the stalled connection")
		}
		defer st.Close()
	}
	// Too early to move anything: it may be a blip.
	h.s.moveOffStalled(h.s.gen, sessions, now.Add(stallMoveAfter/2))
	if f.sw.Resumes() != 0 || f.session() != stalled {
		t.Fatal("the flow was moved before the stall had lasted")
	}

	h.s.moveOffStalled(h.s.gen, sessions, now.Add(stallMoveAfter))
	lcWaitFor(t, "the flow to resume on another connection", func() bool {
		return f.sw.Resumes() >= 1 && f.session() != stalled
	})

	echo("after leaving the stalled connection", promoPayload(300*1024, 4))
}

// A stream whose header does not come holds up nothing but itself: the client
// reads each new stream's header on its own, so a flow opened on the same
// connection right after it starts at once instead of after the header timeout.
func TestWSMuxSilentStreamDoesNotHoldUpTheSession(t *testing.T) {
	tg := echoTarget(t)
	h := newLCHarness(t, toTarget(tg.Addr().String()))
	startResumeClient(t, h, 1) // one connection: the next flow must share it

	h.s.sessionsMu.Lock()
	sess := h.s.sessions[0].session
	h.s.sessionsMu.Unlock()
	silent, err := sess.OpenStream() // opened, and its header never sent
	if err != nil {
		t.Fatal(err)
	}
	defer silent.Close()

	user := h.user().(*net.TCPConn)
	_ = user.SetDeadline(time.Now().Add(2 * time.Second)) // the header timeout is 5 s here
	msg := promoPayload(4096, 7)
	if _, err := user.Write(msg); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(user, got); err != nil || !bytes.Equal(got, msg) {
		t.Fatalf("a flow opened after a silent stream on the same connection did not get through in time: %v", err)
	}
}

// Striped legs are picked one per CDN before anything else, so a charge alone
// would not keep a stalled CDN's only session out of a group: it is left out
// while enough sessions that get through remain, and used when they do not.
func TestStripedLegsLeaveOutAStalledSession(t *testing.T) {
	a1, a2 := &pooledSession{cdn: "A"}, &pooledSession{cdn: "A"}
	b := &pooledSession{cdn: "B"}
	b.stalledSince.Store(time.Now().UnixNano())
	score := func(ps *pooledSession) float64 { return 1 }

	for _, ps := range selectLegs(withoutStalled([]*pooledSession{a1, b, a2}, 2), 2, score) {
		if ps == b {
			t.Fatal("a two-leg group took the stalled session although two others get through")
		}
	}
	got := selectLegs(withoutStalled([]*pooledSession{a1, b, a2}, 3), 3, score)
	if len(got) != 3 {
		t.Fatalf("a three-leg group got %d legs: with too few healthy sessions the stalled one must still be used", len(got))
	}
}

// With nowhere to go, flows stay where they are: taking them off the only
// connection (or one of several that are all stalled) would end them at the end
// of the resume window, although the stall may simply pass. And a flow whose
// resume is still under way is not reported again on every pass.
func TestWSMuxStalledFlowsStayWithoutADestination(t *testing.T) {
	tg := echoTarget(t)
	h := newLCHarness(t, toTarget(tg.Addr().String()), func(c *WsMuxConfig) {
		c.MaxConnAge = time.Hour
		c.MaxDrain = time.Minute
		c.ResumeWindow = 20 * time.Second
	})
	startResumeClient(t, h, 2)
	user := h.user().(*net.TCPConn)
	_ = user.SetDeadline(time.Now().Add(20 * time.Second))
	msg := promoPayload(64*1024, 5)
	user.Write(msg)
	if _, err := io.ReadFull(user, make([]byte, len(msg))); err != nil {
		t.Fatal(err)
	}
	f := soleFlow(t, h)

	h.s.sessionsMu.Lock()
	sessions := append([]*pooledSession(nil), h.s.sessions...)
	h.s.sessionsMu.Unlock()
	now := time.Now()
	for _, ps := range sessions { // every connection is stalled
		ps.probeFails.Store(1)
		ps.noteHealth(now, false)
	}
	h.s.moveOffStalled(h.s.gen, sessions, now.Add(2*stallMoveAfter))
	select {
	case <-f.sw.SuspendedCh():
		t.Fatal("a flow was suspended with no connection left to resume it on")
	default:
	}

	// One of them gets through again: the flow on the other is moved, once.
	var other *pooledSession
	for _, ps := range sessions {
		if ps.session != f.session() {
			other = ps
		}
	}
	other.probeFails.Store(0)
	other.noteHealth(now, false)
	for i := 0; i < 5; i++ {
		h.s.moveOffStalled(h.s.gen, sessions, now.Add(2*stallMoveAfter))
	}
	lcWaitFor(t, "the flow to resume on the connection that gets through", func() bool { return f.sw.Resumes() >= 1 })
	if n := graceEvents(h.s, "session_stalled"); n != 1 {
		t.Fatalf("%d session_stalled events for one flow moved once", n)
	}
}
