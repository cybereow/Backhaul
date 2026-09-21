package transport

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/xtaci/smux"
)

// newRotateTestTransport builds the minimum retireSession touches: ctx, logger,
// sessionCounter and reqNewConnChan.
func newRotateTestTransport() *WsMuxTransport {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	return &WsMuxTransport{
		config:         &WsMuxConfig{MuxCon: 8, MaxConnAge: time.Minute},
		ctx:            context.Background(),
		logger:         logger,
		reqNewConnChan: make(chan struct{}, 4),
	}
}

// A connection being retired at max_conn_age must stay up while a flow is still
// running on it, and must ask for its replacement up front. That is the whole
// point of draining instead of closing: smux cannot move a live stream to
// another connection, so closing on schedule would cut long-lived flows (an SSH
// session, a large download) exactly as a CDN reset does.
func TestRetireSessionDrainsBeforeClosing(t *testing.T) {
	srvConn, cliConn := net.Pipe()
	defer srvConn.Close()
	defer cliConn.Close()

	// Mirror the real pool: the server opens streams, the client accepts them.
	session, err := smux.Client(srvConn, smux.DefaultConfig())
	if err != nil {
		t.Fatalf("smux.Client: %v", err)
	}
	peer, err := smux.Server(cliConn, smux.DefaultConfig())
	if err != nil {
		t.Fatalf("smux.Server: %v", err)
	}
	defer peer.Close()
	go func() {
		for {
			st, err := peer.AcceptStream()
			if err != nil {
				return
			}
			go io.Copy(io.Discard, st)
		}
	}()

	stream, err := session.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}

	s := newRotateTestTransport()
	done := make(chan struct{})
	go func() {
		s.retireSession(session)
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("retireSession gave up on a session that still had a live stream")
	case <-time.After(2100 * time.Millisecond): // a couple of drain ticks
	}

	if session.IsClosed() {
		t.Fatal("session was closed while a stream was still live")
	}

	// Once the flow ends, the connection is free to go.
	stream.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("retireSession did not return after the last stream closed")
	}
}

// Rotation must not wedge on a connection the peer already tore down.
func TestRetireSessionReturnsOnClosedSession(t *testing.T) {
	srvConn, cliConn := net.Pipe()
	defer cliConn.Close()

	session, err := smux.Client(srvConn, smux.DefaultConfig())
	if err != nil {
		t.Fatalf("smux.Client: %v", err)
	}
	session.Close()

	s := newRotateTestTransport()
	done := make(chan struct{})
	go func() {
		s.retireSession(session)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("retireSession did not return for an already-closed session")
	}
}

// rotateTimeout bounds every wait for something that must happen. It is never
// used to let something else happen first; it only elapses when rotation is
// wedged, which is the failure these tests exist to catch.
const rotateTimeout = 5 * time.Second

// rotateSettle is how long a "must NOT happen" check watches. The fixture polls
// every rotateTestPoll, so this spans dozens of rotation decisions.
const (
	rotateTestPoll = 5 * time.Millisecond
	rotateSettle   = 200 * time.Millisecond
)

// newRotateFixture is a transport plus a generation whose context the rotation
// waits on, polling the registry fast so tests need not wait seconds.
func newRotateFixture(t *testing.T) (*WsMuxTransport, *wsGeneration) {
	t.Helper()
	s := newRotateTestTransport()
	g := newWsGeneration(context.Background())
	s.gen, s.ctx, s.cancel = g, g.ctx, g.cancel
	s.rotatePoll = rotateTestPoll
	t.Cleanup(g.stop)
	return s, g
}

// newRotateSession is a real smux pool session (server end, as in production)
// whose peer accepts and discards every stream, so streams can really be open.
// The session is not registered.
func newRotateSession(t *testing.T) *smux.Session {
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
	go func() {
		for {
			st, err := peer.AcceptStream()
			if err != nil {
				return
			}
			go io.Copy(io.Discard, st)
		}
	}()
	t.Cleanup(func() {
		session.Close()
		peer.Close()
		srvConn.Close()
		cliConn.Close()
	})
	return session
}

// addRotateSession admits a real live session the way handleLoop does: it is
// registered, so it is eligible for legs and counts as pool capacity.
func addRotateSession(t *testing.T, s *WsMuxTransport) *smux.Session {
	t.Helper()
	session := newRotateSession(t)
	s.registerSession(session, false)
	return session
}

// eligibleLive counts the registered sessions that are still open: the live
// capacity a flow could be placed on.
func eligibleLive(s *WsMuxTransport) int {
	s.sessionsMu.Lock()
	defer s.sessionsMu.Unlock()
	n := 0
	for _, ps := range s.sessions {
		if !ps.session.IsClosed() {
			n++
		}
	}
	return n
}

func isEligible(s *WsMuxTransport, session *smux.Session) bool {
	_, ok := s.snapshotSessions()[session]
	return ok
}

// awaitAsync runs one rotation decision and reports its result.
func awaitAsync(s *WsMuxTransport, g *wsGeneration, session *smux.Session) <-chan bool {
	got := make(chan bool, 1)
	go func() { got <- s.awaitReplacement(g, session) }()
	return got
}

// waitRequest consumes one replacement request. Only the rotation holding the
// decision gate asks, so seeing a request also says a decision has begun (and
// has already taken its snapshot).
func waitRequest(t *testing.T, s *WsMuxTransport) {
	t.Helper()
	select {
	case <-s.reqNewConnChan:
	case <-time.After(rotateTimeout):
		t.Fatal("no replacement was requested")
	}
}

func assertNoResult(t *testing.T, what string, ch <-chan bool) {
	t.Helper()
	select {
	case ok := <-ch:
		t.Fatalf("%s (returned %v)", what, ok)
	case <-time.After(rotateSettle):
	}
}

// Rotation must not retire a connection until its replacement is actually up.
// Nothing retries a failed pool dial (the client's tunnelDialer abandons one for
// good), so retiring on a timer alone would shrink the pool every time the CDN
// or edge IP was unreachable - exactly when capacity matters most. A bumped
// admission counter is not a replacement: only a live registered session is.
func TestAwaitReplacementWaitsForNewConnection(t *testing.T) {
	s, g := newRotateFixture(t)
	old := addRotateSession(t, s)
	got := awaitAsync(s, g, old)

	// It orders the replacement up front.
	waitRequest(t, s)

	// No connection admitted: it keeps waiting rather than freeing the old one.
	assertNoResult(t, "awaitReplacement returned while the pool had no replacement", got)

	// The admission counter moving is not a successor.
	atomic.AddInt32(&s.admittedSessions, 1)
	assertNoResult(t, "awaitReplacement accepted a bumped counter as a replacement", got)
	if !isEligible(s, old) {
		t.Fatal("the old session left eligibility without a replacement")
	}

	// A connection joins the pool, as handleLoop would record it.
	addRotateSession(t, s)
	select {
	case ok := <-got:
		if !ok {
			t.Fatal("awaitReplacement reported failure after a replacement was admitted")
		}
	case <-time.After(rotateTimeout):
		t.Fatal("awaitReplacement did not notice the replacement connection")
	}
	if isEligible(s, old) {
		t.Fatal("the old session is still eligible after its replacement authorized retirement")
	}
}

// A session that dies while waiting has nothing left to rotate.
func TestAwaitReplacementAbandonsDeadSession(t *testing.T) {
	s, g := newRotateFixture(t)
	session := addRotateSession(t, s)
	got := awaitAsync(s, g, session)

	waitRequest(t, s)
	session.Close()

	select {
	case ok := <-got:
		if ok {
			t.Fatal("awaitReplacement reported success for a dead session")
		}
	case <-time.After(rotateTimeout):
		t.Fatal("awaitReplacement did not return for a dead session")
	}
	if len(g.rotate) != 0 {
		t.Fatal("the decision gate is still held after the rotation gave up")
	}
}

// One admission authorizes at most one retirement. Two old sessions rotate
// together; the first successor lets exactly one of them go, and the other stays
// eligible - serving - until it has a successor of its own.
func TestRotateDistinctReplacement(t *testing.T) {
	s, g := newRotateFixture(t)
	a, b := addRotateSession(t, s), addRotateSession(t, s)
	ra, rb := awaitAsync(s, g, a), awaitAsync(s, g, b)

	waitRequest(t, s) // whichever took the gate
	assertNoResult(t, "a rotation finished without any successor", ra)
	assertNoResult(t, "a rotation finished without any successor", rb)
	if n := eligibleLive(s); n != 2 {
		t.Fatalf("live eligible capacity before any successor = %d, want 2", n)
	}

	// First successor: exactly one old session leaves.
	s1 := addRotateSession(t, s)
	var winner, loser *smux.Session
	var rLoser <-chan bool
	select {
	case ok := <-ra:
		if !ok {
			t.Fatal("rotation of the first old session failed")
		}
		winner, loser, rLoser = a, b, rb
	case ok := <-rb:
		if !ok {
			t.Fatal("rotation of the second old session failed")
		}
		winner, loser, rLoser = b, a, ra
	case <-time.After(rotateTimeout):
		t.Fatal("no rotation completed after a live successor was admitted")
	}
	// The loser now holds the gate: it snapshots (s1 included) and asks for its own.
	waitRequest(t, s)
	assertNoResult(t, "the second rotation reused the first rotation's successor", rLoser)
	if isEligible(s, winner) {
		t.Fatal("the retired session is still eligible")
	}
	if !isEligible(s, loser) || !isEligible(s, s1) {
		t.Fatal("the unreplaced old session or the successor left eligibility")
	}
	if n := eligibleLive(s); n != 2 {
		t.Fatalf("live eligible capacity after one successor = %d, want 2 (one old + successor)", n)
	}

	// Second successor: now the other one can retire.
	s2 := addRotateSession(t, s)
	select {
	case ok := <-rLoser:
		if !ok {
			t.Fatal("second rotation failed after its own successor was admitted")
		}
	case <-time.After(rotateTimeout):
		t.Fatal("second rotation did not complete after its own successor")
	}
	if isEligible(s, loser) || !isEligible(s, s1) || !isEligible(s, s2) {
		t.Fatal("eligibility after both retirements is not exactly the two successors")
	}
	if n := eligibleLive(s); n != 2 {
		t.Fatalf("live eligible capacity after both rotations = %d, want 2", n)
	}
	if len(g.rotate) != 0 {
		t.Fatal("the decision gate is still held after both rotations")
	}
}

// The same guarantee through the real rotation entry point: two aging sessions,
// each on its own timer, one successor.
func TestRotateStripedTwoOldOneSuccessor(t *testing.T) {
	s, g := newRotateFixture(t)
	s.config.MaxConnAge = 20 * time.Millisecond
	a, b := addRotateSession(t, s), addRotateSession(t, s)
	g.start(func() { s.rotateStripedSession(g, a) })
	g.start(func() { s.rotateStripedSession(g, b) })

	waitRequest(t, s)
	addRotateSession(t, s)

	var loser *smux.Session
	select {
	case <-a.CloseChan():
		loser = b
	case <-b.CloseChan():
		loser = a
	case <-time.After(rotateTimeout):
		t.Fatal("no old session rotated out after a successor was admitted")
	}
	waitRequest(t, s)
	select {
	case <-loser.CloseChan():
		t.Fatal("both old sessions retired against a single successor")
	case <-time.After(rotateSettle):
	}
	if n := eligibleLive(s); n != 2 {
		t.Fatalf("live eligible capacity = %d, want 2 (one old + successor)", n)
	}

	addRotateSession(t, s)
	select {
	case <-loser.CloseChan():
	case <-time.After(rotateTimeout):
		t.Fatal("the second old session did not rotate out after its own successor")
	}
	if n := eligibleLive(s); n != 2 {
		t.Fatalf("live eligible capacity = %d, want 2 successors", n)
	}
}

// A successor that is dead, or no longer eligible, cannot authorize retirement,
// and neither can one that predates the decision's snapshot.
func TestRotateDeadReplacement(t *testing.T) {
	s, _ := newRotateFixture(t)
	old := addRotateSession(t, s)
	before := addRotateSession(t, s)
	seen := s.snapshotSessions()

	if s.retireIfReplaced(old, seen) {
		t.Fatal("a session that predates the snapshot authorized retirement")
	}

	dead := addRotateSession(t, s)
	dead.Close()
	if s.retireIfReplaced(old, seen) {
		t.Fatal("a dead successor authorized retirement")
	}

	gone := addRotateSession(t, s)
	s.unregisterSession(gone)
	if s.retireIfReplaced(old, seen) {
		t.Fatal("an unregistered successor authorized retirement")
	}
	if !isEligible(s, old) {
		t.Fatal("the old session left eligibility without a live successor")
	}

	addRotateSession(t, s)
	if !s.retireIfReplaced(old, seen) {
		t.Fatal("a live new successor did not authorize retirement")
	}
	if isEligible(s, old) || !isEligible(s, before) {
		t.Fatal("retirement did not remove exactly the old session")
	}
}

// Through awaitReplacement: a successor that is already dead when the rotation
// looks is not one, and the old session keeps serving until a live one arrives.
func TestRotateDeadReplacementKeepsWaiting(t *testing.T) {
	s, g := newRotateFixture(t)
	old := addRotateSession(t, s)
	got := awaitAsync(s, g, old)
	waitRequest(t, s)

	dead := newRotateSession(t)
	dead.Close()
	s.registerSession(dead, false)
	assertNoResult(t, "a dead successor authorized retirement", got)
	if !isEligible(s, old) {
		t.Fatal("the old session left eligibility behind a dead successor")
	}

	addRotateSession(t, s)
	select {
	case ok := <-got:
		if !ok {
			t.Fatal("rotation failed after a live successor arrived")
		}
	case <-time.After(rotateTimeout):
		t.Fatal("rotation did not complete after a live successor arrived")
	}
}

// Every way out of a rotation releases the gate: a queued waiter whose session
// dies leaves without disturbing the holder, and the generation ending wakes the
// holder and every waiter.
func TestRotateCanceledWaiter(t *testing.T) {
	s, g := newRotateFixture(t)
	a, b, c := addRotateSession(t, s), addRotateSession(t, s), addRotateSession(t, s)

	ra := awaitAsync(s, g, a)
	waitRequest(t, s) // a holds the gate
	rb, rc := awaitAsync(s, g, b), awaitAsync(s, g, c)

	// b dies while queued behind a: it leaves, a keeps the gate and keeps waiting.
	b.Close()
	select {
	case ok := <-rb:
		if ok {
			t.Fatal("a queued rotation reported success for a dead session")
		}
	case <-time.After(rotateTimeout):
		t.Fatal("a queued rotation did not leave when its session died")
	}
	if len(g.rotate) != 1 {
		t.Fatal("a queued waiter leaving disturbed the holder's gate")
	}
	assertNoResult(t, "the holder gave up when a queued waiter left", ra)

	// The generation ends: holder and remaining waiter both wake, gate is free.
	g.stop()
	for name, ch := range map[string]<-chan bool{"holder": ra, "waiter": rc} {
		select {
		case ok := <-ch:
			if ok {
				t.Fatalf("the %s reported success after the generation ended", name)
			}
		case <-time.After(rotateTimeout):
			t.Fatalf("the %s was not woken by the generation ending", name)
		}
	}
	if len(g.rotate) != 0 {
		t.Fatal("the decision gate is still held after the generation ended")
	}
	if !isEligible(s, a) || !isEligible(s, c) {
		t.Fatal("a canceled rotation took its session out of eligibility")
	}
}

// Draining an old session must not hold the decision gate: the next rotation
// must be able to start its own decision while the first is still waiting for its
// streams to end, and the draining session must stay open until they do.
func TestRotateDrainDoesNotHoldGate(t *testing.T) {
	s, g := newRotateFixture(t)
	a, b := addRotateSession(t, s), addRotateSession(t, s)
	stream, err := a.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}

	ra := awaitAsync(s, g, a)
	waitRequest(t, s)
	addRotateSession(t, s)
	select {
	case ok := <-ra:
		if !ok {
			t.Fatal("rotation failed after a successor was admitted")
		}
	case <-time.After(rotateTimeout):
		t.Fatal("rotation did not complete")
	}

	drained := make(chan struct{})
	go func() {
		s.retireSession(a)
		close(drained)
	}()

	// b's decision starts (it asks for its own successor) while a drains.
	rb := awaitAsync(s, g, b)
	waitRequest(t, s)
	select {
	case <-drained:
		t.Fatal("the session was retired while a stream was still live")
	case <-time.After(rotateSettle): // long enough for retireSession to be draining
	}
	if a.IsClosed() {
		t.Fatal("the draining session was closed under its live stream")
	}
	if isEligible(s, a) {
		t.Fatal("the draining session is still eligible for new legs")
	}

	stream.Close()
	select {
	case <-drained:
	case <-time.After(rotateTimeout):
		t.Fatal("the session did not finish draining after its last stream closed")
	}

	addRotateSession(t, s)
	select {
	case ok := <-rb:
		if !ok {
			t.Fatal("second rotation failed after its own successor was admitted")
		}
	case <-time.After(rotateTimeout):
		t.Fatal("second rotation did not complete")
	}
}
