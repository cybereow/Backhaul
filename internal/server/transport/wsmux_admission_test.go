package transport

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/musix/backhaul/config"
	"github.com/sirupsen/logrus"
	"github.com/xtaci/smux"
)

// setupRig is a server transport with only its dispatch loop running: no
// listeners and no control channel, so the tests inject local connections
// straight into localChannel (with whatever accepted-time they need) and nothing
// drains reqNewConnChan, which makes the growth requests countable.
type setupRig struct {
	t      *testing.T
	s      *WsMuxTransport
	g      *wsGeneration
	cancel context.CancelFunc
}

func newSetupRig(t *testing.T, opts ...func(*WsMuxConfig)) *setupRig {
	t.Helper()
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	ctx, cancel := context.WithCancel(context.Background())

	cfg := &WsMuxConfig{
		Mode:             config.WSMUX,
		Path:             "/",
		MuxVersion:       2,
		MuxCon:           8,
		ChannelSize:      100,
		MaxFrameSize:     32768,
		MaxReceiveBuffer: 4194304,
		MaxStreamBuffer:  65536,
		KeepAlive:        30 * time.Second,
		Heartbeat:        30 * time.Second,
		Nodelay:          true,
	}
	for _, o := range opts {
		o(cfg)
	}
	s := NewWSMuxServer(ctx, cfg, logger)
	g := s.gen
	r := &setupRig{t: t, s: s, g: g, cancel: cancel}
	if !g.start(func() { s.dispatchLoop(g) }) {
		t.Fatal("generation refused the dispatch loop")
	}
	t.Cleanup(func() {
		cancel()
		if !g.join(lcDeadline) {
			t.Error("workers of the rig's generation did not end")
		}
	})
	return r
}

// tcpPair returns a connected loopback pair; the first is a *net.TCPConn, as the
// dispatcher requires of an accepted local connection.
func tcpPair(t *testing.T) (accepted, user net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	type res struct {
		c   net.Conn
		err error
	}
	ch := make(chan res, 1)
	go func() {
		c, err := ln.Accept()
		ch <- res{c, err}
	}()
	user, err = net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	r := <-ch
	if r.err != nil {
		t.Fatal(r.err)
	}
	t.Cleanup(func() { r.c.Close(); user.Close() })
	return r.c, user
}

// inject queues one local connection as if the listener had accepted it at
// created, and returns the user's end of it.
func (r *setupRig) inject(created time.Time) net.Conn {
	r.t.Helper()
	accepted, user := tcpPair(r.t)
	atomic.AddInt32(&r.s.streamCounter, 1)
	r.s.localChannel <- LocalTCPConn{conn: accepted, remoteAddr: "1", timeCreated: acceptStamp(created)}
	return user
}

// aged is an accepted-time d in the past: the connection has setupTimeout-d left.
func aged(d time.Duration) time.Time { return time.Now().Add(-d) }

// expectUserClosed waits for the server to close its end of a user connection.
func expectUserClosed(t *testing.T, what string, user net.Conn) time.Time {
	t.Helper()
	_ = user.SetReadDeadline(time.Now().Add(lcDeadline))
	_, err := user.Read(make([]byte, 1))
	if err == nil {
		t.Fatalf("%s: unexpectedly received data", what)
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatalf("%s: still open after %s", what, lcDeadline)
	}
	return time.Now()
}

func (r *setupRig) queued() int { return len(r.s.localChannel) }

func (r *setupRig) growthRequests() int { return len(r.s.reqNewConnChan) }

// settled waits until every counter the setup path owns is back at zero: nothing
// leaked, nothing went negative (a negative value never equals zero).
func (r *setupRig) settled(what string) {
	r.t.Helper()
	lcWaitFor(r.t, what+": counters to settle", func() bool {
		return atomic.LoadInt32(&r.s.streamCounter) == 0 &&
			atomic.LoadInt32(&r.s.plainFlows) == 0 &&
			atomic.LoadInt32(&r.s.stripedFlows) == 0 &&
			atomic.LoadInt32(&r.s.setupsActive) == 0
	})
}

// holdWindow is a bounded observation window for rate and absence checks. It is
// not used to wait for anything to happen.
func holdWindow(d time.Duration) { <-time.After(d) }

// gateWriteConn blocks every Write until gate is closed, which stalls a smux
// session's send loop and with it any OpenStream waiting on that session.
type gateWriteConn struct {
	net.Conn
	gate    chan struct{}
	entered chan struct{}
	once    sync.Once
}

func (c *gateWriteConn) Write(p []byte) (int, error) {
	c.once.Do(func() { close(c.entered) })
	<-c.gate
	return c.Conn.Write(p)
}

// testPool is one registered pool session whose far end is a real smux server.
type testPool struct {
	ps       *pooledSession
	peer     *smux.Session
	accepted chan *smux.Stream
	gate     *gateWriteConn // nil unless the pool was built stalled
	open     sync.Once
}

func (p *testPool) release() {
	if p.gate != nil {
		p.open.Do(func() { close(p.gate.gate) })
	}
}

// addPool registers a live pool session and counts it in the budget, as
// handleLoop would. stalled makes its writes block until release.
func (r *setupRig) addPool(stalled bool) *testPool {
	r.t.Helper()
	srvConn, cliConn := net.Pipe()
	var wire net.Conn = srvConn
	p := &testPool{accepted: make(chan *smux.Stream, 16)}
	if stalled {
		p.gate = &gateWriteConn{Conn: srvConn, gate: make(chan struct{}), entered: make(chan struct{})}
		wire = p.gate
	}
	cfg := smux.DefaultConfig()
	cfg.KeepAliveDisabled = true
	session, err := smux.Client(wire, cfg)
	if err != nil {
		r.t.Fatal(err)
	}
	p.peer, err = smux.Server(cliConn, cfg)
	if err != nil {
		r.t.Fatal(err)
	}
	go func() {
		for {
			st, err := p.peer.AcceptStream()
			if err != nil {
				return
			}
			p.accepted <- st
		}
	}()
	p.ps = r.s.registerSession(session, false)
	atomic.AddInt32(&r.s.sessionCounter, 1)
	r.t.Cleanup(func() {
		p.release()
		session.Close()
		p.peer.Close()
		srvConn.Close()
		cliConn.Close()
	})
	return p
}

// deadPool registers a pool session that is already closed.
func (r *setupRig) deadPool() {
	r.t.Helper()
	p := r.addPool(false)
	p.ps.session.Close()
}

// TestWSMuxAdmissionDeadline: a connection whose setup keeps failing is closed
// at its original accepted time plus the setup budget. A retry never refreshes
// the accepted time (it used to, so a connection could be retried forever).
func TestWSMuxAdmissionDeadline(t *testing.T) {
	cases := []struct {
		name  string
		opts  []func(*WsMuxConfig)
		setup func(*setupRig)
	}{
		{"plain no pool session", nil, func(r *setupRig) {}},
		{"plain closed session", nil, func(r *setupRig) { r.deadPool() }},
		{"striped width deficit", []func(*WsMuxConfig){func(c *WsMuxConfig) { c.StripeFactor = 2 }}, func(r *setupRig) { r.addPool(false) }},
		{"striped failed opens", []func(*WsMuxConfig){func(c *WsMuxConfig) { c.StripeFactor = 2 }}, func(r *setupRig) { r.deadPool(); r.deadPool() }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newSetupRig(t, tc.opts...)
			tc.setup(r)

			const left = time.Second // budget remaining when the connection is queued
			start := time.Now()
			user := r.inject(aged(setupTimeout - left))
			closed := expectUserClosed(t, "the failing setup", user)

			// Several 100ms retries fit in the remaining budget; none may extend it
			// (upper bound: scheduling slack) and nothing may cut it short: timers
			// never fire early, so the only allowance below is the accepted stamp's
			// millisecond truncation.
			if el := closed.Sub(start); el < left-25*time.Millisecond || el > left+1500*time.Millisecond {
				t.Fatalf("closed after %s, want the %s left of the original budget", el, left)
			}
			r.settled("after the deadline")
		})
	}
}

// TestWSMuxAdmissionGrowth: waiters that find the admission budget full ask for
// pool growth once between them - not once per waiter per 10ms poll - and ask
// again only after the previous request was answered or has plainly failed.
func TestWSMuxAdmissionGrowth(t *testing.T) {
	r := newSetupRig(t, func(c *WsMuxConfig) { c.MuxCon = 1; c.ChannelSize = 1000 })
	r.addPool(false)
	atomic.StoreInt32(&r.s.plainFlows, 1) // the one-flow budget is used up

	const waiters = 20
	for i := 0; i < waiters; i++ {
		r.inject(time.Now())
	}
	lcWaitFor(t, "every waiter to leave the queue", func() bool { return r.queued() == 0 })
	lcWaitFor(t, "the first growth request", func() bool { return r.growthRequests() >= 1 })

	holdWindow(400 * time.Millisecond) // observation window: 20 waiters x 40 polls before
	first := r.growthRequests()
	if first > 2 {
		t.Fatalf("%d growth requests from %d waiters in ~400ms, want them coalesced", first, waiters)
	}

	// A session was admitted since the last request: the budget may still be
	// short, so exactly one more request is allowed.
	atomic.AddInt32(&r.s.admittedSessions, 1)
	lcWaitFor(t, "a request after a session was admitted", func() bool { return r.growthRequests() > first })
	holdWindow(200 * time.Millisecond)
	if got := r.growthRequests(); got > first+1 {
		t.Fatalf("%d growth requests after one admission, want %d", got, first+1)
	}
}

// TestWSMuxAdmissionGrowthReask: with no session ever admitted (the dial
// failed), waiting flows re-ask after growthReaskGap instead of never.
func TestWSMuxAdmissionGrowthReask(t *testing.T) {
	r := newSetupRig(t, func(c *WsMuxConfig) { c.MuxCon = 1; c.ChannelSize = 1000 })
	r.s.growthGap = 30 * time.Millisecond // set before any setup worker exists
	r.addPool(false)
	atomic.StoreInt32(&r.s.plainFlows, 1)

	r.inject(time.Now())
	lcWaitFor(t, "repeated growth requests", func() bool { return r.growthRequests() >= 3 })
}

// TestWSMuxAdmissionWidthDeficit: a striped flow that needs more distinct pool
// sessions than exist asks for capacity even though the logical flow budget is
// not full - and asks once, not once per missing leg per retry.
func TestWSMuxAdmissionWidthDeficit(t *testing.T) {
	r := newSetupRig(t, func(c *WsMuxConfig) { c.StripeFactor = 3; c.ChannelSize = 1000 })
	r.addPool(false) // one session for a three-leg group; the budget (1x8) is not full

	r.inject(time.Now())
	lcWaitFor(t, "a growth request for the missing width", func() bool { return r.growthRequests() >= 1 })

	holdWindow(400 * time.Millisecond) // ~4 retry cycles
	if got := r.growthRequests(); got > 2 {
		t.Fatalf("%d growth requests for one width-deficient flow, want them coalesced", got)
	}
}

// TestWSMuxAdmissionSetupCeiling: setup workers are bounded by the permit pool
// (ChannelSize). Connections beyond it wait in localChannel - the one queue -
// rather than each getting a goroutine, and start once a permit frees up.
func TestWSMuxAdmissionSetupCeiling(t *testing.T) {
	const ceiling, extra = 8, 6
	r := newSetupRig(t, func(c *WsMuxConfig) { c.ChannelSize = ceiling })
	if got := cap(r.s.setupSlots); got != ceiling {
		t.Fatalf("setup permits = %d, want ChannelSize %d", got, ceiling)
	}

	// No pool: every setup keeps retrying until its budget (1s left) runs out.
	firstUsers := make([]net.Conn, ceiling)
	for i := range firstUsers {
		firstUsers[i] = r.inject(aged(setupTimeout - time.Second))
	}
	lcWaitFor(t, "the first batch to be taken into setup", func() bool { return r.queued() == 0 })

	lastUsers := make([]net.Conn, extra)
	for i := range lastUsers {
		lastUsers[i] = r.inject(time.Now())
	}
	// Bounded observation: while the permits are all held, the extra connections
	// must stay queued. (Without a ceiling they are dequeued at once.)
	for end := time.Now().Add(300 * time.Millisecond); time.Now().Before(end); time.Sleep(5 * time.Millisecond) {
		if got := r.queued(); got != extra {
			t.Fatalf("%d connection(s) queued while all %d permits are held, want %d", got, ceiling, extra)
		}
	}

	// The first batch expires, freeing permits; the queued ones are then taken.
	for _, u := range firstUsers {
		expectUserClosed(t, "an expired setup", u)
	}
	lcWaitFor(t, "the queued connections to be taken into setup", func() bool { return r.queued() == 0 })
	// The old batch's workers settle a moment after their sockets close, and the
	// new batch's workers start a moment after their dequeue: wait for both.
	lcWaitFor(t, "exactly the new batch to be in setup", func() bool {
		return atomic.LoadInt32(&r.s.setupsActive) == extra && atomic.LoadInt32(&r.s.streamCounter) == extra
	})
	for _, u := range lastUsers {
		_ = u.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		if _, err := u.Read(make([]byte, 1)); err == nil {
			t.Fatal("unexpected data on a waiting connection")
		} else if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
			t.Fatalf("a connection with budget left was closed: %v", err)
		}
	}
}

// TestWSMuxAdmissionActiveHoldsNoPermit: a running transfer must not hold a setup
// permit. With only two permits, more concurrent long-lived flows than that all
// still start, and every permit is back once they are up.
func TestWSMuxAdmissionActiveHoldsNoPermit(t *testing.T) {
	h := newLCHarness(t, func(c *WsMuxConfig) { c.ChannelSize = 2 })
	h.control()
	h.pool()
	lcWaitFor(t, "an admitted session", func() bool { return h.sessions() == 1 })

	const flows = 5
	users := make([]net.Conn, flows)
	for i := range users {
		users[i] = h.user()
		lcEcho(t, users[i], "hello")
	}
	lcWaitFor(t, "every setup permit to be back", func() bool { return atomic.LoadInt32(&h.s.setupsActive) == 0 })
	for _, u := range users { // and they are still live, holding no permit
		lcEcho(t, u, "again")
	}
	if got := atomic.LoadInt32(&h.s.plainFlows); got != flows {
		t.Fatalf("plainFlows = %d, want %d active flows", got, flows)
	}
}

// TestWSMuxAdmissionStalledOpen: smux OpenStream cannot be cancelled. When the
// budget expires while it is stuck, the local socket is closed on time but the
// worker keeps its permit and counters until the open returns; the stream that
// finally arrives is closed unused, the counters settle exactly once, and the
// session (with its other streams) is untouched.
func TestWSMuxAdmissionStalledOpen(t *testing.T) {
	r := newSetupRig(t)
	pool := r.addPool(true)

	user := r.inject(aged(setupTimeout - time.Second))
	lcWaitClosed(t, "OpenStream to be in flight", pool.gate.entered)
	expectUserClosed(t, "the expired setup's local socket", user)

	// The worker still owns the attempt: the open has not returned.
	if got := atomic.LoadInt32(&r.s.streamCounter); got != 1 {
		t.Fatalf("streamCounter = %d while the open is still blocked, want 1", got)
	}
	if got := atomic.LoadInt32(&r.s.setupsActive); got != 1 {
		t.Fatalf("setups in flight = %d while the open is still blocked, want 1", got)
	}
	if got := atomic.LoadInt32(&r.s.plainFlows); got != 1 {
		t.Fatalf("plainFlows = %d while the open is still blocked, want 1", got)
	}

	pool.release() // the open now completes, too late
	r.settled("after the late open")

	// The late stream was closed, never used: the peer sees it end without data.
	var late *smux.Stream
	select {
	case late = <-pool.accepted:
	case <-time.After(lcDeadline):
		t.Fatal("the late stream never reached the peer")
	}
	_ = late.SetReadDeadline(time.Now().Add(lcDeadline))
	if n, err := late.Read(make([]byte, 64)); err != io.EOF {
		t.Fatalf("late stream: read %d bytes, err %v; want a clean close with no data", n, err)
	}
	lcWaitFor(t, "the late stream to be gone from the session", func() bool { return pool.ps.session.NumStreams() == 0 })

	// The session was not closed to cancel that setup; another stream works on it.
	if pool.ps.session.IsClosed() {
		t.Fatal("the pool session was closed")
	}
	st, err := pool.ps.session.OpenStream()
	if err != nil {
		t.Fatalf("session unusable after the late open: %v", err)
	}
	defer st.Close()
	if _, err := st.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	select {
	case other := <-pool.accepted:
		buf := make([]byte, 1)
		_ = other.SetReadDeadline(time.Now().Add(lcDeadline))
		if _, err := io.ReadFull(other, buf); err != nil || buf[0] != 'x' {
			t.Fatalf("other stream: %q, %v", buf, err)
		}
	case <-time.After(lcDeadline):
		t.Fatal("the other stream never reached the peer")
	}
}

// TestWSMuxAdmissionCancel: stopping the generation closes a setup's local socket
// at once and the worker ends, whether it was waiting or retrying.
func TestWSMuxAdmissionCancel(t *testing.T) {
	r := newSetupRig(t)
	user := r.inject(time.Now()) // full budget left: only the cancel can end it
	lcWaitFor(t, "the connection to be taken into setup", func() bool { return r.queued() == 0 })

	r.cancel()
	expectUserClosed(t, "the cancelled setup", user)
	if !r.g.join(lcDeadline) {
		t.Fatal("the setup worker outlived its generation")
	}
	if got := atomic.LoadInt32(&r.s.streamCounter); got != 0 {
		t.Fatalf("streamCounter = %d after the cancel, want 0", got)
	}
}

// holdCloseConn blocks Close until released, to freeze the expiry callback in the
// middle of closing the local socket.
type holdCloseConn struct {
	net.Conn
	entered chan struct{}
	release chan struct{}
	closes  atomic.Int32
	once    sync.Once
}

func (c *holdCloseConn) Close() error {
	c.closes.Add(1)
	c.once.Do(func() { close(c.entered) })
	<-c.release
	return c.Conn.Close()
}

func bareAttempt(t *testing.T, created time.Time, conn net.Conn) *setupAttempt {
	t.Helper()
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	s := &WsMuxTransport{logger: logger}
	g := newWsGeneration(context.Background())
	t.Cleanup(g.stop)
	slots := newSetupSlots(1)
	slots <- struct{}{} // the permit this attempt was started with
	return s.newSetupAttempt(g, slots, LocalTCPConn{conn: conn, timeCreated: acceptStamp(created)})
}

// TestWSMuxAdmissionHandoffJoinsExpiry: the expiry callback is joined before the
// handoff decides. If it is already closing the socket, handoff waits for it and
// reports the loss - the flow is never started on a half-closed socket.
func TestWSMuxAdmissionHandoffJoinsExpiry(t *testing.T) {
	a1, _ := net.Pipe()
	conn := &holdCloseConn{Conn: a1, entered: make(chan struct{}), release: make(chan struct{})}
	a := bareAttempt(t, aged(setupTimeout-50*time.Millisecond), conn)

	lcWaitClosed(t, "the expiry callback to start closing", conn.entered)
	result := make(chan bool, 1)
	go func() { result <- a.handoff() }()

	select {
	case <-result:
		t.Fatal("handoff returned while the expiry callback was still closing the socket")
	case <-time.After(100 * time.Millisecond): // absence check only; progress is signalled below
	}
	close(conn.release)
	select {
	case ok := <-result:
		if ok {
			t.Fatal("handoff succeeded on a socket the expiry already closed")
		}
	case <-time.After(lcDeadline):
		t.Fatal("handoff never returned after the callback finished")
	}
}

// TestWSMuxAdmissionHandoffStopsExpiry: once handoff succeeds the expiry callback
// is disarmed, so it can never close a flow that is already running.
func TestWSMuxAdmissionHandoffStopsExpiry(t *testing.T) {
	a1, a2 := net.Pipe()
	defer a2.Close()
	conn := &holdCloseConn{Conn: a1, entered: make(chan struct{}), release: make(chan struct{})}
	close(conn.release)
	const left = 150 * time.Millisecond
	a := bareAttempt(t, aged(setupTimeout-left), conn)
	expiry := a.expiry

	if !a.handoff() {
		t.Fatal("handoff failed before the expiry")
	}
	<-time.After(time.Until(expiry) + 100*time.Millisecond) // let the (disarmed) deadline pass
	if n := conn.closes.Load(); n != 0 {
		t.Fatalf("the expiry callback closed a handed-off connection (%d closes)", n)
	}
	if a.fired.Load() {
		t.Fatal("the expiry callback fired after a successful handoff")
	}
	a.releasePermit()
	a.releasePermit() // exactly once, however often it is called
	if got := len(a.slots); got != 0 {
		t.Fatalf("permit pool holds %d after release, want 0", got)
	}
}

// TestWSMuxAdmissionExpiryMonotonic: the accepted stamp is a wall-clock time, but
// the deadline built from it must be on the monotonic clock, like the timers and
// waits that enforce it. A deadline compared against the wall clock moves with
// every wall-clock step (NTP, a VM resume): connections were closed a second or
// more before their budget ended on a host whose clock stepped mid-run.
func TestWSMuxAdmissionExpiryMonotonic(t *testing.T) {
	a1, a2 := net.Pipe()
	defer a2.Close()
	a := bareAttempt(t, aged(setupTimeout-time.Second), a1)
	if !strings.Contains(a.expiry.String(), "m=") {
		t.Fatalf("expiry %q carries no monotonic reading: it would follow the wall clock", a.expiry)
	}
	if left := time.Until(a.expiry); left < 900*time.Millisecond || left > time.Second {
		t.Fatalf("expiry is %s away, want the ~1s left of the original budget", left)
	}
	// The stamp's age is measured on the monotonic clock as well: it is exactly
	// the elapsed time, whatever the wall clock did meanwhile.
	if age := stampAge(acceptStamp(aged(2 * time.Second))); age < 1990*time.Millisecond || age > 2100*time.Millisecond {
		t.Fatalf("stamp age = %s, want the 2s it was aged by", age)
	}
	a.settle()
	a.releasePermit()
}
