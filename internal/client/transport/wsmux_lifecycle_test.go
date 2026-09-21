package transport

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/musix/backhaul/config"
	"github.com/musix/backhaul/internal/utils"
	"github.com/sirupsen/logrus"
	"github.com/xtaci/smux"
)

const (
	lifecycleToken    = "lifecycle-test-token"
	lifecycleDeadline = 5 * time.Second
)

// lifecycleServer is a loopback WebSocket upgrade server that hands every
// accepted tunnel/control socket to the test, so a test can observe from the far
// end whether the client really closed it.
type lifecycleServer struct {
	srv *httptest.Server

	mu       sync.Mutex
	tunnels  []net.Conn
	controls []net.Conn
	// tunnelHook, when set, runs for each accepted tunnel socket instead of just
	// recording it (the socket is still recorded first).
	tunnelHook func(net.Conn)
}

func newLifecycleServer(t *testing.T) *lifecycleServer {
	t.Helper()
	s := &lifecycleServer{}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+lifecycleToken {
			http.Error(w, "bad token", http.StatusUnauthorized)
			return
		}
		conn, _, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			return
		}
		s.mu.Lock()
		if strings.HasSuffix(r.URL.Path, "/channel") {
			s.controls = append(s.controls, conn)
			s.mu.Unlock()
			return
		}
		s.tunnels = append(s.tunnels, conn)
		hook := s.tunnelHook
		s.mu.Unlock()
		if hook != nil {
			hook(conn)
		}
	}))
	t.Cleanup(func() {
		s.srv.CloseClientConnections()
		s.mu.Lock()
		for _, c := range append(append([]net.Conn(nil), s.tunnels...), s.controls...) {
			c.Close()
		}
		s.mu.Unlock()
		s.srv.Close()
	})
	return s
}

func (s *lifecycleServer) addr() string { return s.srv.Listener.Addr().String() }

func (s *lifecycleServer) tunnelConns() []net.Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]net.Conn(nil), s.tunnels...)
}

func (s *lifecycleServer) controlConns() []net.Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]net.Conn(nil), s.controls...)
}

// waitConns polls until n tunnel sockets have been accepted and returns the
// first n.
func (s *lifecycleServer) waitTunnels(t *testing.T, n int) []net.Conn {
	t.Helper()
	waitFor(t, "tunnel sockets to be accepted", func() bool { return len(s.tunnelConns()) >= n })
	return s.tunnelConns()[:n]
}

// expectPeerClosed fails unless the far end of conn is closed within the
// deadline. The client never writes on these sockets, so any Read outcome other
// than a timeout means the client closed its end.
func expectPeerClosed(t *testing.T, what string, conn net.Conn) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(lifecycleDeadline))
	buf := make([]byte, 1)
	_, err := conn.Read(buf)
	if err == nil {
		t.Fatalf("%s: unexpectedly received data", what)
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatalf("%s: client socket still open %s after it should have been closed", what, lifecycleDeadline)
	}
}

func lifecycleMuxConfig(addr string, pool int) *WsMuxConfig {
	return &WsMuxConfig{
		RemoteAddr:       addr,
		Token:            lifecycleToken,
		Mode:             config.WSMUX,
		Path:             "/",
		MuxVersion:       2,
		ConnPoolSize:     pool,
		DialTimeOut:      2 * time.Second,
		KeepAlive:        30 * time.Second,
		RetryInterval:    100 * time.Millisecond,
		MaxFrameSize:     32768,
		MaxReceiveBuffer: 4194304,
		MaxStreamBuffer:  65536,
	}
}

func lifecycleLogger() *logrus.Logger {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return l
}

// TestWSMuxLifecycleIdleCancel: pool sockets that are idle (the server never
// opens a stream) must be closed, and their counters settled, when the transport
// is cancelled. Cancellation alone cannot wake smux's AcceptStream; only
// closing the socket can.
func TestWSMuxLifecycleIdleCancel(t *testing.T) {
	srv := newLifecycleServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := NewWSMuxClient(ctx, lifecycleMuxConfig(srv.addr(), 2), lifecycleLogger())
	c.Start()

	conns := srv.waitTunnels(t, 2)
	waitFor(t, "both pool connections to be counted", func() bool { return atomic.LoadInt32(&c.poolConnections) == 2 })

	cancel()

	for i, conn := range conns {
		expectPeerClosed(t, "idle pool socket "+string(rune('0'+i)), conn)
	}
	waitFor(t, "pool counter to settle", func() bool { return atomic.LoadInt32(&c.poolConnections) == 0 })
}

// TestWSMuxLifecycleConstructorFailure: when smux refuses to wrap a freshly
// dialed socket, that socket must be closed - it is not reachable from anywhere
// else - and the pool counter must come back to zero exactly once.
func TestWSMuxLifecycleConstructorFailure(t *testing.T) {
	srv := newLifecycleServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := lifecycleMuxConfig(srv.addr(), 1)
	cfg.MuxVersion = 3 // smux.Server rejects an unknown protocol version
	c := NewWSMuxClient(ctx, cfg, lifecycleLogger())
	c.Start()

	conns := srv.waitTunnels(t, 1)
	expectPeerClosed(t, "socket of a failed smux constructor", conns[0])

	cancel()
	waitFor(t, "pool counter to settle", func() bool { return atomic.LoadInt32(&c.poolConnections) == 0 })
}

// lifecycleSmuxConfig matches what NewWSMuxClient builds for lifecycleMuxConfig,
// so a test server can speak smux to the client's pool connections.
func lifecycleSmuxConfig() *smux.Config {
	return &smux.Config{
		Version:           2,
		KeepAliveInterval: 20 * time.Second,
		KeepAliveTimeout:  40 * time.Second,
		MaxFrameSize:      32768,
		MaxReceiveBuffer:  4194304,
		MaxStreamBuffer:   65536,
	}
}

// muxServer wraps every accepted tunnel socket in the smux client side, the way
// the real server does, and hands the sessions to the test.
func muxServer(t *testing.T) (*lifecycleServer, <-chan *smux.Session) {
	t.Helper()
	srv := newLifecycleServer(t)
	sessions := make(chan *smux.Session, 32)
	srv.tunnelHook = func(conn net.Conn) {
		sess, err := smux.Client(conn, lifecycleSmuxConfig())
		if err != nil {
			conn.Close()
			return
		}
		sessions <- sess
	}
	return srv, sessions
}

func nextSession(t *testing.T, sessions <-chan *smux.Session) *smux.Session {
	t.Helper()
	select {
	case s := <-sessions:
		return s
	case <-time.After(lifecycleDeadline):
		t.Fatal("timed out waiting for a pool session")
		return nil
	}
}

// holdTarget is a local TCP service that accepts connections and reports when
// each one is closed by the client.
type holdTarget struct {
	ln       net.Listener
	accepted chan struct{}
	closed   chan struct{}
}

func newHoldTarget(t *testing.T) *holdTarget {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h := &holdTarget{ln: ln, accepted: make(chan struct{}, 8), closed: make(chan struct{}, 8)}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			h.accepted <- struct{}{}
			go func() {
				io.Copy(io.Discard, conn) // returns when the client closes its end
				conn.Close()
				h.closed <- struct{}{}
			}()
		}
	}()
	return h
}

func (h *holdTarget) addr() string { return h.ln.Addr().String() }

func expectSignal(t *testing.T, what string, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(lifecycleDeadline):
		t.Fatalf("timed out waiting for: %s", what)
	}
}

func pendingStripeGroups(c *WsMuxTransport) int {
	c.stripeGroupsMu.Lock()
	defer c.stripeGroupsMu.Unlock()
	return len(c.stripeGroups)
}

// TestWSMuxLifecycleRestart: a full restart must close everything the old
// generation owned - the pool socket, a running plain flow's local connection, a
// UDP flow, and the legs of an incomplete stripe group - and only return once
// every one of its workers has ended, with the new generation starting clean.
func TestWSMuxLifecycleRestart(t *testing.T) {
	srv, sessions := muxServer(t)
	target := newHoldTarget(t)

	udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { udp.Close() })
	go func() {
		buf := make([]byte, 1500)
		for {
			n, from, err := udp.ReadFromUDP(buf)
			if err != nil {
				return
			}
			udp.WriteToUDP(buf[:n], from)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := NewWSMuxClient(ctx, lifecycleMuxConfig(srv.addr(), 1), lifecycleLogger())
	c.Start()

	sess := nextSession(t, sessions)

	// A running plain flow.
	plain, err := sess.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := utils.SendFlowPlain(plain, 0, target.addr()); err != nil {
		t.Fatal(err)
	}
	expectSignal(t, "plain flow to reach its local target", target.accepted)

	// A running UDP flow, proven live by an echoed datagram.
	udpStream, err := sess.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := utils.SendFlowUDP(udpStream, udp.LocalAddr().String()); err != nil {
		t.Fatal(err)
	}
	if err := utils.WriteUDPFrame(udpStream, []byte("dgram")); err != nil {
		t.Fatal(err)
	}
	_ = udpStream.SetReadDeadline(time.Now().Add(lifecycleDeadline))
	buf := make([]byte, 64)
	if n, err := utils.ReadUDPFrame(udpStream, buf); err != nil || string(buf[:n]) != "dgram" {
		t.Fatalf("udp flow not live: n=%d err=%v", n, err)
	}

	// One leg of a three-leg stripe group: the group stays pending.
	leg, err := sess.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := utils.SendFlowStriped(leg, 7, 0, 3, 0, target.addr()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "stripe group to be pending", func() bool { return pendingStripeGroups(c) == 1 })
	waitFor(t, "the pool connection to be counted", func() bool { return atomic.LoadInt32(&c.poolConnections) == 1 })

	old := c.gen
	c.Restart()

	// Restart returned, so the old generation has nothing left running.
	if !old.join(time.Second) {
		t.Fatal("Restart returned while old-generation workers were still running")
	}
	expectSignal(t, "the plain flow's local connection to be closed", target.closed)
	// The far end sees the pool socket gone: its streams fail instead of timing out.
	_ = plain.SetReadDeadline(time.Now().Add(lifecycleDeadline))
	if _, err := plain.Read(make([]byte, 1)); err == nil {
		t.Fatal("old pool session still delivered data after the restart")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("old pool socket was not closed by the restart")
	}
	if n := pendingStripeGroups(c); n != 0 {
		t.Fatalf("%d stripe group(s) survived the restart", n)
	}

	// The new generation is live and did not inherit the old one's counters.
	sess2 := nextSession(t, sessions)
	if sess2 == sess {
		t.Fatal("new generation reused the old session")
	}
	waitFor(t, "the new pool connection to be counted once", func() bool { return atomic.LoadInt32(&c.poolConnections) == 1 })
	if c.gen == old {
		t.Fatal("Restart did not publish a new generation")
	}
}

// stuckWorker registers a worker on g that ends only when release is closed,
// and decrements the pool counter when it does - the shape of a session's late
// close callback. It returns once the worker is running.
func stuckWorker(c *WsMuxTransport, g *wsGeneration) (release chan struct{}) {
	release, running := make(chan struct{}), make(chan struct{})
	g.start(func() {
		close(running)
		<-release
		atomic.AddInt32(&c.poolConnections, -1)
	})
	<-running
	return release
}

// TestWSMuxLifecycleRestartWaitsForOldWorkers is the publication barrier: while
// an old-generation callback is still pending, Restart must not publish, and the
// callback's late counter update must land on the old counters - the new
// generation's stay untouched.
func TestWSMuxLifecycleRestartWaitsForOldWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Nothing listens here: the new generation's dials fail, so its counters
	// only change if a stale callback changes them.
	c := NewWSMuxClient(ctx, lifecycleMuxConfig("127.0.0.1:1", 1), lifecycleLogger())
	c.Start()

	old := c.gen
	atomic.StoreInt32(&c.poolConnections, 1) // as if one pool connection were live
	release := stuckWorker(c, old)

	restarted := make(chan struct{})
	go func() {
		c.Restart()
		close(restarted)
	}()

	select {
	case <-restarted:
		t.Fatal("Restart published a new generation while an old callback was still pending")
	case <-time.After(300 * time.Millisecond): // absence check only; progress below is signalled
	}
	// The generation is stopped: it accepts no new workers or sockets.
	waitFor(t, "the old generation to be stopped", func() bool { return old.ctx.Err() != nil })
	if old.start(func() {}) {
		t.Fatal("a stopped generation accepted a new worker")
	}

	close(release) // the old callback finally runs
	waitClosed(t, "Restart to finish", restarted)

	if got := atomic.LoadInt32(&c.poolConnections); got != 0 {
		t.Fatalf("new generation's pool counter is %d, want 0: the stale callback mutated it", got)
	}
	if c.gen == old {
		t.Fatal("Restart did not publish a new generation")
	}
}

// TestWSMuxLifecycleRestartJoinTimeout: a worker that never ends must not let
// Restart publish a new generation over it, nor hang the caller; once the worker
// ends the retry publishes.
func TestWSMuxLifecycleRestartJoinTimeout(t *testing.T) {
	prev := restartJoinTimeout
	restartJoinTimeout = 150 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	// LIFO: cancel the transport first, then restore the timeout.
	defer func() { restartJoinTimeout = prev }()
	defer cancel()

	c := NewWSMuxClient(ctx, lifecycleMuxConfig("127.0.0.1:1", 1), lifecycleLogger())
	c.Start()

	old := c.gen
	release := stuckWorker(c, old)

	returned := make(chan struct{})
	go func() {
		c.Restart()
		close(returned)
	}()
	waitClosed(t, "Restart to give up on the stuck worker", returned)

	c.restartMutex.Lock()
	published := c.gen != old
	c.restartMutex.Unlock()
	if published {
		t.Fatal("a new generation was published over a worker that had not ended")
	}

	close(release)
	waitFor(t, "the retried Restart to publish", func() bool {
		c.restartMutex.Lock()
		defer c.restartMutex.Unlock()
		return c.gen != old
	})
}

// TestWSMuxLifecycleControlReattach: losing the control channel re-dials it
// within the same generation and leaves the pool and a running flow alone.
func TestWSMuxLifecycleControlReattach(t *testing.T) {
	srv, sessions := muxServer(t)

	echo, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { echo.Close() })
	go func() {
		for {
			conn, err := echo.Accept()
			if err != nil {
				return
			}
			go func() { io.Copy(conn, conn); conn.Close() }()
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := NewWSMuxClient(ctx, lifecycleMuxConfig(srv.addr(), 1), lifecycleLogger())
	c.Start()

	sess := nextSession(t, sessions)
	waitFor(t, "first control channel", func() bool { return len(srv.controlConns()) == 1 })
	gen, genCtx := c.gen, c.ctx

	flow, err := sess.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := utils.SendFlowPlain(flow, 0, echo.Addr().String()); err != nil {
		t.Fatal(err)
	}
	roundTrip := func(msg string) {
		t.Helper()
		_ = flow.SetDeadline(time.Now().Add(lifecycleDeadline))
		if _, err := flow.Write([]byte(msg)); err != nil {
			t.Fatalf("write %q: %v", msg, err)
		}
		got := make([]byte, len(msg))
		if _, err := io.ReadFull(flow, got); err != nil || string(got) != msg {
			t.Fatalf("echo of %q: got %q, err %v", msg, got, err)
		}
	}
	roundTrip("before")

	// The far end drops the control channel.
	srv.controlConns()[0].Close()
	waitFor(t, "the client to re-establish its control channel", func() bool { return len(srv.controlConns()) == 2 })

	roundTrip("after")
	if c.gen != gen || c.ctx != genCtx {
		t.Fatal("control reattach replaced the generation")
	}
	if genCtx.Err() != nil {
		t.Fatal("control reattach cancelled the generation")
	}
	if n := len(srv.tunnelConns()); n != 1 {
		t.Fatalf("control reattach changed the pool: %d tunnel sockets accepted, want 1", n)
	}
	if got := atomic.LoadInt32(&c.poolConnections); got != 1 {
		t.Fatalf("pool counter is %d after reattach, want 1", got)
	}
}

// TestWSMuxLifecycleRestartAbandonedOnShutdown: parent cancellation before a
// restart completes must not start a fresh generation.
func TestWSMuxLifecycleRestartAbandonedOnShutdown(t *testing.T) {
	srv := newLifecycleServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	c := NewWSMuxClient(ctx, lifecycleMuxConfig(srv.addr(), 1), lifecycleLogger())
	c.Start()
	srv.waitTunnels(t, 1)

	old := c.gen
	cancel()
	c.Restart()

	c.restartMutex.Lock()
	same := c.gen == old
	c.restartMutex.Unlock()
	if !same {
		t.Fatal("a new generation was started after the parent context was cancelled")
	}
	time.Sleep(200 * time.Millisecond) // absence check: no fresh dials arrive
	if n := len(srv.tunnelConns()); n != 1 {
		t.Fatalf("%d tunnel sockets accepted, want only the original one", n)
	}
}
