package transport

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/musix/backhaul/config"
	"github.com/musix/backhaul/internal/utils"
	"github.com/musix/backhaul/internal/utils/network"
	"github.com/sirupsen/logrus"
	"github.com/xtaci/smux"
)

const (
	lcToken    = "lifecycle-test-token"
	lcDeadline = 5 * time.Second
)

func lcWaitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	end := time.Now().Add(lcDeadline)
	for !cond() {
		if time.Now().After(end) {
			t.Fatalf("timed out waiting for: %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func lcWaitClosed(t *testing.T, what string, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(lcDeadline):
		t.Fatalf("timed out waiting for: %s", what)
	}
}

func lcFreeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// trackedConn reports, on readClosed, that the far end (or this side) closed the
// socket: the first failed Read closes the channel.
type trackedConn struct {
	net.Conn
	readClosed chan struct{}
	once       sync.Once
}

func (c *trackedConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if err != nil {
		c.once.Do(func() { close(c.readClosed) })
	}
	return n, err
}

// poolPeer is the client end of one pool connection: the smux server side, as
// the real client runs it.
type poolPeer struct {
	sess       *smux.Session
	readClosed chan struct{}
}

func lcSmuxConfig() *smux.Config {
	return &smux.Config{
		Version:           2,
		KeepAliveInterval: 20 * time.Second,
		KeepAliveTimeout:  40 * time.Second,
		MaxFrameSize:      32768,
		MaxReceiveBuffer:  4194304,
		MaxStreamBuffer:   65536,
	}
}

// serveEcho answers the streams the server opens on this connection: RTT probes
// are echoed, plain flows are echoed byte for byte (a stand-in for the local
// service the real client would dial).
func (p *poolPeer) serveEcho() {
	for {
		st, err := p.sess.AcceptStream()
		if err != nil {
			return
		}
		go func() {
			defer st.Close()
			kind, err := utils.ReadFlowKind(st)
			if err != nil {
				return
			}
			switch kind {
			case utils.FlowPing:
				if nonce, err := utils.ReceiveFlowPing(st); err == nil {
					_ = utils.EchoFlowPing(st, nonce)
				}
			case utils.FlowPlain:
				if _, _, err := utils.ReceiveFlowPlain(st); err != nil {
					return
				}
				io.Copy(st, st)
			case utils.FlowUDP:
				if _, err := utils.ReceiveFlowUDP(st); err != nil {
					return
				}
				buf := make([]byte, 64*1024)
				for {
					n, err := utils.ReadUDPFrame(st, buf)
					if err != nil || utils.WriteUDPFrame(st, buf[:n]) != nil {
						return
					}
				}
			}
		}()
	}
}

type lcHarness struct {
	t        *testing.T
	s        *WsMuxTransport
	addr     string
	userAddr string
	cancel   context.CancelFunc
}

func newLCHarness(t *testing.T, opts ...func(*WsMuxConfig)) *lcHarness {
	t.Helper()
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	h := &lcHarness{t: t, addr: lcFreeAddr(t), userAddr: lcFreeAddr(t), cancel: cancel}
	cfg := &WsMuxConfig{
		BindAddr:         h.addr,
		Token:            lcToken,
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
		Ports:            []string{h.userAddr + "=1"},
	}
	for _, o := range opts {
		o(cfg)
	}
	h.s = NewWSMuxServer(ctx, cfg, logger)
	h.s.Start()
	// Wait for the tunnel listener to accept, so the first WebSocket dial does not
	// fail and sit out the dialer's one-second backoff.
	lcWaitFor(t, "the tunnel listener to accept", func() bool {
		c, err := net.DialTimeout("tcp", h.addr, time.Second)
		if err != nil {
			return false
		}
		c.Close()
		return true
	})
	return h
}

func (h *lcHarness) dial(path string) *network.WebSocketConn {
	h.t.Helper()
	conn, err := network.WebSocketDialer(context.Background(), h.addr, "", path, 2*time.Second, 30*time.Second, true, lcToken, "lifecycle-test", config.WSMUX, 5, 0, 0, 0, false)
	if err != nil {
		h.t.Fatalf("dial %s: %v", path, err)
	}
	h.t.Cleanup(func() { conn.Close() })
	return conn
}

func (h *lcHarness) control() *network.WebSocketConn { return h.dial("/channel") }

func (h *lcHarness) pool() *poolPeer {
	h.t.Helper()
	conn := h.dial("/tunnel")
	tc := &trackedConn{Conn: conn.NetConn(), readClosed: make(chan struct{})}
	sess, err := smux.Server(tc, lcSmuxConfig())
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { sess.Close() })
	p := &poolPeer{sess: sess, readClosed: tc.readClosed}
	go p.serveEcho()
	return p
}

// user opens a public-port connection and returns it once the listener is up.
func (h *lcHarness) user() net.Conn {
	h.t.Helper()
	var conn net.Conn
	lcWaitFor(h.t, "the public port to accept", func() bool {
		c, err := net.DialTimeout("tcp", h.userAddr, time.Second)
		if err != nil {
			return false
		}
		conn = c
		return true
	})
	h.t.Cleanup(func() { conn.Close() })
	return conn
}

func lcEcho(t *testing.T, conn net.Conn, msg string) {
	t.Helper()
	_ = conn.SetDeadline(time.Now().Add(lcDeadline))
	if _, err := conn.Write([]byte(msg)); err != nil {
		t.Fatalf("write %q: %v", msg, err)
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(conn, got); err != nil || string(got) != msg {
		t.Fatalf("echo of %q: got %q, err %v", msg, got, err)
	}
}

func lcExpectEOF(t *testing.T, what string, conn net.Conn) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(lcDeadline))
	_, err := conn.Read(make([]byte, 1))
	if err == nil {
		t.Fatalf("%s: unexpectedly received data", what)
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatalf("%s: still open after the restart", what)
	}
}

func (h *lcHarness) sessions() int { return int(atomic.LoadInt32(&h.s.sessionCounter)) }

// TestWSMuxLifecycleRestart: a full restart must close every socket the old
// generation owned - a session still queued before admission, admitted ones, one
// pulled out of the selection registry while it drains, and the user connection
// of a running flow - and only return once every worker has ended. The next
// generation then starts clean on the very same ports, with no sleep between.
func TestWSMuxLifecycleRestart(t *testing.T) {
	t.Run("queued session", func(t *testing.T) {
		h := newLCHarness(t)
		// No control channel yet: nothing admits this session, it sits in the
		// tunnel channel.
		queued := h.pool()
		lcWaitFor(t, "the session to be queued", func() bool { return len(h.s.tunnelChannel) == 1 })

		old := h.s.gen
		h.s.Restart()

		lcWaitClosed(t, "the queued session's socket to be closed", queued.readClosed)
		if !old.join(time.Second) {
			t.Fatal("Restart returned while old-generation workers were still running")
		}
		if n := len(h.s.tunnelChannel); n != 0 {
			t.Fatalf("%d session(s) still queued in the new generation", n)
		}
	})

	t.Run("admitted draining and active flow", func(t *testing.T) {
		h := newLCHarness(t)
		h.control()
		peers := []*poolPeer{h.pool(), h.pool(), h.pool()}
		lcWaitFor(t, "three admitted sessions", func() bool { return h.sessions() == 3 && h.s.liveSessionCount() == 3 })

		// Pull one out of the selection registry, as rotation does while a
		// session drains: it stays alive and carrying streams, so it must stay
		// owned.
		h.s.sessionsMu.Lock()
		draining := h.s.sessions[len(h.s.sessions)-1].session
		h.s.sessionsMu.Unlock()
		h.s.unregisterSession(draining)
		if n := h.s.liveSessionCount(); n != 2 {
			t.Fatalf("%d selectable sessions after unregistering one, want 2", n)
		}

		user := h.user()
		lcEcho(t, user, "hello")

		old := h.s.gen
		h.s.Restart()

		if !old.join(time.Second) {
			t.Fatal("Restart returned while old-generation workers were still running")
		}
		for i, p := range peers {
			lcWaitClosed(t, fmt.Sprintf("pool socket %d (queued, admitted or draining) to be closed", i), p.readClosed)
		}
		lcExpectEOF(t, "the running flow's user connection", user)

		// Counters and registries belong to the new generation: empty.
		for name, v := range map[string]int32{
			"sessionCounter": atomic.LoadInt32(&h.s.sessionCounter),
			"streamCounter":  atomic.LoadInt32(&h.s.streamCounter),
			"plainFlows":     atomic.LoadInt32(&h.s.plainFlows),
			"stripedFlows":   atomic.LoadInt32(&h.s.stripedFlows),
		} {
			if v != 0 {
				t.Errorf("%s = %d after restart, want 0", name, v)
			}
		}
		if n := h.s.liveSessionCount(); n != 0 {
			t.Errorf("%d sessions registered after restart", n)
		}
		// The old port listener is gone until the new generation has a control
		// channel again.
		if c, err := net.DialTimeout("tcp", h.userAddr, time.Second); err == nil {
			c.Close()
			t.Error("the public port still accepts after restart")
		}

		// The new generation works, binding the same port straight away.
		h.control()
		h.pool()
		lcWaitFor(t, "a session in the new generation", func() bool { return h.sessions() == 1 })
		lcEcho(t, h.user(), "again")
		if h.s.gen == old {
			t.Fatal("Restart did not publish a new generation")
		}
	})
}

// lcStuckWorker registers a worker on g that ends only when release is closed,
// and decrements the session counter when it does - the shape of a session's late
// close callback. It returns once the worker is running.
func lcStuckWorker(s *WsMuxTransport, g *wsGeneration) (release chan struct{}) {
	release, running := make(chan struct{}), make(chan struct{})
	g.start(func() {
		close(running)
		<-release
		atomic.AddInt32(&s.sessionCounter, -1)
	})
	<-running
	return release
}

// TestWSMuxLifecycleRestartWaitsForOldWorkers is the publication barrier: while
// an old-generation callback is pending Restart must not publish, and the
// callback's late counter update lands on the old counters - the new generation's
// stay untouched.
func TestWSMuxLifecycleRestartWaitsForOldWorkers(t *testing.T) {
	h := newLCHarness(t)
	old := h.s.gen
	atomic.StoreInt32(&h.s.sessionCounter, 1) // as if one session were live
	release := lcStuckWorker(h.s, old)

	restarted := make(chan struct{})
	go func() {
		h.s.Restart()
		close(restarted)
	}()

	select {
	case <-restarted:
		t.Fatal("Restart published a new generation while an old callback was still pending")
	case <-time.After(300 * time.Millisecond): // absence check only; progress below is signalled
	}
	lcWaitFor(t, "the old generation to be stopped", func() bool { return old.ctx.Err() != nil })
	if old.start(func() {}) {
		t.Fatal("a stopped generation accepted a new worker")
	}
	if old.own(nopCloser{}) {
		t.Fatal("a stopped generation accepted a new socket")
	}

	close(release) // the old callback finally runs
	lcWaitClosed(t, "Restart to finish", restarted)

	if got := atomic.LoadInt32(&h.s.sessionCounter); got != 0 {
		t.Fatalf("new generation's session counter is %d, want 0: the stale callback mutated it", got)
	}
	if h.s.gen == old {
		t.Fatal("Restart did not publish a new generation")
	}
}

type nopCloser struct{}

func (nopCloser) Close() error { return nil }

// TestWSMuxLifecycleRestartJoinTimeout: a worker that never ends must not let
// Restart publish a new generation over it, nor hang the caller; once it ends
// the retry publishes.
func TestWSMuxLifecycleRestartJoinTimeout(t *testing.T) {
	prev := restartJoinTimeout
	restartJoinTimeout = 150 * time.Millisecond
	defer func() { restartJoinTimeout = prev }()

	h := newLCHarness(t)
	old := h.s.gen
	release := lcStuckWorker(h.s, old)

	returned := make(chan struct{})
	go func() {
		h.s.Restart()
		close(returned)
	}()
	lcWaitClosed(t, "Restart to give up on the stuck worker", returned)

	h.s.restartMutex.Lock()
	published := h.s.gen != old
	h.s.restartMutex.Unlock()
	if published {
		t.Fatal("a new generation was published over a worker that had not ended")
	}

	close(release)
	lcWaitFor(t, "the retried Restart to publish", func() bool {
		h.s.restartMutex.Lock()
		defer h.s.restartMutex.Unlock()
		return h.s.gen != old
	})
	// Nothing may still be armed to touch the restored timeout.
	h.cancel()
}

// TestWSMuxLifecycleRestartAbandonedOnShutdown: parent cancellation while (or
// before) a restart runs must not start a fresh generation.
func TestWSMuxLifecycleRestartAbandonedOnShutdown(t *testing.T) {
	h := newLCHarness(t)
	old := h.s.gen
	h.cancel()

	h.s.Restart()

	h.s.restartMutex.Lock()
	same := h.s.gen == old
	h.s.restartMutex.Unlock()
	if !same {
		t.Fatal("a new generation was started after the parent context was cancelled")
	}
}

// TestWSMuxLifecycleControlClosedRestart: the client's SG_Closed makes the
// control handler ask for a full restart. The handler is one of the workers the
// restart joins, so it must ask from outside itself, not run it inline.
func TestWSMuxLifecycleControlClosedRestart(t *testing.T) {
	h := newLCHarness(t)
	ctl := h.control()
	peer := h.pool()
	lcWaitFor(t, "an admitted session", func() bool { return h.sessions() == 1 })
	old := h.s.gen

	if err := utils.WriteControlSignal(ctl, utils.SG_Closed); err != nil {
		t.Fatal(err)
	}

	lcWaitClosed(t, "the pool socket to be closed by the restart", peer.readClosed)
	lcWaitFor(t, "the restart to publish", func() bool {
		h.s.restartMutex.Lock()
		defer h.s.restartMutex.Unlock()
		return h.s.gen != old
	})
}

// TestWSMuxLifecycleControlReattach: losing the control channel and reattaching
// one stays inside the same generation and leaves the pool session and a running
// flow untouched.
func TestWSMuxLifecycleControlReattach(t *testing.T) {
	h := newLCHarness(t)
	ctl := h.control()
	peer := h.pool()
	lcWaitFor(t, "an admitted session", func() bool { return h.sessions() == 1 })

	user := h.user()
	lcEcho(t, user, "before")
	gen, genCtx := h.s.gen, h.s.ctx

	// The client's control connection dies; the server holds the pool.
	ctl.Close()
	lcWaitFor(t, "the server to notice the lost control channel", func() bool {
		h.s.controlMu.Lock()
		defer h.s.controlMu.Unlock()
		return h.s.controlChannel == nil
	})

	h.control() // reattach
	lcWaitFor(t, "the control channel to be reattached", func() bool {
		h.s.controlMu.Lock()
		defer h.s.controlMu.Unlock()
		return h.s.controlChannel != nil
	})

	lcEcho(t, user, "after")
	select {
	case <-peer.readClosed:
		t.Fatal("the pool socket was closed by a control reattach")
	default:
	}
	if h.s.gen != gen || h.s.ctx != genCtx || genCtx.Err() != nil {
		t.Fatal("control reattach replaced or cancelled the generation")
	}
	if n := h.sessions(); n != 1 {
		t.Fatalf("session counter is %d after reattach, want 1", n)
	}
}

// TestWSMuxLifecycleRestartUDPWorkers: the UDP listener, its read loop and its
// per-source flow workers are all part of the generation. After a restart the
// UDP port is free again (the listener was closed, not leaked) and every worker
// has ended.
func TestWSMuxLifecycleRestartUDPWorkers(t *testing.T) {
	h := newLCHarness(t, func(c *WsMuxConfig) { c.AcceptUDP = true })
	h.control()
	peer := h.pool()
	lcWaitFor(t, "an admitted session", func() bool { return h.sessions() == 1 })

	// One live UDP flow, proven by an echoed datagram.
	var client *net.UDPConn
	lcWaitFor(t, "the UDP port to be up", func() bool {
		raddr, _ := net.ResolveUDPAddr("udp", h.userAddr)
		c, err := net.DialUDP("udp", nil, raddr)
		if err != nil {
			return false
		}
		_ = c.SetDeadline(time.Now().Add(500 * time.Millisecond))
		if _, err := c.Write([]byte("dgram")); err != nil {
			c.Close()
			return false
		}
		buf := make([]byte, 64)
		if n, err := c.Read(buf); err != nil || string(buf[:n]) != "dgram" {
			c.Close()
			return false
		}
		client = c
		return true
	})
	defer client.Close()

	old := h.s.gen
	h.s.Restart()

	if !old.join(time.Second) {
		t.Fatal("Restart returned while old-generation workers were still running")
	}
	lcWaitClosed(t, "the pool socket to be closed", peer.readClosed)
	laddr, _ := net.ResolveUDPAddr("udp", h.userAddr)
	l, err := net.ListenUDP("udp", laddr)
	if err != nil {
		t.Fatalf("the UDP port was not released by the restart: %v", err)
	}
	l.Close()
}

// TestWSMuxLifecycleRestartPendingDispatch: a user connection whose striped
// setup is still waiting for enough pool sessions (it requeues and backs off)
// must be closed by the restart itself - not left to a later timeout - and its
// dispatch worker must have ended.
func TestWSMuxLifecycleRestartPendingDispatch(t *testing.T) {
	h := newLCHarness(t, func(c *WsMuxConfig) { c.StripeFactor = 2 })
	h.control()
	h.pool() // one session: a two-leg group can never be opened
	lcWaitFor(t, "an admitted session", func() bool { return h.sessions() == 1 })

	user := h.user()
	lcWaitFor(t, "the connection to be counted", func() bool { return atomic.LoadInt32(&h.s.streamCounter) == 1 })

	old := h.s.gen
	h.s.Restart()

	if !old.join(time.Second) {
		t.Fatal("Restart returned while old-generation workers were still running")
	}
	// Already closed when Restart returns; a short deadline separates that from
	// the dispatcher's own 3s setup timeout.
	_ = user.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := user.Read(make([]byte, 1)); err == nil {
		t.Fatal("unexpectedly received data")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("the pending user connection survived the restart")
	}
}
