package transport

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/musix/backhaul/config"
	client_transport "github.com/musix/backhaul/internal/client/transport"
	"github.com/musix/backhaul/internal/utils"
	"github.com/musix/backhaul/internal/utils/handlers"
	"github.com/musix/backhaul/internal/utils/striping"
	"github.com/sirupsen/logrus"
	"github.com/xtaci/smux"
)

// Plan 005: plain-to-striped promotion. TestWSMuxPromotionHandshake drives the
// server's promoteFlow against a scripted client (real smux pool sessions, no
// real client), TestWSMuxPromotionE2E runs the real client and server.

// promoWait bounds every wait for something that must happen.
const promoWait = 10 * time.Second

func promoRecv[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(promoWait):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}

func promoPayload(n int, salt byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*131+i/251) ^ salt
	}
	return b
}

// --- log capture: the observable "promotion completed" signal -----------------

// promoHook records the promotion outcome logged by one side.
type promoHook struct {
	promoted chan struct{} // receives one token per successful promotion
	failed   atomic.Int32  // "promotion of flow ... failed" warnings
}

func newPromoHook() *promoHook { return &promoHook{promoted: make(chan struct{}, 8)} }

func (h *promoHook) Levels() []logrus.Level { return logrus.AllLevels }

func (h *promoHook) Fire(e *logrus.Entry) error {
	switch {
	case strings.Contains(e.Message, "promoted to"):
		h.promoted <- struct{}{}
	case strings.Contains(e.Message, "promotion of flow"):
		h.failed.Add(1)
	}
	return nil
}

// --- the scripted client -----------------------------------------------------

type promoLeg struct {
	st                   *smux.Stream
	flowID               uint64
	group                uint32
	index, total, parity uint8
}

type promoFlow struct {
	st     *smux.Stream
	flowID uint64
}

// promoRig is a real server with a scripted client: pool sessions whose streams
// are handed to the test instead of being served.
type promoRig struct {
	h     *lcHarness
	flows chan promoFlow
	legs  chan promoLeg
	n     int // legs per promoted flow
}

func newPromoRig(t *testing.T, parity int, timeout time.Duration) *promoRig {
	t.Helper()
	old := handlers.PromoteHandshakeTimeout
	handlers.PromoteHandshakeTimeout = timeout
	t.Cleanup(func() { handlers.PromoteHandshakeTimeout = old })

	h := newLCHarness(t, func(c *WsMuxConfig) {
		c.StripeFactor = 2
		c.StripeParity = parity
		c.StripePorts = []string{"2"} // the destination is port 1: plain, promotable
		c.PromoteBytes = 4096
	})
	r := &promoRig{h: h, flows: make(chan promoFlow, 8), legs: make(chan promoLeg, 16), n: 2 + parity}
	h.control()
	for i := 0; i < r.n; i++ {
		conn := h.dial("/tunnel")
		tc := &trackedConn{Conn: conn.NetConn(), readClosed: make(chan struct{})}
		sess, err := smux.Server(tc, lcSmuxConfig())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { sess.Close() })
		go r.serve(sess)
	}
	lcWaitFor(t, "the pool sessions", func() bool { return h.sessions() == r.n && h.s.liveSessionCount() == r.n })
	return r
}

func (r *promoRig) serve(sess *smux.Session) {
	for {
		st, err := sess.AcceptStream()
		if err != nil {
			return
		}
		go func() {
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
				if id, _, err := utils.ReceiveFlowPlain(st); err == nil {
					r.flows <- promoFlow{st, id}
				}
			case utils.FlowPromote:
				if id, g, i, n, p, err := utils.ReceiveFlowPromote(st); err == nil {
					r.legs <- promoLeg{st, id, g, i, n, p}
				}
			}
		}()
	}
}

// start opens a user flow, sends 8 KiB (past promote_bytes), reads it on the
// scripted client's plain stream and collects the promotion legs the server
// opens, ordered by index.
func (r *promoRig) start(t *testing.T) (user net.Conn, flow promoFlow, legs []promoLeg, sent []byte) {
	t.Helper()
	user = r.h.user()
	_ = user.SetDeadline(time.Now().Add(2 * promoWait))
	sent = promoPayload(8192, 1)
	if _, err := user.Write(sent); err != nil {
		t.Fatal(err)
	}
	flow = promoRecv(t, r.flows, "the plain flow")
	if flow.flowID == 0 {
		t.Fatal("flow is not promotable")
	}
	_ = flow.st.SetReadDeadline(time.Now().Add(promoWait))
	got := make([]byte, len(sent))
	if _, err := io.ReadFull(flow.st, got); err != nil || !bytes.Equal(got, sent) {
		t.Fatalf("plain stream: %v", err)
	}
	_ = flow.st.SetReadDeadline(time.Time{})
	legs = make([]promoLeg, r.n)
	for i := 0; i < r.n; i++ {
		l := promoRecv(t, r.legs, "a promotion leg")
		if l.flowID != flow.flowID || int(l.total) != r.n || int(l.index) >= r.n || legs[l.index].st != nil {
			t.Fatalf("bad promotion leg header: %+v", l)
		}
		legs[l.index] = l
	}
	return user, flow, legs, sent
}

func expectClosed(t *testing.T, what string, c net.Conn) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(promoWait))
	if _, err := io.Copy(io.Discard, c); err != nil {
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			t.Fatalf("%s: still open", what)
		}
	}
}

func (r *promoRig) waitFlowsGone(t *testing.T) {
	t.Helper()
	lcWaitFor(t, "the flow's slot to be released", func() bool {
		return atomic.LoadInt32(&r.h.s.plainFlows) == 0 && atomic.LoadInt32(&r.h.s.streamCounter) == 0
	})
}

func TestWSMuxPromotionHandshake(t *testing.T) {
	for _, parity := range []int{0, 1} {
		parity := parity
		t.Run(fmt.Sprintf("parity=%d", parity), func(t *testing.T) {
			// The server writes its committed old-tunnel count first; the count
			// must be exactly the bytes the scripted client read on the plain stream.
			readOwn := func(t *testing.T, legs []promoLeg, want int) {
				t.Helper()
				_ = legs[0].st.SetReadDeadline(time.Now().Add(promoWait))
				own, err := utils.ReadCount(legs[0].st)
				if err != nil || own != uint64(want) {
					t.Fatalf("server count = %d, %v; want %d", own, err, want)
				}
				_ = legs[0].st.SetReadDeadline(time.Time{})
			}

			t.Run("stalled count peer", func(t *testing.T) {
				r := newPromoRig(t, parity, 400*time.Millisecond)
				user, flow, legs, sent := r.start(t)
				readOwn(t, legs, len(sent))
				// The peer never answers: the handshake gives up, the post-freeze
				// failure aborts the flow and closes every resource.
				expectClosed(t, "user connection", user)
				expectClosed(t, "plain stream", flow.st)
				for i, l := range legs {
					expectClosed(t, fmt.Sprintf("leg %d", i), l.st)
				}
				r.waitFlowsGone(t)
			})

			t.Run("leg closed before the count exchange", func(t *testing.T) {
				r := newPromoRig(t, parity, promoWait)
				user, flow, legs, _ := r.start(t)
				for _, l := range legs {
					l.st.Close()
				}
				expectClosed(t, "user connection", user)
				expectClosed(t, "plain stream", flow.st)
				r.waitFlowsGone(t)
			})

			t.Run("installed then legs closed", func(t *testing.T) {
				r := newPromoRig(t, parity, promoWait)
				user, flow, legs, sent := r.start(t)
				readOwn(t, legs, len(sent))
				// Our own committed old count: nothing was ever sent downstream.
				if err := utils.WriteCount(legs[0].st, 0); err != nil {
					t.Fatal(err)
				}
				// Only now build the striped wrapper on the scripted side (one
				// reader per leg), and prove the server switched over: the
				// upload now arrives on the group and not on the plain stream.
				conns := make([]net.Conn, len(legs))
				for i, l := range legs {
					conns[i] = l.st
				}
				var striped net.Conn
				if parity > 0 {
					c, err := striping.NewFEC(conns, striping.DefaultChunkSize, 2, parity)
					if err != nil {
						t.Fatal(err)
					}
					striped = c
				} else {
					striped = striping.New(conns, striping.DefaultChunkSize)
				}
				more := promoPayload(20000, 2)
				if _, err := user.Write(more); err != nil {
					t.Fatal(err)
				}
				_ = striped.SetReadDeadline(time.Now().Add(promoWait))
				got := make([]byte, len(more))
				if _, err := io.ReadFull(striped, got); err != nil || !bytes.Equal(got, more) {
					t.Fatalf("post-promotion upload: %v", err)
				}
				// The scripted client dies mid-stream (no clean end marker): the
				// flow must end, not hang.
				striped.(interface{ AbortWrite() }).AbortWrite()
				striped.Close()
				expectClosed(t, "user connection", user)
				expectClosed(t, "plain stream", flow.st)
				r.waitFlowsGone(t)
			})

			t.Run("restart releases a stalled handshake", func(t *testing.T) {
				// The 10 second bound is not what ends this one: cancelling the
				// generation does, and Restart waits for the worker.
				r := newPromoRig(t, parity, 10*time.Second)
				user, _, legs, sent := r.start(t)
				readOwn(t, legs, len(sent))
				done := make(chan struct{})
				go func() { r.h.s.Restart(); close(done) }()
				lcWaitClosed(t, "Restart to return with a promotion stalled", done)
				lcExpectEOF(t, "user connection", user)
			})

			t.Run("stuck old write fails before the freeze and stays plain", func(t *testing.T) {
				r := newPromoRig(t, parity, 400*time.Millisecond)
				user := r.h.user()
				_ = user.SetDeadline(time.Now().Add(2 * promoWait))
				first := promoPayload(8192, 3)
				user.Write(first)
				flow := promoRecv(t, r.flows, "the plain flow")
				_ = flow.st.SetReadDeadline(time.Now().Add(promoWait))
				if _, err := io.ReadFull(flow.st, make([]byte, len(first))); err != nil {
					t.Fatal(err)
				}
				// The scripted client stops reading the plain stream while the user
				// keeps uploading: the server's old-tunnel write stalls on flow
				// control, so its freeze cannot complete.
				rest := promoPayload(600*1024, 4)
				go user.Write(rest)
				var legs []promoLeg
				for len(legs) < r.n {
					legs = append(legs, promoRecv(t, r.legs, "a promotion leg"))
				}
				for _, l := range legs { // the failed attempt closes every new leg
					expectClosed(t, "promotion leg", l.st)
				}
				// The flow is still running plain and loses nothing.
				_ = flow.st.SetReadDeadline(time.Now().Add(promoWait))
				got := make([]byte, len(rest))
				if _, err := io.ReadFull(flow.st, got); err != nil || !bytes.Equal(got, rest) {
					t.Fatalf("plain flow after a failed pre-freeze attempt: %v", err)
				}
				if _, err := flow.st.Write([]byte("still alive")); err != nil {
					t.Fatalf("download direction: %v", err)
				}
				buf := make([]byte, len("still alive"))
				if _, err := io.ReadFull(user, buf); err != nil || string(buf) != "still alive" {
					t.Fatalf("user did not receive the plain download: %q %v", buf, err)
				}
			})
		})
	}
}

// --- end to end, real client and real server -----------------------------------

// startPromoClient runs the real wsmux client against h with hook attached.
func startPromoClient(t *testing.T, h *lcHarness, parity int, hook *promoHook) context.CancelFunc {
	t.Helper()
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	logger.SetLevel(logrus.DebugLevel)
	logger.AddHook(hook)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	client := client_transport.NewWSMuxClient(ctx, &client_transport.WsMuxConfig{
		RemoteAddr: h.addr, Token: lcToken, Mode: config.WSMUX, Path: "/", MuxVersion: 2,
		ConnPoolSize: 8, DialTimeOut: 5 * time.Second, RetryInterval: 20 * time.Millisecond,
		KeepAlive: 30 * time.Second, MaxFrameSize: 32768, MaxReceiveBuffer: 4194304,
		MaxStreamBuffer: 65536, StripeFactor: 2, StripeParity: parity,
	}, logger)
	go client.Start()
	lcWaitFor(t, "the client's pool sessions", func() bool { return h.sessions() >= 2+parity })
	return cancel
}

// promoTarget is the destination behind the tunnel. It answers in stages so the
// traffic straddles the promotion: dn1 before it, dn2 after it, then (only once
// the user's EOF has arrived) the final reply.
type promoTarget struct {
	ln      net.Listener
	gotUp1  chan struct{} // the first upload chunk crossed the plain tunnel
	release chan struct{} // closed once promotion completed on both sides
	result  chan promoTargetResult
}

type promoTargetResult struct {
	up  []byte
	err error
}

func newPromoTarget(t *testing.T, up1 int, dn1, dn2, reply []byte) *promoTarget {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	tg := &promoTarget{ln: ln, gotUp1: make(chan struct{}), release: make(chan struct{}), result: make(chan promoTargetResult, 1)}
	go func() {
		c, err := ln.Accept()
		if err != nil {
			tg.result <- promoTargetResult{err: err}
			return
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(3 * promoWait))
		first := make([]byte, up1)
		if _, err := io.ReadFull(c, first); err != nil {
			tg.result <- promoTargetResult{err: err}
			return
		}
		close(tg.gotUp1)
		if _, err := c.Write(dn1); err != nil {
			tg.result <- promoTargetResult{err: err}
			return
		}
		<-tg.release
		if _, err := c.Write(dn2); err != nil {
			tg.result <- promoTargetResult{err: err}
			return
		}
		rest, err := io.ReadAll(c) // to the user's EOF
		if err != nil {
			tg.result <- promoTargetResult{err: err}
			return
		}
		// The reply is only sent after the request's EOF reached the target.
		if _, err := c.Write(reply); err != nil {
			tg.result <- promoTargetResult{err: err}
			return
		}
		_ = c.(*net.TCPConn).CloseWrite()
		tg.result <- promoTargetResult{up: append(first, rest...)}
	}()
	return tg
}

func TestWSMuxPromotionE2E(t *testing.T) {
	for _, parity := range []int{0, 1} {
		parity := parity
		t.Run(fmt.Sprintf("parity=%d", parity), func(t *testing.T) {
			const up1Len = 256 * 1024
			up := promoPayload(up1Len+300*1024+17, 5)
			dn1, dn2, reply := promoPayload(200*1024, 6), promoPayload(250*1024+3, 7), promoPayload(90*1024+1, 8)
			tg := newPromoTarget(t, up1Len, dn1, dn2, reply)

			serverHook := newPromoHook()
			h := newLCHarness(t, toTarget(tg.ln.Addr().String()), func(c *WsMuxConfig) {
				c.StripeFactor = 2
				c.StripeParity = parity
				c.StripePorts = []string{"2"} // the destination is not listed: starts plain
				c.PromoteBytes = 64 * 1024
			})
			h.s.logger.SetLevel(logrus.DebugLevel)
			h.s.logger.AddHook(serverHook)
			clientHook := newPromoHook()
			startPromoClient(t, h, parity, clientHook)

			user := h.user().(*net.TCPConn)
			_ = user.SetDeadline(time.Now().Add(3 * promoWait))
			type recv struct {
				b   []byte
				err error
			}
			down := make(chan recv, 1)
			go func() {
				b, err := io.ReadAll(user)
				down <- recv{b, err}
			}()

			// Stage 1 travels the plain tunnel: it is past promote_bytes, and the
			// target has it before anything else happens.
			if _, err := user.Write(up[:up1Len]); err != nil {
				t.Fatal(err)
			}
			promoRecv(t, tg.gotUp1, "the first upload chunk at the target")

			// Promotion must complete on BOTH sides; it is asserted, not inferred.
			promoRecv(t, serverHook.promoted, "the server to log a completed promotion")
			promoRecv(t, clientHook.promoted, "the client to log a completed promotion")
			close(tg.release)

			// Stage 2 travels the striped group, then the EOF, then the reply.
			if _, err := user.Write(up[up1Len:]); err != nil {
				t.Fatal(err)
			}
			if err := user.CloseWrite(); err != nil {
				t.Fatal(err)
			}
			res := promoRecv(t, tg.result, "the target to finish")
			if res.err != nil || !bytes.Equal(res.up, up) {
				t.Fatalf("upload: target got %d bytes, err %v; want the exact %d", len(res.up), res.err, len(up))
			}
			d := promoRecv(t, down, "the user's download")
			want := append(append(append([]byte{}, dn1...), dn2...), reply...)
			if d.err != nil || !bytes.Equal(d.b, want) {
				t.Fatalf("download: user got %d bytes, err %v; want the exact %d", len(d.b), d.err, len(want))
			}
			if n := serverHook.failed.Load() + clientHook.failed.Load(); n != 0 {
				t.Fatalf("%d promotion failure(s) were logged", n)
			}
			if len(serverHook.promoted) != 0 || len(clientHook.promoted) != 0 {
				t.Fatal("a flow was promoted more than once")
			}
		})
	}

	// A reply that must survive the request's EOF BEFORE promotion would need
	// directional EOF on the plain smux stream, which promotable flows do not
	// have: they keep the legacy FlowPlain full close.
	t.Run("reply after upload EOF before promotion", func(t *testing.T) {
		t.Skip("smux half-close on promotable flows is unsupported: plan 024 decision")
	})

	t.Run("cancellation after promotion", func(t *testing.T) {
		tg := newPromoTarget(t, 100*1024, promoPayload(1024, 9), nil, nil)
		serverHook := newPromoHook()
		h := newLCHarness(t, toTarget(tg.ln.Addr().String()), func(c *WsMuxConfig) {
			c.StripeFactor = 2
			c.StripePorts = []string{"2"}
			c.PromoteBytes = 64 * 1024
		})
		h.s.logger.SetLevel(logrus.DebugLevel)
		h.s.logger.AddHook(serverHook)
		clientHook := newPromoHook()
		cancelClient := startPromoClient(t, h, 0, clientHook)

		user := h.user()
		_ = user.SetDeadline(time.Now().Add(3 * promoWait))
		if _, err := user.Write(promoPayload(100*1024, 10)); err != nil {
			t.Fatal(err)
		}
		promoRecv(t, tg.gotUp1, "the first upload chunk")
		promoRecv(t, serverHook.promoted, "the server's promotion")
		promoRecv(t, clientHook.promoted, "the client's promotion")
		close(tg.release)

		// Mid-flow, with the target still holding its side open, both ends are
		// cancelled: nothing may hang or keep the user connection alive.
		cancelClient()
		h.cancel()
		expectClosed(t, "the user connection after cancellation", user) // drains the staged download first
	})
}
