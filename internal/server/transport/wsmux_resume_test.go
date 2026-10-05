package transport

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/musix/backhaul/config"
	client_transport "github.com/musix/backhaul/internal/client/transport"
	"github.com/musix/backhaul/internal/smux"
	"github.com/sirupsen/logrus"
)

// startResumeClient runs the real wsmux client against h.
func startResumeClient(t *testing.T, h *lcHarness, pool int) {
	t.Helper()
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	client := client_transport.NewWSMuxClient(ctx, &client_transport.WsMuxConfig{
		RemoteAddr: h.addr, Token: lcToken, Mode: config.WSMUX, Path: "/", MuxVersion: 2,
		ConnPoolSize: pool, DialTimeOut: 5 * time.Second, RetryInterval: 20 * time.Millisecond,
		KeepAlive: 30 * time.Second, MaxFrameSize: 32768, MaxReceiveBuffer: 4194304,
		MaxStreamBuffer: 65536,
	}, logger)
	go client.Start()
	lcWaitFor(t, "the client's pool sessions", func() bool { return h.sessions() >= pool })
}

// echoTarget accepts one connection and copies it back, byte for byte, until the
// peer's EOF; then it ends its own direction.
func echoTarget(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		io.Copy(c, c)
		_ = c.(*net.TCPConn).CloseWrite()
	}()
	return ln
}

// retire does what rotation does to a session once its replacement is up: take
// it out of the leg registry, so nothing new lands on it.
func (h *lcHarness) retire(sess *smux.Session) {
	h.s.sessionsMu.Lock()
	h.s.unregisterLocked(sess)
	h.s.sessionsMu.Unlock()
}

// A resumable flow survives its session: it is moved to a stream on another
// session while traffic flows in both directions, again after that session is
// retired, and finishes with every byte intact in both directions even though
// the sessions it started on were closed.
func TestWSMuxResumableFlowMoves(t *testing.T) {
	t.Run("planned-only", func(t *testing.T) { testResumableFlowMoves(t, 0) })
	// The same, with resume support on: the swaps run over tunnels that carry ACKs
	// and keep replay rings.
	t.Run("with-replay", func(t *testing.T) { testResumableFlowMoves(t, 20*time.Second) })
}

func testResumableFlowMoves(t *testing.T, resumeWindow time.Duration) {
	tg := echoTarget(t)
	h := newLCHarness(t, toTarget(tg.Addr().String()), func(c *WsMuxConfig) {
		c.MaxConnAge = time.Hour // resumable flows on; natural rotation never fires here
		c.MaxDrain = time.Minute
		c.ResumeWindow = resumeWindow
	})
	startResumeClient(t, h, 3)

	user := h.user().(*net.TCPConn)
	_ = user.SetDeadline(time.Now().Add(30 * time.Second))

	payload := promoPayload(3*1024*1024+11, 9)
	sent := 0
	send := func(n int) []byte {
		t.Helper()
		chunk := payload[sent : sent+n]
		if _, err := user.Write(chunk); err != nil {
			t.Fatal(err)
		}
		sent += n
		return chunk
	}
	step := ""
	expect := func(want []byte) {
		t.Helper()
		got := make([]byte, len(want))
		if n, err := io.ReadFull(user, got); err != nil {
			t.Fatalf("reading the echo at %s: got %d of %d: %v", step, n, len(want), err)
		}
		if !bytes.Equal(got, want) {
			t.Fatal("echoed bytes differ")
		}
	}

	// Traffic before any move proves the flow is resumable and registered.
	step = "before any move"
	expect(send(100 * 1024))
	var f *resumableFlow
	lcWaitFor(t, "the flow to be registered", func() bool {
		h.s.flowsMu.Lock()
		defer h.s.flowsMu.Unlock()
		for _, x := range h.s.flows {
			f = x
		}
		return f != nil
	})

	for move := 1; move <= 2; move++ {
		from := f.session()
		if from == nil {
			t.Fatal("flow has no session")
		}
		// Data in flight while the move happens, in both directions.
		inflight := send(700 * 1024)
		step = fmt.Sprintf("move %d in-flight data", move)

		h.retire(from)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		h.s.migrateFlowsOff(ctx, from)
		cancel()
		if f.session() == from {
			t.Fatalf("move %d: the flow is still on the retired session", move)
		}
		expect(inflight)

		// The old session can now be cut without the flow noticing.
		from.Close()
		step = fmt.Sprintf("move %d after cutting the old session", move)
		expect(send(200 * 1024))
		lcWaitFor(t, "the old stream to be released", func() bool { return from.NumStreams() == 0 })
	}

	rest := send(len(payload) - sent)
	step = "the rest"
	expect(rest)

	// Both ends finish cleanly: the user's EOF reaches the target, whose own EOF
	// comes back through the (twice moved) flow.
	_ = user.CloseWrite()
	if _, err := user.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("after the user's EOF: %v, want io.EOF", err)
	}
}

// A client that does not know the flow refuses the attach; nothing freezes, the
// flow stays where it is and keeps working.
func TestWSMuxAttachRefusedLeavesFlowAlone(t *testing.T) {
	tg := echoTarget(t)
	h := newLCHarness(t, toTarget(tg.Addr().String()), func(c *WsMuxConfig) {
		c.MaxConnAge = time.Hour
		c.MaxDrain = time.Minute
	})
	startResumeClient(t, h, 3)

	user := h.user().(*net.TCPConn)
	_ = user.SetDeadline(time.Now().Add(20 * time.Second))
	msg := promoPayload(50000, 3)
	user.Write(msg)
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(user, got); err != nil || !bytes.Equal(got, msg) {
		t.Fatalf("echo before the attempt: %v", err)
	}

	var f *resumableFlow
	lcWaitFor(t, "the flow", func() bool {
		h.s.flowsMu.Lock()
		defer h.s.flowsMu.Unlock()
		for _, x := range h.s.flows {
			f = x
		}
		return f != nil
	})
	// Pretend the server tracks a flow the client has never heard of.
	ghost := &resumableFlow{id: newFlowID(), sw: f.sw, sess: []*smux.Session{f.session()}}
	err := h.s.migrateFlow(context.Background(), ghost, ghost.session())
	if err == nil {
		t.Fatal("an attach for an unknown flow was accepted")
	}
	if f.session() != ghost.session() {
		t.Fatal("a refused attach changed the flow's session")
	}

	more := promoPayload(80000, 4)
	user.Write(more)
	got = make([]byte, len(more))
	if _, err := io.ReadFull(user, got); err != nil || !bytes.Equal(got, more) {
		t.Fatalf("the flow broke after a refused attach: %v", err)
	}
}

// A flow whose session is cut WITHOUT warning (no retirement, no migration)
// is resumed on another session: every byte of a long echo arrives once and in
// order across repeated cuts, under traffic in both directions.
func TestWSMuxResumeAfterUnannouncedCut(t *testing.T) {
	tg := echoTarget(t)
	h := newLCHarness(t, toTarget(tg.Addr().String()), func(c *WsMuxConfig) {
		c.MaxConnAge = time.Hour // resumable flows on; no rotation fires during the test
		c.MaxDrain = time.Minute
		c.ResumeWindow = 20 * time.Second
	})
	startResumeClient(t, h, 4)

	user := h.user().(*net.TCPConn)
	_ = user.SetDeadline(time.Now().Add(60 * time.Second))

	payload := promoPayload(6*1024*1024+13, 21)
	var sent, recvd int
	readerDone := make(chan error, 1)
	go func() {
		got := make([]byte, 64<<10)
		for recvd < len(payload) {
			n, err := user.Read(got)
			if n > 0 {
				if !bytes.Equal(got[:n], payload[recvd:recvd+n]) {
					readerDone <- fmt.Errorf("echoed bytes differ at offset %d", recvd)
					return
				}
				recvd += n
			}
			if err != nil {
				readerDone <- fmt.Errorf("reading the echo at %d of %d: %w", recvd, len(payload), err)
				return
			}
		}
		readerDone <- nil
	}()

	var f *resumableFlow
	cuts := 0
	for sent < len(payload) {
		n := min(256<<10, len(payload)-sent)
		if _, err := user.Write(payload[sent : sent+n]); err != nil {
			t.Fatalf("writing at %d: %v (after %d cuts)", sent, err, cuts)
		}
		sent += n
		if f == nil {
			h.s.flowsMu.Lock()
			for _, x := range h.s.flows {
				f = x
			}
			h.s.flowsMu.Unlock()
		}
		if f != nil && cuts < 3 && sent > (cuts+1)*(1<<20) {
			from := f.session()
			from.Close() // the CDN's cut: nothing announced, nothing migrated
			cuts++
			lcWaitFor(t, "the flow to resume on another session", func() bool {
				return f.sw.Resumes() >= uint64(cuts) && f.session() != from
			})
		}
	}
	_ = user.CloseWrite()
	if err := <-readerDone; err != nil {
		t.Fatal(err)
	}
	if cuts == 0 || f.sw.Resumes() < uint64(cuts) {
		t.Fatalf("cuts=%d resumes=%d", cuts, f.sw.Resumes())
	}
}

// A flow whose download direction already ended and whose upload direction is
// idle in a read of the user's socket performs no tunnel I/O, so no pump can notice
// its session died: only the server watching the session starts the resume. It
// must happen before the user sends anything, and the request sent after still
// arrives.
func TestWSMuxResumeIdleHalfClosedFlow(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	got := make(chan string, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		_ = c.(*net.TCPConn).CloseWrite() // nothing to say: the reply direction ends at once
		b, _ := io.ReadAll(c)
		got <- string(b)
	}()

	h := newLCHarness(t, toTarget(ln.Addr().String()), func(c *WsMuxConfig) {
		c.MaxConnAge = time.Hour
		c.MaxDrain = time.Minute
		c.ResumeWindow = 20 * time.Second
	})
	startResumeClient(t, h, 3)

	user := h.user().(*net.TCPConn)
	_ = user.SetDeadline(time.Now().Add(30 * time.Second))

	var f *resumableFlow
	lcWaitFor(t, "the flow", func() bool {
		h.s.flowsMu.Lock()
		defer h.s.flowsMu.Unlock()
		for _, x := range h.s.flows {
			f = x
		}
		return f != nil
	})
	time.Sleep(300 * time.Millisecond) // let the END reach the server
	from := f.session()
	from.Close()
	lcWaitFor(t, "the idle flow to resume without any traffic", func() bool { return f.sw.Resumes() >= 1 && f.session() != from })

	user.Write([]byte("request after the cut"))
	user.CloseWrite()
	select {
	case s := <-got:
		if s != "request after the cut" {
			t.Fatalf("the target received %q", s)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the request after the cut never arrived")
	}
}

// --- promotable flows with rotation on -------------------------------------------

// promotableHarness is a server with rotation on (natural rotation never fires
// here) and promotion after promoteAt bytes, the real client striping like it, and
// a pool wide enough to rebuild a group while another is still up.
func promotableHarness(t *testing.T, target string, parity int, resume time.Duration, promoteAt uint64) *lcHarness {
	t.Helper()
	h := newLCHarness(t, toTarget(target), func(c *WsMuxConfig) {
		c.MaxConnAge = time.Hour
		c.MaxDrain = time.Minute
		c.ResumeWindow = resume
		c.StripeFactor = 2
		c.StripeParity = parity
		c.StripePorts = []string{"2"} // the destination is not listed: starts plain
		c.PromoteBytes = promoteAt
	})
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	if os.Getenv("BH_TEST_LOG") != "" {
		logger.SetOutput(os.Stderr)
		logger.SetLevel(logrus.DebugLevel)
		h.s.logger.SetOutput(os.Stderr)
		h.s.logger.SetLevel(logrus.DebugLevel)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	client := client_transport.NewWSMuxClient(ctx, &client_transport.WsMuxConfig{
		RemoteAddr: h.addr, Token: lcToken, Mode: config.WSMUX, Path: "/", MuxVersion: 2,
		ConnPoolSize: 8, DialTimeOut: 5 * time.Second, RetryInterval: 20 * time.Millisecond,
		KeepAlive: 30 * time.Second, MaxFrameSize: 32768, MaxReceiveBuffer: 4194304,
		MaxStreamBuffer: 65536, StripeFactor: 2, StripeParity: parity,
	}, logger)
	go client.Start()
	lcWaitFor(t, "the whole pool", func() bool { return h.sessions() >= 8 })
	if resume > 0 {
		// Whether a promotable flow gets replay state depends on the client's
		// answer to the probe of the session it opens on: wait for them all.
		lcWaitFor(t, "every session's replay probe", func() bool {
			h.s.sessionsMu.Lock()
			defer h.s.sessionsMu.Unlock()
			for _, ps := range h.s.sessions {
				if !ps.replayPromote.Load() {
					return false
				}
			}
			return true
		})
	}
	return h
}

// echoUser drives one user connection to an echo target with a known payload.
type echoUser struct {
	t       *testing.T
	conn    *net.TCPConn
	payload []byte
	sent    int
}

func newEchoUser(t *testing.T, h *lcHarness, total int) *echoUser {
	t.Helper()
	conn := h.user().(*net.TCPConn)
	_ = conn.SetDeadline(time.Now().Add(90 * time.Second))
	return &echoUser{t: t, conn: conn, payload: promoPayload(total, 11)}
}

// start sends the next n bytes and reads their echo; the function it returns
// waits for that and checks every byte.
func (u *echoUser) start(step string, n int) (wait func()) {
	chunk := u.payload[u.sent : u.sent+n]
	u.sent += n
	go u.conn.Write(chunk)
	done := make(chan error, 1)
	go func() {
		got := make([]byte, n)
		k, err := io.ReadFull(u.conn, got)
		if err != nil {
			err = fmt.Errorf("got %d of %d: %v", k, n, err)
		} else if !bytes.Equal(got, chunk) {
			err = fmt.Errorf("echoed bytes differ")
		}
		done <- err
	}()
	return func() {
		u.t.Helper()
		if err := <-done; err != nil {
			u.t.Fatalf("%s: %v", step, err)
		}
	}
}

func (u *echoUser) echo(step string, n int) {
	u.t.Helper()
	u.start(step, n)()
}

// finish echoes what is left of the payload and ends both directions cleanly.
func (u *echoUser) finish() {
	u.t.Helper()
	u.echo("the rest", len(u.payload)-u.sent)
	_ = u.conn.CloseWrite()
	if _, err := u.conn.Read(make([]byte, 1)); err != io.EOF {
		u.t.Fatalf("after the user's EOF: %v, want io.EOF", err)
	}
}

// soleFlow is the one resumable flow the server is running.
func soleFlow(t *testing.T, h *lcHarness) *resumableFlow {
	t.Helper()
	var f *resumableFlow
	lcWaitFor(t, "the flow to be registered", func() bool {
		h.s.flowsMu.Lock()
		defer h.s.flowsMu.Unlock()
		for _, x := range h.s.flows {
			f = x
		}
		return f != nil
	})
	return f
}

func (f *resumableFlow) sessions() []*smux.Session {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*smux.Session(nil), f.sess...)
}

// With rotation on, a promotable flow is not cut when a session under it is
// retired: before promotion it is moved to a stream on another session, and once
// promoted its whole striped group is rebuilt on the sessions that stay. Every
// byte arrives, in both directions, although every session the flow was ever on
// has been closed by then. With a resume window the flow keeps replay state until
// it is promoted, and gives it up there.
func TestWSMuxPromotableFlowSurvivesRotation(t *testing.T) {
	for _, tc := range []struct {
		parity int
		resume time.Duration
	}{{0, 0}, {1, 0}, {0, 20 * time.Second}, {1, 20 * time.Second}} {
		tc := tc
		t.Run(fmt.Sprintf("parity=%d/resume=%s", tc.parity, tc.resume), func(t *testing.T) {
			tg := echoTarget(t)
			h := promotableHarness(t, tg.Addr().String(), tc.parity, tc.resume, 1024*1024)
			const inFlight = 48 * 1024 * 1024 // what is on its way while a promoted flow is moved
			u := newEchoUser(t, h, 2*inFlight+4*1024*1024+7)

			// retireAll retires every session the flow is on now, as rotation does:
			// move the flows off, then close the session once it carries no stream
			// (the flow's old tunnel keeps its streams until both ends are done with it).
			retireAll := func(step string) {
				t.Helper()
				f := soleFlow(t, h)
				on := f.sessions()
				for _, sess := range on {
					h.retire(sess)
					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
					h.s.migrateFlowsOff(ctx, sess)
					cancel()
					if f.on(sess) {
						t.Fatalf("%s: the flow is still on a retired session", step)
					}
				}
				for _, sess := range on {
					lcWaitFor(t, step+": the retired session to drain", func() bool { return sess.NumStreams() == 0 })
					sess.Close()
				}
			}

			u.echo("before any move", 100*1024)
			f := soleFlow(t, h)
			if got, want := f.sw.Replaying(), tc.resume > 0; got != want {
				t.Fatalf("replay state before promotion: %v, want %v", got, want)
			}
			retireAll("plain")
			if f.isStriped() {
				t.Fatal("the flow was promoted before promote_bytes")
			}
			u.echo("after the plain move", 200*1024)

			u.echo("past promote_bytes", 2*1024*1024)
			lcWaitFor(t, "the promotion", f.isStriped)
			if f.sw.Replaying() {
				t.Fatal("the flow still keeps replay state on a striped group")
			}
			for round := 1; round <= 2; round++ {
				step := fmt.Sprintf("striped move %d", round)
				// The moves happen under load: each old group still has megabytes
				// on their way, in both directions, when the flow is taken off it.
				wait := u.start(step, inFlight)
				retireAll(step)
				if !f.isStriped() {
					t.Fatalf("%s: the flow is no longer striped", step)
				}
				wait()
			}
			u.finish()
		})
	}
}

// A promotable flow also survives a session that is cut under it without warning:
// before promotion it is resumed on another session from its replay state, and
// once promoted a group with parity to spare loses the leg, carries on, and is
// rebuilt on a full set of legs at once, so that the next cut finds it whole again.
// Two parity legs: with one, a row on its way when the leg is lost may already
// have had its only spare shard skipped for a congested leg (see FECConn.flushRow).
func TestWSMuxPromotableFlowSurvivesCuts(t *testing.T) {
	tg := echoTarget(t)
	h := promotableHarness(t, tg.Addr().String(), 2, 20*time.Second, 4*1024*1024)
	const inFlight = 32 * 1024 * 1024
	u := newEchoUser(t, h, 3*inFlight+8*1024*1024+5)

	u.echo("before any cut", 100*1024)
	f := soleFlow(t, h)
	if !f.sw.Replaying() {
		t.Fatal("the flow has no replay state before promotion")
	}

	// Not promoted yet: its session goes, with data on the way.
	wait := u.start("cut before promotion", 2*1024*1024)
	dead := f.session()
	dead.Close()
	wait()
	lcWaitFor(t, "the flow to resume on another session", func() bool { return f.sw.Resumes() >= 1 && !f.on(dead) })
	if f.isStriped() {
		t.Fatal("the flow was promoted before promote_bytes")
	}

	u.echo("past promote_bytes", 4*1024*1024)
	lcWaitFor(t, "the promotion", f.isStriped)

	for round := 1; round <= 3; round++ {
		step := fmt.Sprintf("cut of a leg %d", round)
		wait := u.start(step, inFlight)
		dead := f.session()
		dead.Close()
		wait()
		lcWaitFor(t, step+": the group to be rebuilt off the lost session", func() bool { return !f.on(dead) && f.sw.Swappable() })
		if n := len(f.sessions()); n != 4 {
			t.Fatalf("%s: the flow is on %d sessions, want a full group of 4", step, n)
		}
	}
	u.finish()
}
