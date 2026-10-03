package transport

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
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
	ghost := &resumableFlow{id: newFlowID(), sw: f.sw, sess: f.session()}
	err := h.s.migrateFlow(context.Background(), ghost)
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
