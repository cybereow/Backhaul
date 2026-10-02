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
	"github.com/sirupsen/logrus"
	"github.com/xtaci/smux"
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
	tg := echoTarget(t)
	h := newLCHarness(t, toTarget(tg.Addr().String()), func(c *WsMuxConfig) {
		c.MaxConnAge = time.Hour // resumable flows on; natural rotation never fires here
		c.MaxDrain = time.Minute
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
