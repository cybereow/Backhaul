package transport

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/musix/backhaul/internal/utils"
	"github.com/musix/backhaul/internal/utils/striping"
	"github.com/xtaci/smux"
)

// setupHeaderWait is the client's initial-header deadline in these tests
// (WsMuxConfig.DialTimeOut), short so that a stalled header expires quickly.
const setupHeaderWait = 300 * time.Millisecond

// setupClient starts a client against the loopback tunnel server and returns the
// server's end of its one pool session, on which the tests open streams the way
// the real server does.
func setupClient(t *testing.T) (*WsMuxTransport, *smux.Session) {
	t.Helper()
	srv, sessions := muxServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := lifecycleMuxConfig(srv.addr(), 1)
	cfg.DialTimeOut = setupHeaderWait
	c := NewWSMuxClient(ctx, cfg, lifecycleLogger())
	c.Start()
	return c, nextSession(t, sessions)
}

// expectStreamEnds fails unless the client closes its end of st (or the read
// otherwise fails) within the deadline; a timeout means the client left it open.
func expectStreamEnds(t *testing.T, what string, st *smux.Stream) {
	t.Helper()
	_ = st.SetReadDeadline(time.Now().Add(lifecycleDeadline))
	_, err := st.Read(make([]byte, 1))
	if err == nil {
		t.Fatalf("%s: unexpectedly received data", what)
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatalf("%s: the client left the stream open for %s", what, lifecycleDeadline)
	}
}

// echoTarget is a local TCP service that echoes every connection.
func echoTarget(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				io.Copy(c, c)
			}()
		}
	}()
	return ln.Addr().String()
}

// openPlain opens a stream and sends a complete plain-flow header for target.
func openPlain(t *testing.T, sess *smux.Session, target string) *smux.Stream {
	t.Helper()
	st, err := sess.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := utils.SendFlowPlain(st, 0, target); err != nil {
		t.Fatal(err)
	}
	return st
}

// TestWSMuxSetupPartialHeader: a stream that never completes its initial header
// is closed when the header deadline passes, and the next valid stream is served.
// Header parsing is serial, so the valid one may wait up to that deadline behind
// the stalled one - bounded, not zero.
func TestWSMuxSetupPartialHeader(t *testing.T) {
	cases := []struct {
		name string
		head []byte
	}{
		{"no bytes", nil},
		{"partial plain", []byte{utils.FlowPlain, 0, 0, 0}},
		{"partial half-close plain", []byte{utils.FlowPlainHC, 0}},
		{"partial striped", []byte{utils.FlowStriped, 0, 0}},
		{"partial promote", []byte{utils.FlowPromote, 0}},
		{"partial udp", []byte{utils.FlowUDP}},
		{"partial speedtest", []byte{utils.FlowSpeedtest, 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, sess := setupClient(t)
			target := newHoldTarget(t)

			stalled, err := sess.OpenStream()
			if err != nil {
				t.Fatal(err)
			}
			if len(tc.head) > 0 {
				if _, err := stalled.Write(tc.head); err != nil {
					t.Fatal(err)
				}
			}
			openPlain(t, sess, target.addr())

			expectSignal(t, "the valid stream behind the stalled one to be served", target.accepted)
			expectStreamEnds(t, "the stream with the partial header", stalled)
		})
	}
}

// TestWSMuxSetupUnknownKind: a flow kind this client does not know closes the
// stream. It is not reinterpreted as the legacy header format (which would read
// the next bytes as an address and dial it).
func TestWSMuxSetupUnknownKind(t *testing.T) {
	for _, kind := range []byte{0x07, 0x7f, 0xff} {
		_, sess := setupClient(t)
		target := newHoldTarget(t)

		st, err := sess.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		// After the unknown kind byte come bytes the legacy parser would take as a
		// length-prefixed address to dial.
		addr := target.addr()
		msg := append([]byte{kind, byte(len(addr) >> 8), byte(len(addr))}, addr...)
		if _, err := st.Write(msg); err != nil {
			t.Fatal(err)
		}
		expectStreamEnds(t, "the stream with an unknown kind", st)
		if n := len(target.accepted); n != 0 {
			t.Fatalf("kind 0x%02x: the target was dialed: the unknown kind was parsed as a legacy header", kind)
		}

		openPlain(t, sess, target.addr())
		expectSignal(t, "a valid stream after the unknown kind", target.accepted)
	}
}

// TestWSMuxSetupClearsDeadline: the header deadline must not follow a stream into
// its data phase. A plain flow and the legs of a striped group stay quiet for
// longer than the deadline after their headers, then still carry data.
func TestWSMuxSetupClearsDeadline(t *testing.T) {
	quiet := func() { <-time.After(2 * setupHeaderWait) } // longer than the header deadline

	t.Run("plain", func(t *testing.T) {
		_, sess := setupClient(t)
		st := openPlain(t, sess, echoTarget(t))
		quiet()
		_ = st.SetDeadline(time.Now().Add(lifecycleDeadline))
		if _, err := st.Write([]byte("late")); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, 4)
		if _, err := io.ReadFull(st, got); err != nil || string(got) != "late" {
			t.Fatalf("echo after the quiet period: %q, %v", got, err)
		}
	})

	t.Run("striped", func(t *testing.T) {
		c, sess := setupClient(t)
		target := echoTarget(t)
		open := func() *smux.Stream {
			st, err := sess.OpenStream()
			if err != nil {
				t.Fatal(err)
			}
			return st
		}
		leg0 := open()
		if err := utils.SendFlowStriped(leg0, 9, 0, 2, 0, target); err != nil {
			t.Fatal(err)
		}
		waitFor(t, "the first leg to be filed", func() bool { return pendingStripeGroups(c) == 1 })
		quiet() // the pending leg outlives its header deadline; the group timer is separate
		leg1 := open()
		if err := utils.SendFlowStriped(leg1, 9, 1, 2, 0, target); err != nil {
			t.Fatal(err)
		}
		waitFor(t, "the group to be assembled", func() bool { return pendingStripeGroups(c) == 0 })

		conn := striping.New([]net.Conn{leg0, leg1}, striping.DefaultChunkSize)
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(lifecycleDeadline))
		if _, err := conn.Write([]byte("stripe")); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, 6)
		if _, err := io.ReadFull(conn, got); err != nil || string(got) != "stripe" {
			t.Fatalf("echo through the striped group: %q, %v", got, err)
		}
	})
}
