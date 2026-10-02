package handlers

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"
)

// newReplayFlowEnd is newFlowEnd with replay enabled on an enveloped tunnel.
func newReplayFlowEnd(t *testing.T, ctx context.Context, tunnel net.Conn, limit int) *flowEnd {
	t.Helper()
	u, appc := tcpConnPair(t)
	p := NewPromotablePump(ctx, false, appc, tunnel, testLogger(), testUsage(t), 0, false)
	if p == nil {
		t.Fatal("NewPromotablePump returned nil")
	}
	if err := p.EnableReplay(limit); err != nil {
		t.Fatal(err)
	}
	p.Start()
	return &flowEnd{user: u, pump: p}
}

func waitReplayEmpty(t *testing.T, p *PumpSwapper, what string) {
	t.Helper()
	deadline := time.Now().Add(hcTestTimeout)
	for p.ReplayLen() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%s: %d bytes still unacknowledged", what, p.ReplayLen())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Delivered bytes are acknowledged, so the sender's replay ring drains instead of
// growing with the flow.
func TestReplayAcksTrimTheRing(t *testing.T) {
	ctx, _ := testCtx(t)
	a0, b0 := tunnelPair(t, true)
	A, B := newReplayFlowEnd(t, ctx, a0, 256<<10), newReplayFlowEnd(t, ctx, b0, 256<<10)

	up := genPayload(900000)
	go A.user.Write(up)
	if got := readExact(t, B.user, len(up)); !bytes.Equal(got, up) {
		t.Fatal("payload differs")
	}
	waitReplayEmpty(t, A.pump, "A after B delivered everything")
}

// A sender whose bytes are not acknowledged stops at the replay limit instead of
// queueing without bound; each ACK lets it send that much more.
func TestReplayBackpressure(t *testing.T) {
	const limit = 256 << 10
	ctx, _ := testCtx(t)
	a0, b0 := tunnelPair(t, true)
	// B has no replay, so it never acknowledges on its own: the test acknowledges
	// by hand and so controls exactly how much A may have in flight.
	A := newReplayFlowEnd(t, ctx, a0, limit)
	B := newFlowEnd(t, ctx, b0)

	up := genPayload(5 * limit)
	go A.user.Write(up)

	waitUp := func(want uint64) {
		t.Helper()
		deadline := time.Now().Add(hcTestTimeout)
		for A.pump.UpBytes() < want {
			if time.Now().After(deadline) {
				t.Fatalf("sender stalled at %d bytes, want %d", A.pump.UpBytes(), want)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}
	// The sender waits for room for a whole read (up to 64 KiB), so it stops
	// somewhere in the last read-size below the limit.
	const slack = 64 << 10
	waitUp(limit - slack)
	time.Sleep(200 * time.Millisecond) // "must not move" assertion: waiting is the check
	if got := A.pump.UpBytes(); got > limit {
		t.Fatalf("sender passed the replay limit: %d bytes sent unacknowledged, limit %d", got, limit)
	}
	if A.pump.ReplayLen() > limit {
		t.Fatalf("ring holds %d bytes, limit %d", A.pump.ReplayLen(), limit)
	}

	for i := 1; i <= 4; i++ {
		// Acknowledge everything sent so far (an ACK beyond what was sent is
		// ignored), which frees the whole ring again.
		sent := A.pump.UpBytes()
		if err := b0.(ackConn).SendAck(sent); err != nil {
			t.Fatal(err)
		}
		waitUp(sent + limit - slack)
	}
	// From here acknowledge continuously, and everything must arrive intact at B.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(10 * time.Millisecond):
				_ = b0.(ackConn).SendAck(A.pump.UpBytes())
			}
		}
	}()
	if got := readExact(t, B.user, len(up)); !bytes.Equal(got, up) {
		t.Fatal("payload differs after backpressure")
	}
}

// A flow whose upload ended must still deliver acks for the download, and its
// peer must still read them: otherwise the replying side fills its ring and
// deadlocks on a flow that is perfectly healthy (a command's output after the
// request's EOF).
func TestReplayHalfClosedFlowDoesNotDeadlock(t *testing.T) {
	const limit = 256 << 10
	ctx, _ := testCtx(t)
	a0, b0 := tunnelPair(t, true)
	A, B := newReplayFlowEnd(t, ctx, a0, limit), newReplayFlowEnd(t, ctx, b0, limit)

	A.user.Write([]byte("request"))
	A.user.CloseWrite() // the request is over
	readExact(t, B.user, len("request"))

	reply := genPayload(12 * limit) // far more than the ring holds
	go func() {
		B.user.Write(reply)
		B.user.CloseWrite()
	}()
	got, err := readAllDeadline(A.user)
	if err != nil || !bytes.Equal(got, reply) {
		t.Fatalf("reply: %d bytes err=%v, want %d", len(got), err, len(reply))
	}
	waitDone(t, A.pump, "A")
	waitDone(t, B.pump, "B")
}

// ACKs keep flowing across swaps, on a live flow: after each swap the new tunnel
// carries the acknowledgements for everything delivered, so the rings drain again.
// Run both with traffic in both directions and with a half-closed flow, where one
// side has no reader on the new tunnel and must serve its ACKs separately.
func TestReplayAcksSurviveSwaps(t *testing.T) {
	for _, halfClosed := range []bool{false, true} {
		halfClosed := halfClosed
		name := "both directions"
		if halfClosed {
			name = "upload ended"
		}
		t.Run(name, func(t *testing.T) {
			ctx, _ := testCtx(t)
			a0, b0 := tunnelPair(t, true)
			A, B := newReplayFlowEnd(t, ctx, a0, 256<<10), newReplayFlowEnd(t, ctx, b0, 256<<10)

			if halfClosed {
				A.user.Write([]byte("request"))
				A.user.CloseWrite()
				readExact(t, B.user, len("request"))
			}

			for i := 1; i <= 3; i++ {
				dn := genPayload(150000 + i)
				B.user.Write(dn)
				readExact(t, A.user, len(dn))
				if !halfClosed {
					up := genPayload(90000 + i)
					A.user.Write(up)
					readExact(t, B.user, len(up))
				}
				waitReplayEmpty(t, B.pump, "B before the swap")
				waitReplayEmpty(t, A.pump, "A before the swap")

				if err := swapTunnelWith(t, ctx, A.pump, B.pump, true); err != nil {
					t.Fatalf("swap %d: %v", i, err)
				}
				waitSwaps(t, A.pump, uint64(i))
				waitSwaps(t, B.pump, uint64(i))

				// New traffic after the swap is acknowledged on the NEW tunnel.
				dn2 := genPayload(60000 + i)
				B.user.Write(dn2)
				readExact(t, A.user, len(dn2))
				waitReplayEmpty(t, B.pump, "B after the swap")
			}
		})
	}
}
