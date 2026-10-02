package handlers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"
)

// These tests drive two PumpSwappers back to back, the way the two ends of a
// tunnel run them, and swap the tunnel under live traffic more than once.

// flowEnd is one end of a flow: the user socket driving it and its pump.
type flowEnd struct {
	user *net.TCPConn
	pump *PumpSwapper
}

func newFlowEnd(t *testing.T, ctx context.Context, tunnel net.Conn) *flowEnd {
	t.Helper()
	u, appc := tcpConnPair(t)
	p := PromotablePump(ctx, false, appc, tunnel, testLogger(), testUsage(t), 0, false)
	if p == nil {
		t.Fatal("PromotablePump returned nil")
	}
	return &flowEnd{user: u, pump: p}
}

// swapTunnel runs one whole two-sided swap: both ends freeze, exchange their
// counts (here directly, in production over the new leg) and install a fresh
// tunnel.
func swapTunnel(t *testing.T, ctx context.Context, a, b *PumpSwapper) error {
	t.Helper()
	na, nb := tcpConnPair(t)

	var ownA, ownB uint64
	var errA, errB error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); ownA, errA = a.FreezeUp(ctx) }()
	go func() { defer wg.Done(); ownB, errB = b.FreezeUp(ctx) }()
	wg.Wait()
	if errA != nil || errB != nil {
		return fmt.Errorf("freeze: a=%v b=%v", errA, errB)
	}
	if err := a.Install(na, ownB); err != nil {
		return fmt.Errorf("install a: %w", err)
	}
	if err := b.Install(nb, ownA); err != nil {
		return fmt.Errorf("install b: %w", err)
	}
	return nil
}

func waitSwaps(t *testing.T, p *PumpSwapper, n uint64) {
	t.Helper()
	deadline := time.Now().Add(hcTestTimeout)
	for p.Swaps() < n {
		if time.Now().After(deadline) {
			t.Fatalf("swap %d never completed (have %d)", n, p.Swaps())
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// Two swaps in a row under traffic in both directions: every byte arrives once
// and in order, the offsets count from the start of the flow, and each replaced
// tunnel is released.
func TestPumpSwapperTwoSwaps(t *testing.T) {
	ctx, _ := testCtx(t)
	a0, b0 := tcpConnPair(t)
	A, B := newFlowEnd(t, ctx, a0), newFlowEnd(t, ctx, b0)

	up, dn := genPayload(40000), genPayload(30000)
	A.user.Write(up[:10000])
	B.user.Write(dn[:7000])
	got := readExact(t, B.user, 10000)
	if !bytes.Equal(got, up[:10000]) {
		t.Fatal("first segment differs")
	}
	readExact(t, A.user, 7000)

	if err := swapTunnel(t, ctx, A.pump, B.pump); err != nil {
		t.Fatal(err)
	}
	waitSwaps(t, A.pump, 1)
	waitSwaps(t, B.pump, 1)
	expectEOF(t, a0, "first tunnel released")

	A.user.Write(up[10000:25000])
	B.user.Write(dn[7000:20000])
	readExact(t, B.user, 15000)
	readExact(t, A.user, 13000)

	if err := swapTunnel(t, ctx, A.pump, B.pump); err != nil {
		t.Fatal(err)
	}
	waitSwaps(t, A.pump, 2)
	waitSwaps(t, B.pump, 2)

	A.user.Write(up[25000:])
	B.user.Write(dn[20000:])
	A.user.CloseWrite()
	B.user.CloseWrite()
	gotB, err := readAllDeadline(B.user)
	if err != nil || !bytes.Equal(gotB, up[25000:]) {
		t.Fatalf("B user: %d bytes err=%v", len(gotB), err)
	}
	gotA, err := readAllDeadline(A.user)
	if err != nil || !bytes.Equal(gotA, dn[20000:]) {
		t.Fatalf("A user: %d bytes err=%v", len(gotA), err)
	}
	waitDone(t, A.pump, "A")
	waitDone(t, B.pump, "B")
	if A.pump.UpBytes() != uint64(len(up)) || A.pump.DlBytes() != uint64(len(dn)) ||
		B.pump.UpBytes() != uint64(len(dn)) || B.pump.DlBytes() != uint64(len(up)) {
		t.Fatalf("offsets not counted from the start of the flow: A up=%d dl=%d B up=%d dl=%d",
			A.pump.UpBytes(), A.pump.DlBytes(), B.pump.UpBytes(), B.pump.DlBytes())
	}
}

// A second freeze while a swap is still in progress is refused, and works again
// once that swap has completed.
func TestPumpSwapperFreezeRefusedWhileSwapping(t *testing.T) {
	ctx, _ := testCtx(t)
	a, _ := streamPair(t)
	s := startSide(t, ctx, a)

	if r := waitFreeze(t, startFreeze(s.pump, ctx)); r.err != nil {
		t.Fatal(r.err)
	}
	if _, err := s.pump.FreezeUp(ctx); !errors.Is(err, ErrPromoteUnavailable) {
		t.Fatalf("second FreezeUp during a swap = %v, want ErrPromoteUnavailable", err)
	}

	newMine, newPeer := tcpConnPair(t)
	if err := installTunnel(s.pump, newMine, 0); err != nil {
		t.Fatal(err)
	}
	if err := installTunnel(s.pump, newMine, 0); !errors.Is(err, ErrPromoteUnavailable) {
		t.Fatalf("second Install = %v, want ErrPromoteUnavailable", err)
	}
	_ = newPeer
	waitSwaps(t, s.pump, 1)

	if r := waitFreeze(t, startFreeze(s.pump, ctx)); r.err != nil {
		t.Fatalf("FreezeUp after the swap completed: %v", r.err)
	}
}

// A flow whose upload has already ended can still be moved: the end is
// re-sent on the new tunnel, and the other direction keeps running across it.
func TestPumpSwapperMovesHalfClosedFlow(t *testing.T) {
	ctx, _ := testCtx(t)
	a0, b0 := tcpConnPair(t)
	A, B := newFlowEnd(t, ctx, a0), newFlowEnd(t, ctx, b0)

	req := genPayload(5000)
	A.user.Write(req)
	A.user.CloseWrite() // the request is over; the reply is still to come
	if got := readExact(t, B.user, len(req)); !bytes.Equal(got, req) {
		t.Fatal("request differs")
	}
	expectEOF(t, B.user, "B user must see the request's end")

	if err := swapTunnel(t, ctx, A.pump, B.pump); err != nil {
		t.Fatalf("a half-closed flow could not be moved: %v", err)
	}
	waitSwaps(t, A.pump, 1)
	waitSwaps(t, B.pump, 1)

	reply := genPayload(60000)
	B.user.Write(reply[:30000])
	if got := readExact(t, A.user, 30000); !bytes.Equal(got, reply[:30000]) {
		t.Fatal("reply after the move differs")
	}
	// And once more, with the request side still ended.
	if err := swapTunnel(t, ctx, A.pump, B.pump); err != nil {
		t.Fatalf("second move of a half-closed flow: %v", err)
	}
	waitSwaps(t, A.pump, 2)
	B.user.Write(reply[30000:])
	B.user.CloseWrite()
	rest, err := readAllDeadline(A.user)
	if err != nil || !bytes.Equal(rest, reply[30000:]) {
		t.Fatalf("A user: %d bytes err=%v", len(rest), err)
	}
	waitDone(t, A.pump, "A")
	waitDone(t, B.pump, "B")
}

// Random flows with random sizes, pacing, half-closes and 1-6 swaps: no byte may
// be lost, duplicated or reordered, in either direction, whatever the
// interleaving. Seeds make a failure reproducible.
func TestPumpSwapperRandomSwaps(t *testing.T) {
	seeds := 30
	if testing.Short() {
		seeds = 6
	}
	for seed := int64(1); seed <= int64(seeds); seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			t.Parallel()
			runRandomSwaps(t, seed)
		})
	}
}

func randomBytes(rng *rand.Rand, n int) []byte {
	b := make([]byte, n)
	rng.Read(b)
	return b
}

// paced writes data in random chunks with occasional pauses.
func paced(w io.Writer, data []byte, rng *rand.Rand) error {
	for len(data) > 0 {
		n := 1 + rng.Intn(20000)
		if n > len(data) {
			n = len(data)
		}
		if _, err := w.Write(data[:n]); err != nil {
			return err
		}
		data = data[n:]
		if rng.Intn(4) == 0 {
			time.Sleep(time.Duration(rng.Intn(3)) * time.Millisecond)
		}
	}
	return nil
}

func runRandomSwaps(t *testing.T, seed int64) {
	rng := rand.New(rand.NewSource(seed))
	ctx, _ := testCtx(t)
	a0, b0 := tcpConnPair(t)
	A, B := newFlowEnd(t, ctx, a0), newFlowEnd(t, ctx, b0)

	nSwaps := 1 + rng.Intn(6)
	toB := randomBytes(rng, rng.Intn(400000)) // may be empty: an immediate half-close
	toA := randomBytes(rng, 50000+rng.Intn(400000))
	rngA, rngB := rand.New(rand.NewSource(seed*2+1)), rand.New(rand.NewSource(seed*2+2))
	swapsDone := make(chan struct{})

	type result struct {
		data []byte
		err  error
	}
	resA, resB := make(chan result, 1), make(chan result, 1)
	read := func(c net.Conn, out chan<- result) {
		d, err := readAllDeadline(c)
		out <- result{d, err}
	}
	go read(A.user, resA)
	go read(B.user, resB)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { // A's request: ends on its own, possibly before any swap
		defer wg.Done()
		if err := paced(A.user, toB, rngA); err != nil {
			t.Errorf("A write: %v", err)
		}
		A.user.CloseWrite()
	}()
	go func() { // B's reply: ends only after the last swap
		defer wg.Done()
		if err := paced(B.user, toA, rngB); err != nil {
			t.Errorf("B write: %v", err)
		}
		<-swapsDone
		B.user.CloseWrite()
	}()

	for i := 0; i < nSwaps; i++ {
		time.Sleep(time.Duration(rng.Intn(6)) * time.Millisecond)
		if err := swapTunnel(t, ctx, A.pump, B.pump); err != nil {
			close(swapsDone)
			t.Fatalf("swap %d/%d: %v", i+1, nSwaps, err)
		}
		waitSwaps(t, A.pump, uint64(i+1))
		waitSwaps(t, B.pump, uint64(i+1))
	}
	close(swapsDone)
	wg.Wait()

	gotA, gotB := <-resA, <-resB
	if gotB.err != nil || !bytes.Equal(gotB.data, toB) {
		t.Fatalf("A->B: got %d bytes err=%v, want %d", len(gotB.data), gotB.err, len(toB))
	}
	if gotA.err != nil || !bytes.Equal(gotA.data, toA) {
		t.Fatalf("B->A: got %d bytes err=%v, want %d", len(gotA.data), gotA.err, len(toA))
	}
	waitDone(t, A.pump, "A")
	waitDone(t, B.pump, "B")
	if A.pump.UpBytes() != uint64(len(toB)) || B.pump.DlBytes() != uint64(len(toB)) ||
		B.pump.UpBytes() != uint64(len(toA)) || A.pump.DlBytes() != uint64(len(toA)) {
		t.Fatalf("offsets: A up=%d dl=%d B up=%d dl=%d", A.pump.UpBytes(), A.pump.DlBytes(), B.pump.UpBytes(), B.pump.DlBytes())
	}
	if A.pump.Swaps() != uint64(nSwaps) || B.pump.Swaps() != uint64(nSwaps) {
		t.Fatalf("swaps: A=%d B=%d want %d", A.pump.Swaps(), B.pump.Swaps(), nSwaps)
	}
}
