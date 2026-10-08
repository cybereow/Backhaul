package handlers

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"
)

// rawTunnel is a TCP connection pair whose ends the test can cut the way a CDN
// does: abruptly, with unread bytes lost and no END or ABORT sent.
type rawTunnel struct{ a, b *net.TCPConn }

func newRawTunnel(t *testing.T) (*rawTunnel, net.Conn, net.Conn) {
	t.Helper()
	a, b := tcpConnPair(t)
	return &rawTunnel{a, b}, NewHalfCloseConn(a), NewHalfCloseConn(b)
}

func (r *rawTunnel) cut() {
	r.a.SetLinger(0)
	r.b.SetLinger(0)
	r.a.Close()
	r.b.Close()
}

// resumeBoth resumes both ends of a suspended flow onto a fresh tunnel, the way
// the owners of the two ends do over a new stream: each runs the handshake on its
// side of the raw leg.
func resumeBoth(t *testing.T, ctx context.Context, a, b *PumpSwapper) *rawTunnel {
	t.Helper()
	// The owner learns of the cut: no need to wait for the pumps to notice. A side
	// that is already settled (it has everything and the peer acknowledged it all)
	// does not suspend; the other then has nothing left to resume for either.
	okA, okB := a.Suspend(), b.Suspend()
	if !okA || !okB {
		return nil
	}
	rt, _, _ := newRawTunnel(t)
	var wg sync.WaitGroup
	var errA, errB error
	wg.Add(2)
	go func() {
		defer wg.Done()
		errA = a.Resume(ctx, rt.a, func() (net.Conn, error) { return NewHalfCloseConn(rt.a), nil })
	}()
	go func() {
		defer wg.Done()
		errB = b.Resume(ctx, rt.b, func() (net.Conn, error) { return NewHalfCloseConn(rt.b), nil })
	}()
	wg.Wait()
	if errA != nil || errB != nil {
		t.Fatalf("resume: %v / %v", errA, errB)
	}
	return rt
}

func newResumableEnd(t *testing.T, ctx context.Context, tunnel net.Conn) *flowEnd {
	t.Helper()
	return newReplayFlowEnd(t, ctx, tunnel, 256<<10)
}

// A flow whose tunnel is cut without warning, under traffic in both directions,
// resumes on a new tunnel with every byte delivered once and in order. The cuts
// land at random moments, including while a direction has already ended.
func TestResumeAfterCuts(t *testing.T) {
	for seed := int64(1); seed <= 12; seed++ {
		seed := seed
		t.Run("", func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			ctx, _ := testCtx(t)
			rt, a0, b0 := newRawTunnel(t)
			A, B := newResumableEnd(t, ctx, a0), newResumableEnd(t, ctx, b0)

			up := make([]byte, 600_000+rng.Intn(900_000))
			dn := make([]byte, 400_000+rng.Intn(900_000))
			rng.Read(up)
			rng.Read(dn)

			write := func(c *net.TCPConn, data []byte, seed int64) {
				r := rand.New(rand.NewSource(seed))
				for off := 0; off < len(data); {
					n := min(1+r.Intn(60_000), len(data)-off)
					if _, err := c.Write(data[off : off+n]); err != nil {
						return
					}
					off += n
					if r.Intn(8) == 0 {
						time.Sleep(time.Duration(r.Intn(3)) * time.Millisecond)
					}
				}
				c.CloseWrite()
			}
			go write(A.user, up, seed*2)
			go write(B.user, dn, seed*2+1)

			type result struct {
				got []byte
				err error
			}
			read := func(c *net.TCPConn) <-chan result {
				ch := make(chan result, 1)
				go func() {
					c.SetReadDeadline(time.Now().Add(4 * hcTestTimeout))
					b, err := io.ReadAll(c)
					ch <- result{b, err}
				}()
				return ch
			}
			gotUp, gotDn := read(B.user), read(A.user)

			// Cut at random moments until the data is through.
			cuts := 0
		cutting:
			for {
				select {
				case <-time.After(time.Duration(1+rng.Intn(12)) * time.Millisecond):
				case <-ctx.Done():
					break cutting
				}
				select {
				case <-A.pump.DoneWait():
					break cutting
				case <-B.pump.DoneWait():
					break cutting
				default:
				}
				if A.pump.Resumes() >= 6 {
					break
				}
				rt.cut()
				next := resumeBoth(t, ctx, A.pump, B.pump)
				if next == nil {
					break
				}
				rt = next
				cuts++
			}

			ru, rd := <-gotUp, <-gotDn
			if ru.err != nil || rd.err != nil {
				t.Fatalf("reading: up %v, down %v (after %d cuts)", ru.err, rd.err, cuts)
			}
			dump := func(n string, p *PumpSwapper) {
				p.mu.Lock()
				t.Logf("%s: suspended=%v aborted=%v upEnded=%v dlEnded=%v upParked=%v dlParked=%v up=%d dl=%d ring=%d resumes=%d",
					n, p.suspended, p.aborted, p.upEnded, p.dlEnded, p.upParked, p.dlParked, p.upBytes.Load(), p.dlBytes.Load(), p.replay.ring.len(), p.resumes)
				p.mu.Unlock()
			}
			if !bytes.Equal(ru.got, up) || !bytes.Equal(rd.got, dn) {
				dump("A", A.pump)
				dump("B", B.pump)
			}
			if !bytes.Equal(ru.got, up) {
				t.Fatalf("upload differs after %d cuts: got %d bytes, want %d", cuts, len(ru.got), len(up))
			}
			if !bytes.Equal(rd.got, dn) {
				t.Fatalf("download differs after %d cuts: got %d bytes, want %d", cuts, len(rd.got), len(dn))
			}
			if cuts == 0 {
				t.Log("no cut landed before the flow finished")
			}
		})
	}
}

// A flow with nothing in flight (an idle SSH session) resumes too, and keeps
// working in both directions afterwards.
func TestResumeIdleFlow(t *testing.T) {
	ctx, _ := testCtx(t)
	rt, a0, b0 := newRawTunnel(t)
	A, B := newResumableEnd(t, ctx, a0), newResumableEnd(t, ctx, b0)

	A.user.Write([]byte("hello"))
	if got := readExact(t, B.user, 5); string(got) != "hello" {
		t.Fatalf("got %q", got)
	}
	B.user.Write([]byte("world"))
	readExact(t, A.user, 5)

	for i := 0; i < 3; i++ {
		rt.cut()
		rt = resumeBoth(t, ctx, A.pump, B.pump)
		msg := genPayload(2000 + i)
		A.user.Write(msg)
		if !bytes.Equal(readExact(t, B.user, len(msg)), msg) {
			t.Fatalf("round %d: upload differs", i)
		}
		B.user.Write(msg)
		if !bytes.Equal(readExact(t, A.user, len(msg)), msg) {
			t.Fatalf("round %d: download differs", i)
		}
	}
	if A.pump.Resumes() != 3 || B.pump.Resumes() != 3 {
		t.Fatalf("resumes: %d/%d, want 3", A.pump.Resumes(), B.pump.Resumes())
	}
}

// Nobody resumes a suspended flow: it ends when the window does, and its app sees
// the connection fail rather than hang.
func TestResumeWindowExpires(t *testing.T) {
	ctx, _ := testCtx(t)
	rt, a0, _ := newRawTunnel(t)
	u, appc := tcpConnPair(t)
	p := NewPromotablePump(ctx, false, appc, a0, testLogger(), testUsage(t), 0, false)
	if err := p.EnableReplay(256 << 10); err != nil {
		t.Fatal(err)
	}
	p.SetResumeWindow(150 * time.Millisecond)
	p.Start()

	rt.cut()
	select {
	case <-p.SuspendedCh():
	case <-time.After(hcTestTimeout):
		t.Fatal("the cut did not suspend the flow")
	}
	select {
	case <-p.DoneWait():
	case <-time.After(hcTestTimeout):
		t.Fatal("the suspended flow outlived its resume window")
	}
	u.SetReadDeadline(time.Now().Add(hcTestTimeout))
	if _, err := u.Read(make([]byte, 1)); err == nil {
		t.Fatal("the app read succeeded on an aborted flow")
	}
}

// A peer that aborts the flow on purpose (its ABORT arrives) ends it here; the
// flow is not suspended waiting for a resume nobody will do.
func TestPeerAbortDoesNotSuspend(t *testing.T) {
	ctx, _ := testCtx(t)
	_, a0, b0 := newRawTunnel(t)
	A, B := newResumableEnd(t, ctx, a0), newResumableEnd(t, ctx, b0)
	A.user.Write([]byte("x"))
	readExact(t, B.user, 1)

	B.pump.Abort()
	select {
	case <-A.pump.DoneWait():
	case <-time.After(hcTestTimeout):
		t.Fatal("the flow did not end after the peer's ABORT")
	}
	select {
	case <-A.pump.SuspendedCh():
		t.Fatal("the flow was suspended instead of ended")
	default:
	}
}

// A swap that was under way when the tunnel died is cancelled with it, and the
// flow can be resumed and then swapped again.
func TestResumeCancelsSwapInProgress(t *testing.T) {
	ctx, _ := testCtx(t)
	rt, a0, b0 := newRawTunnel(t)
	A, B := newResumableEnd(t, ctx, a0), newResumableEnd(t, ctx, b0)
	A.user.Write(genPayload(5000))
	readExact(t, B.user, 5000)

	if _, err := A.pump.FreezeUp(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := B.pump.FreezeUp(ctx); err != nil {
		t.Fatal(err)
	}
	rt.cut()
	rt = resumeBoth(t, ctx, A.pump, B.pump)

	msg := genPayload(70000)
	A.user.Write(msg)
	if !bytes.Equal(readExact(t, B.user, len(msg)), msg) {
		t.Fatal("upload after the cancelled swap differs")
	}
	B.user.Write(msg)
	if !bytes.Equal(readExact(t, A.user, len(msg)), msg) {
		t.Fatal("download after the cancelled swap differs")
	}

	// And a regular swap still works.
	if err := swapTunnelWith(t, ctx, A.pump, B.pump, true); err != nil {
		t.Fatalf("swap after resume: %v", err)
	}
	A.user.Write(msg)
	if !bytes.Equal(readExact(t, B.user, len(msg)), msg) {
		t.Fatal("upload after the swap differs")
	}
}

// A peer that claims to have received more than was ever sent, or less than the
// ring still holds, cannot be resumed: the flow ends instead of corrupting.
func TestResumeRejectsInconsistentCounts(t *testing.T) {
	ctx, _ := testCtx(t)
	rt, a0, b0 := newRawTunnel(t)
	A, _ := newResumableEnd(t, ctx, a0), newResumableEnd(t, ctx, b0)
	A.user.Write(genPayload(1000))
	time.Sleep(50 * time.Millisecond)
	rt.cut()
	if !A.pump.Suspend() {
		t.Fatal("not suspended")
	}
	_, na, _ := newRawTunnel(t)
	if err := A.pump.ResumeFinish(na, 1<<40); err == nil {
		t.Fatal("a count beyond what was sent was accepted")
	}
	select {
	case <-A.pump.DoneWait():
	case <-time.After(hcTestTimeout):
		t.Fatal("the flow survived an impossible resume")
	}
}

func dumpPump(t *testing.T, n string, p *PumpSwapper) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	t.Logf("%s: suspended=%v aborted=%v upEnded=%v dlEnded=%v up=%d dl=%d swaps=%d freezeReq=%v frozen=%v", n, p.suspended, p.aborted, p.upEnded, p.dlEnded, p.upBytes.Load(), p.dlBytes.Load(), p.swaps, p.freezeReq, p.frozen)
	if p.replay != nil {
		r := p.replay
		t.Logf("%s: ring=%d endAcked=%v endAckSent=%v ackSent=%d busy=%v", n, r.ring.len(), r.endAcked.Load(), r.endAckSent.Load(), r.ackSent.Load(), r.ackBusy.Load())
	}
}

// A replay flow refuses a replacement tunnel that cannot carry ACK records: it
// would run until its ring filled and then stall for good.
func TestReplayRefusesTunnelWithoutAcks(t *testing.T) {
	ctx, _ := testCtx(t)
	_, a0, b0 := newRawTunnel(t)
	A, _ := newResumableEnd(t, ctx, a0), newResumableEnd(t, ctx, b0)
	plain, _ := tcpConnPair(t)
	if err := A.pump.Install(plain, 0); err == nil {
		t.Fatal("Install accepted a tunnel without ACK support")
	}
	A.pump.Suspend()
	if err := A.pump.ResumeFinish(plain, 0); err == nil {
		t.Fatal("ResumeFinish accepted a tunnel without ACK support")
	}
}

// A freeze that finds the replay ring full does not push the ring past its limit:
// the read it was holding is sent after the swap, and nothing is lost or repeated.
func TestReplayRingStaysBoundedAcrossSwaps(t *testing.T) {
	const limit = 256 << 10
	ctx, _ := testCtx(t)
	a0, b0 := tunnelPair(t, true)
	// B never acknowledges by itself (no replay), so A's ring fills.
	A := newReplayFlowEnd(t, ctx, a0, limit)
	B := newFlowEnd(t, ctx, b0)

	up := genPayload(6 * limit)
	go A.user.Write(up)
	got := make(chan []byte, 1)
	go func() { // B's app reads, or its download could never reach a swap's limit
		b := make([]byte, 0, len(up))
		buf := make([]byte, 32<<10)
		for len(b) < len(up) {
			n, err := B.user.Read(buf)
			b = append(b, buf[:n]...)
			if err != nil {
				break
			}
		}
		got <- b
	}()
	deadline := time.Now().Add(hcTestTimeout)
	for A.pump.ReplayLen() < limit-64<<10 {
		if time.Now().After(deadline) {
			t.Fatalf("ring never filled (%d)", A.pump.ReplayLen())
		}
		time.Sleep(2 * time.Millisecond)
	}
	for i := 0; i < 4; i++ {
		if err := swapTunnelWith(t, ctx, A.pump, B.pump, true); err != nil {
			t.Fatalf("swap %d: %v", i, err)
		}
		// A swap completes only once both directions have switched; the next one
		// cannot start before.
		waitSwaps(t, A.pump, uint64(i+1))
		waitSwaps(t, B.pump, uint64(i+1))
		if n := A.pump.ReplayLen(); n > limit {
			t.Fatalf("after swap %d the ring holds %d bytes, over its limit %d", i, n, limit)
		}
	}
	// B does not acknowledge, so stand in for it: acknowledge what was sent.
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	timeout := time.After(4 * hcTestTimeout)
	for {
		select {
		case b := <-got:
			if !bytes.Equal(b, up) {
				t.Fatalf("payload differs after the swaps: %d bytes", len(b))
			}
			return
		case <-tick.C:
			A.pump.replay.ring.ackTo(A.pump.UpBytes())
		case <-timeout:
			t.Fatal("the payload never arrived")
		}
	}
}

// The PROXY header is payload the peer may not have received when the tunnel dies:
// the sender keeps it for replay, and the flow resumes with the header intact.
func TestResumeReplaysProxyHeader(t *testing.T) {
	const limit = 256 << 10
	ctx, _ := testCtx(t)
	rt, a0, b0 := newRawTunnel(t)
	u, appc := tcpConnPair(t)
	A := NewPromotablePump(ctx, true, appc, a0, testLogger(), testUsage(t), 0, false)
	if A == nil {
		t.Fatal("NewPromotablePump returned nil")
	}
	if err := A.EnableReplay(limit); err != nil {
		t.Fatal(err)
	}
	A.Start()
	B := newResumableEnd(t, ctx, b0)
	header, err := ProxyProtocolHeader(appc.RemoteAddr(), a0.RemoteAddr())
	if err != nil {
		t.Fatal(err)
	}

	rt.cut() // before B has necessarily read the header
	resumeBoth(t, ctx, A, B.pump)
	msg := genPayload(5000)
	u.Write(msg)
	got := readExact(t, B.user, len(header)+len(msg))
	if !bytes.Equal(got[:len(header)], header) || !bytes.Equal(got[len(header):], msg) {
		t.Fatal("the header or the payload after the resume differs")
	}
}

// An app that has stopped reading (a paused download) keeps the download pump in
// a write to it. A cut then must not wait for the app: the flow is suspended and
// resumed with that write interrupted, and when the app reads again it gets
// every byte, once and in order.
func TestResumeWhileTheAppIsNotReading(t *testing.T) {
	ctx, _ := testCtx(t)
	rt, a0, b0 := newRawTunnel(t)
	A, B := newResumableEnd(t, ctx, a0), newResumableEnd(t, ctx, b0)

	msg := genPayload(8 << 20)
	go A.user.Write(msg) // far more than B's app, which reads nothing, will take
	for i := 0; i < 2; i++ {
		time.Sleep(300 * time.Millisecond) // B's pump is stuck writing to its app by now
		rt.cut()
		if rt = resumeBoth(t, ctx, A.pump, B.pump); rt == nil {
			t.Fatal("the flow could not be suspended")
		}
	}
	if !bytes.Equal(readExact(t, B.user, len(msg)), msg) {
		t.Fatal("the download differs after resuming under a blocked app")
	}
	if B.pump.Resumes() != 2 {
		t.Fatalf("resumes: %d, want 2", B.pump.Resumes())
	}
}

// slowCloseConn is a connection that has stopped getting through: closing it
// waits, the way a stream's FIN waits on a stalled session.
type slowCloseConn struct {
	*net.TCPConn
	wait time.Duration
}

func (c slowCloseConn) Close() error {
	time.Sleep(c.wait)
	return c.TCPConn.Close()
}

// Suspending a flow does not wait for its dead tunnel to be closed: on a stalled
// connection that takes smux's whole close timeout, and whoever is taking flows
// off such a connection would get to the next one only after it.
func TestSuspendDoesNotWaitForTheTunnelToClose(t *testing.T) {
	ctx, _ := testCtx(t)
	a, b := tcpConnPair(t)
	A := newResumableEnd(t, ctx, NewHalfCloseConn(slowCloseConn{a, 3 * time.Second}))
	B := newResumableEnd(t, ctx, NewHalfCloseConn(b))

	A.user.Write([]byte("hello"))
	readExact(t, B.user, 5)

	start := time.Now()
	if !A.pump.Suspend() {
		t.Fatal("the flow could not be suspended")
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("Suspend took %v: it waited for the tunnel to close", d)
	}
	B.pump.Suspend()
}

// A flow whose opening never reached the peer is opened again on a new tunnel:
// the peer there is a brand-new flow end, and it gets everything from the first
// byte, once. A flow that has already exchanged data cannot be.
func TestReopenSendsEverythingFromTheFirstByte(t *testing.T) {
	ctx, _ := testCtx(t)
	a0, lost := tcpConnPair(t) // the peer's end is never read: the opening is "held up"
	defer lost.Close()
	A := newResumableEnd(t, ctx, NewHalfCloseConn(a0))

	msg := genPayload(100 << 10)
	A.user.Write(msg)
	time.Sleep(100 * time.Millisecond)
	if A.pump.HeardFromPeer() {
		t.Fatal("nothing came back, yet the flow says it heard from its peer")
	}
	if !A.pump.Suspend() {
		t.Fatal("the flow could not be suspended")
	}

	a1, b1 := tcpConnPair(t)
	B := newResumableEnd(t, ctx, NewHalfCloseConn(b1)) // the peer, starting from scratch
	if err := A.pump.Reopen(ctx, func() (net.Conn, error) { return NewHalfCloseConn(a1), nil }); err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if !bytes.Equal(readExact(t, B.user, len(msg)), msg) {
		t.Fatal("the reopened flow did not deliver what was sent before it")
	}
	B.user.Write([]byte("pong"))
	if got := readExact(t, A.user, 4); string(got) != "pong" {
		t.Fatalf("the reopened flow does not carry the way back: %q", got)
	}

	// Now that data has come back, the peer knows the flow: no second reopen.
	if !A.pump.HeardFromPeer() {
		t.Fatal("data came back, yet the flow says it has not heard from its peer")
	}
	A.pump.Suspend()
	a2, b2 := tcpConnPair(t)
	defer b2.Close()
	if err := A.pump.Reopen(ctx, func() (net.Conn, error) { return NewHalfCloseConn(a2), nil }); err == nil {
		t.Fatal("a flow that had exchanged data was reopened: its peer would get it twice")
	}
}
