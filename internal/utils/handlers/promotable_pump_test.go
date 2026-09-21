package handlers

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These tests drive PumpSwapper with real TCP sockets and real smux streams.
// Every wait is a channel or a deadline bounded by hcTestTimeout; none sleeps
// to synchronise. A short timer appears only in "must NOT have happened yet"
// assertions, where waiting is the check itself.

// --- transition API adapters -------------------------------------------------

func freezeUp(p *PumpSwapper, ctx context.Context) (uint64, error) { return p.FreezeUp(ctx) }

func installTunnel(p *PumpSwapper, c net.Conn, dl uint64) error { return p.Install(c, dl) }

func promote(p *PumpSwapper, ctx context.Context, legs []net.Conn, build func() (net.Conn, error)) error {
	return p.Promote(ctx, legs, build)
}

// --- probes ------------------------------------------------------------------

// probeConn wraps a conn to gate/observe its Write side. It forwards CloseWrite
// (falling back to Close, like closeWrite does for smux streams) and records
// AbortWrite, so the tests can see how a flow's end was classified.
type probeConn struct {
	net.Conn
	written atomic.Int64
	aborted atomic.Bool
	closeWr atomic.Bool

	mu      sync.Mutex
	gate    chan struct{}
	entered chan struct{}
	woken   chan struct{} // signalled by every SetReadDeadline (FreezeUp's wakeup)
}

func newProbe(c net.Conn) *probeConn {
	return &probeConn{Conn: c, entered: make(chan struct{}, 64), woken: make(chan struct{}, 64)}
}

// arm makes every following Write block until the returned release is called.
func (p *probeConn) arm(t *testing.T) (release func()) {
	t.Helper()
	g := make(chan struct{})
	p.mu.Lock()
	p.gate = g
	p.mu.Unlock()
	var once sync.Once
	release = func() {
		once.Do(func() {
			p.mu.Lock()
			p.gate = nil
			p.mu.Unlock()
			close(g)
		})
	}
	t.Cleanup(release)
	return release
}

func (p *probeConn) Write(b []byte) (int, error) {
	p.mu.Lock()
	g := p.gate
	p.mu.Unlock()
	if g != nil {
		p.entered <- struct{}{}
		<-g
	}
	n, err := p.Conn.Write(b)
	p.written.Add(int64(n))
	return n, err
}

func (p *probeConn) CloseWrite() error {
	p.closeWr.Store(true)
	if cw, ok := p.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return p.Conn.Close()
}

func (p *probeConn) AbortWrite() { p.aborted.Store(true) }

func (p *probeConn) SetReadDeadline(t time.Time) error {
	if !t.IsZero() {
		select {
		case p.woken <- struct{}{}:
		default:
		}
	}
	return p.Conn.SetReadDeadline(t)
}

// waitWoken waits until FreezeUp has requested the freeze (it wakes the app read).
func waitWoken(t *testing.T, p *probeConn) {
	t.Helper()
	select {
	case <-p.woken:
	case <-time.After(hcTestTimeout):
		t.Fatal("FreezeUp never requested the freeze")
	}
}

func waitEntered(t *testing.T, p *probeConn) {
	t.Helper()
	select {
	case <-p.entered:
	case <-time.After(hcTestTimeout):
		t.Fatal("pump never entered the gated Write")
	}
}

// --- fixtures ----------------------------------------------------------------

type side struct {
	u    *net.TCPConn // the user / destination socket driving this side's app conn
	app  *probeConn
	old  *probeConn
	pump *PumpSwapper
}

// startSide runs a PumpSwapper between a real TCP app conn and the given old tunnel.
func startSide(t *testing.T, ctx context.Context, old net.Conn) *side {
	t.Helper()
	u, appc := tcpConnPair(t)
	s := &side{u: u, app: newProbe(appc), old: newProbe(old)}
	s.pump = PromotablePump(ctx, false, s.app, s.old, testLogger(), testUsage(t), 0, false)
	if s.pump == nil {
		t.Fatal("PromotablePump returned nil")
	}
	return s
}

func testCtx(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return ctx, cancel
}

func readExact(t *testing.T, c net.Conn, n int) []byte {
	t.Helper()
	c.SetReadDeadline(time.Now().Add(hcTestTimeout))
	b := make([]byte, n)
	if _, err := io.ReadFull(c, b); err != nil {
		t.Fatalf("read %d bytes: %v", n, err)
	}
	c.SetReadDeadline(time.Time{})
	return b
}

// expectNoData asserts nothing arrives on c for a short while.
func expectNoData(t *testing.T, c net.Conn, what string) {
	t.Helper()
	c.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	var b [1]byte
	n, err := c.Read(b[:])
	c.SetReadDeadline(time.Time{})
	if n != 0 || !isTimeout(err) {
		t.Fatalf("%s: got n=%d err=%v, want a quiet timeout", what, n, err)
	}
}

func waitDone(t *testing.T, p *PumpSwapper, what string) {
	t.Helper()
	select {
	case <-p.DoneWait():
	case <-time.After(hcTestTimeout):
		t.Fatalf("flow never finished: %s", what)
	}
}

func expectEOF(t *testing.T, c net.Conn, what string) {
	t.Helper()
	c.SetReadDeadline(time.Now().Add(hcTestTimeout))
	var b [1]byte
	if _, err := c.Read(b[:]); err == nil || isTimeout(err) {
		t.Fatalf("%s: err=%v, want EOF/closed", what, err)
	}
}

type freezeResult struct {
	n   uint64
	err error
}

func startFreeze(p *PumpSwapper, ctx context.Context) <-chan freezeResult {
	ch := make(chan freezeResult, 1)
	go func() {
		n, err := freezeUp(p, ctx)
		ch <- freezeResult{n, err}
	}()
	return ch
}

func waitFreeze(t *testing.T, ch <-chan freezeResult) freezeResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(hcTestTimeout):
		t.Fatal("FreezeUp never returned")
	}
	return freezeResult{}
}

func runPromote(p *PumpSwapper, ctx context.Context, legs []net.Conn, build func() (net.Conn, error)) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- promote(p, ctx, legs, build) }()
	return ch
}

func waitErr(t *testing.T, ch <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(hcTestTimeout):
		t.Fatalf("%s: never returned", what)
	}
	return nil
}

// --- tests -------------------------------------------------------------------

// A full two-sided promotion in the middle of traffic in both directions: every
// byte arrives exactly once and in order, the counts exchanged are exactly the
// old-tunnel writes, EOF is delivered to the destination (not the source) and a
// reply written after the upload's EOF survives, and both flows finish (the old
// pump deadlocked releasing the old tunnel once the second direction switched).
func TestPromotablePumpTransitionByteExact(t *testing.T) {
	for _, replyAfterEOF := range []bool{false, true} {
		name := "concurrent-reply"
		if replyAfterEOF {
			name = "reply-after-eof"
		}
		t.Run(name, func(t *testing.T) {
			ctx, _ := testCtx(t)
			oa, ob := streamPair(t)
			A := startSide(t, ctx, oa)
			B := startSide(t, ctx, ob)

			up := genPayload(1<<20 + 12345)
			dn := genPayload(900_001)
			for i := range dn {
				dn[i] = ^dn[i]
			}
			half := func(b []byte) int { return len(b) / 2 }

			midA, midB := make(chan struct{}), make(chan struct{})
			promoted := make(chan struct{})
			eofAtB := make(chan struct{})
			type recv struct {
				b   []byte
				err error
			}
			recvAch, recvBch := make(chan recv, 1), make(chan recv, 1)
			reader := func(c net.Conn, mid chan struct{}, eof chan struct{}, out chan<- recv) {
				var got []byte
				buf := make([]byte, 32*1024)
				signalled := false
				c.SetReadDeadline(time.Now().Add(2 * hcTestTimeout))
				for {
					n, err := c.Read(buf)
					got = append(got, buf[:n]...)
					if !signalled && len(got) >= 64*1024 {
						signalled = true
						close(mid)
					}
					if err != nil {
						if eof != nil {
							close(eof)
						}
						if err == io.EOF {
							err = nil
						}
						out <- recv{got, err}
						return
					}
				}
			}
			go reader(A.u, midA, nil, recvAch)
			go reader(B.u, midB, eofAtB, recvBch)

			writerDone := make(chan error, 2)
			go func() { // user at A uploads, then half-closes
				_, err := A.u.Write(up[:half(up)])
				if err == nil {
					<-promoted
					_, err = A.u.Write(up[half(up):])
				}
				A.u.CloseWrite()
				writerDone <- err
			}()
			go func() { // destination at B replies
				_, err := B.u.Write(dn[:half(dn)])
				if err == nil {
					if replyAfterEOF {
						<-eofAtB // the reply is only sent after the upload's EOF arrived
					} else {
						<-promoted
					}
					_, err = B.u.Write(dn[half(dn):])
				}
				B.u.CloseWrite()
				writerDone <- err
			}()

			// Both directions have really moved bytes over the old tunnels.
			for _, ch := range []chan struct{}{midA, midB} {
				select {
				case <-ch:
				case <-time.After(hcTestTimeout):
					t.Fatal("no traffic before promotion")
				}
			}

			legA, legB := tcpConnPair(t)
			newA, newB := tcpConnPair(t)
			nA, nB := newProbe(newA), newProbe(newB)
			eA := runPromote(A.pump, ctx, []net.Conn{legA}, func() (net.Conn, error) { return nA, nil })
			eB := runPromote(B.pump, ctx, []net.Conn{legB}, func() (net.Conn, error) { return nB, nil })
			if err := waitErr(t, eA, "promote A"); err != nil {
				t.Fatalf("promote A: %v", err)
			}
			if err := waitErr(t, eB, "promote B"); err != nil {
				t.Fatalf("promote B: %v", err)
			}
			close(promoted)

			for i := 0; i < 2; i++ {
				select {
				case err := <-writerDone:
					if err != nil {
						t.Fatalf("writer: %v", err)
					}
				case <-time.After(hcTestTimeout):
					t.Fatal("writer stuck")
				}
			}
			rA, rB := <-recvAch, <-recvBch
			if rA.err != nil || rB.err != nil {
				t.Fatalf("read errors: A=%v B=%v", rA.err, rB.err)
			}
			if !bytes.Equal(rB.b, up) {
				t.Fatalf("upload corrupted: got %d bytes want %d", len(rB.b), len(up))
			}
			if !bytes.Equal(rA.b, dn) {
				t.Fatalf("download corrupted: got %d bytes want %d", len(rA.b), len(dn))
			}
			waitDone(t, A.pump, "A")
			waitDone(t, B.pump, "B")

			// Conservation: the old tunnel carried exactly the frozen prefix, the
			// new one the rest, and the transition was real.
			for _, c := range []struct {
				name      string
				old, nw   *probeConn
				wantTotal int
			}{{"A->B", A.old, nA, len(up)}, {"B->A", B.old, nB, len(dn)}} {
				o, n := int(c.old.written.Load()), int(c.nw.written.Load())
				if o == 0 || n == 0 || o+n != c.wantTotal {
					t.Fatalf("%s: old=%d new=%d total=%d, want both > 0 summing to %d", c.name, o, n, o+n, c.wantTotal)
				}
			}
			if nA.aborted.Load() || nB.aborted.Load() || A.app.aborted.Load() || B.app.aborted.Load() {
				t.Fatal("a clean flow was classified as aborted")
			}
		})
	}
}

// FreezeUp must return the count of bytes really committed to the old tunnel:
// it waits for an in-flight old-tunnel write to complete; the pump then stops
// writing to the old tunnel and later bytes go to the new one; the second
// direction's completion releases the old tunnel.
func TestPromotablePumpFreezeWaitsForInFlightWrite(t *testing.T) {
	ctx, _ := testCtx(t)
	a, b := streamPair(t)
	s := startSide(t, ctx, a)
	release := s.old.arm(t)

	msg := genPayload(1000)
	if _, err := s.u.Write(msg); err != nil {
		t.Fatal(err)
	}
	waitEntered(t, s.old)

	fc := startFreeze(s.pump, ctx)
	select {
	case r := <-fc:
		t.Fatalf("FreezeUp returned (%d, %v) while the old-tunnel write was still in flight", r.n, r.err)
	case <-time.After(150 * time.Millisecond):
	}
	release()
	r := waitFreeze(t, fc)
	if r.err != nil || r.n != uint64(len(msg)) {
		t.Fatalf("FreezeUp = (%d, %v), want (%d, nil)", r.n, r.err, len(msg))
	}
	if got := readExact(t, b, len(msg)); !bytes.Equal(got, msg) {
		t.Fatal("old tunnel bytes differ")
	}

	// Frozen: the boundary is final - nothing more may reach the old tunnel.
	more := genPayload(50)
	if _, err := s.u.Write(more); err != nil {
		t.Fatal(err)
	}
	expectNoData(t, b, "old tunnel after freeze")

	newMine, newPeer := tcpConnPair(t)
	if err := installTunnel(s.pump, newMine, 0); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if got := readExact(t, newPeer, len(more)); !bytes.Equal(got, more) {
		t.Fatal("post-freeze bytes must arrive on the new tunnel")
	}

	// dlLimit 0 with the old read parked: the boundary read must be woken, so the
	// download switches; then both directions have switched and the old tunnel
	// is released.
	reply := genPayload(70)
	if _, err := newPeer.Write(reply); err != nil {
		t.Fatal(err)
	}
	if got := readExact(t, s.u, len(reply)); !bytes.Equal(got, reply) {
		t.Fatal("download from the new tunnel differs")
	}
	expectEOF(t, b, "old tunnel released after both directions switched")
}

// A freeze that cannot complete in time is withdrawn: the flow stays plain,
// nothing is lost or duplicated and a later freeze reports the full count.
func TestPromotablePumpFreezeTimeoutLeavesFlowPlain(t *testing.T) {
	ctx, _ := testCtx(t)
	a, b := streamPair(t)
	s := startSide(t, ctx, a)
	release := s.old.arm(t)

	first := genPayload(1000)
	s.u.Write(first)
	waitEntered(t, s.old)

	fctx, fcancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer fcancel()
	if r := waitFreeze(t, startFreeze(s.pump, fctx)); !errors.Is(r.err, context.DeadlineExceeded) {
		t.Fatalf("FreezeUp = (%d, %v), want context deadline", r.n, r.err)
	}
	release()
	if got := readExact(t, b, len(first)); !bytes.Equal(got, first) {
		t.Fatal("in-flight bytes lost")
	}

	second := genPayload(50)
	s.u.Write(second)
	if got := readExact(t, b, len(second)); !bytes.Equal(got, second) {
		t.Fatal("flow did not keep running plain after the withdrawn freeze")
	}
	r := waitFreeze(t, startFreeze(s.pump, ctx))
	if r.err != nil || r.n != uint64(len(first)+len(second)) {
		t.Fatalf("second FreezeUp = (%d, %v), want (%d, nil)", r.n, r.err, len(first)+len(second))
	}
}

// Cancelling while a freeze waits on a stuck write releases the waiter, and
// cancelling the flow finishes both pumps.
func TestPromotablePumpCancelDuringFreeze(t *testing.T) {
	ctx, cancelFlow := testCtx(t)
	a, _ := streamPair(t)
	s := startSide(t, ctx, a)
	release := s.old.arm(t)
	s.u.Write(genPayload(500))
	waitEntered(t, s.old)

	fctx, cancelFreeze := context.WithCancel(ctx)
	fc := startFreeze(s.pump, fctx)
	cancelFreeze()
	if r := waitFreeze(t, fc); !errors.Is(r.err, context.Canceled) {
		t.Fatalf("FreezeUp = (%d, %v), want context.Canceled", r.n, r.err)
	}

	fc2 := startFreeze(s.pump, ctx)
	cancelFlow()
	if r := waitFreeze(t, fc2); r.err == nil {
		t.Fatal("FreezeUp succeeded on a cancelled flow")
	}
	release() // let the stuck Write finish so the pump can exit
	waitDone(t, s.pump, "cancelled flow")
}

// The upload's EOF goes to its destination (the old tunnel), not back to its
// source, so the reply written after it still reaches the user. The old tunnel
// here is real TCP: a smux stream cannot half-close (legacy full-close
// semantics, plan 024), which is not what this pins.
func TestPromotablePumpUploadEOFReachesOldDestination(t *testing.T) {
	ctx, _ := testCtx(t)
	mine, peer := tcpConnPair(t)
	s := startSide(t, ctx, mine)

	req, reply := genPayload(3000), genPayload(2000)
	s.u.Write(req)
	s.u.CloseWrite()

	got, err := readAllDeadline(peer)
	if err != nil || !bytes.Equal(got, req) {
		t.Fatalf("old destination: %d bytes, err=%v (want the upload then a clean EOF)", len(got), err)
	}
	peer.Write(reply)
	peer.CloseWrite()
	got, err = readAllDeadline(s.u)
	if err != nil || !bytes.Equal(got, reply) {
		t.Fatalf("user: %d bytes, err=%v (reply after EOF must survive)", len(got), err)
	}
	waitDone(t, s.pump, "EOF before promotion")
	if s.old.aborted.Load() || s.app.aborted.Load() {
		t.Fatal("clean EOF classified as aborted")
	}
	// A flow whose upload already ended is not promotable any more.
	if _, err := freezeUp(s.pump, ctx); err == nil {
		t.Fatal("FreezeUp succeeded after the flow ended")
	}
}

// EOF after promotion goes to the new tunnel, including an EOF that arrived
// while the freeze was still waiting for an in-flight write.
func TestPromotablePumpEOFAfterPromotion(t *testing.T) {
	ctx, _ := testCtx(t)
	a, b := streamPair(t)
	s := startSide(t, ctx, a)
	release := s.old.arm(t)

	msg := genPayload(4000)
	s.u.Write(msg)
	s.u.CloseWrite() // EOF is already queued behind the data
	waitEntered(t, s.old)
	fc := startFreeze(s.pump, ctx)
	waitWoken(t, s.app) // the freeze is requested before the write completes
	release()
	r := waitFreeze(t, fc)
	if r.err != nil || r.n != uint64(len(msg)) {
		t.Fatalf("FreezeUp = (%d, %v), want (%d, nil)", r.n, r.err, len(msg))
	}
	if got := readExact(t, b, len(msg)); !bytes.Equal(got, msg) {
		t.Fatal("old tunnel bytes differ")
	}

	newMine, newPeer := tcpConnPair(t)
	if err := installTunnel(s.pump, newMine, 0); err != nil {
		t.Fatal(err)
	}
	got, err := readAllDeadline(newPeer)
	if err != nil || len(got) != 0 {
		t.Fatalf("new tunnel: %d bytes err=%v, want a clean EOF and no data", len(got), err)
	}
	// The reverse direction is still open: a reply after the upload's EOF.
	reply := genPayload(999)
	newPeer.Write(reply)
	newPeer.CloseWrite()
	if got, err := readAllDeadline(s.u); err != nil || !bytes.Equal(got, reply) {
		t.Fatalf("user: %d bytes err=%v", len(got), err)
	}
	waitDone(t, s.pump, "EOF after promotion")
}

// The old download delivers exactly its promised prefix: a late EOF at the
// limit is fine, a shorter stream is truncation, and a new-tunnel FIN is a
// clean end while a reset is a failure.
func TestPromotablePumpDownloadClassification(t *testing.T) {
	setup := func(t *testing.T, limit uint64) (s *side, oldPeer net.Conn, newPeer net.Conn, np *probeConn) {
		ctx, _ := testCtx(t)
		a, b := streamPair(t)
		s = startSide(t, ctx, a)
		if r := waitFreeze(t, startFreeze(s.pump, ctx)); r.err != nil || r.n != 0 {
			t.Fatalf("FreezeUp = (%d, %v)", r.n, r.err)
		}
		m, p := tcpConnPair(t)
		nprobe := newProbe(m)
		if err := installTunnel(s.pump, nprobe, limit); err != nil {
			t.Fatal(err)
		}
		return s, b, p, nprobe
	}

	t.Run("exact-prefix-then-new", func(t *testing.T) {
		s, oldPeer, newPeer, np := setup(t, 100)
		oldPeer.Write(genPayload(100))
		oldPeer.Close() // late EOF at the boundary
		tail := genPayload(30)
		newPeer.Write(tail)
		newPeer.(*net.TCPConn).CloseWrite()
		s.u.CloseWrite()
		got, err := readAllDeadline(s.u)
		want := append(genPayload(100), tail...)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("user got %d bytes err=%v, want %d", len(got), err, len(want))
		}
		waitDone(t, s.pump, "exact prefix")
		if np.aborted.Load() || s.app.aborted.Load() {
			t.Fatal("clean flow classified as aborted")
		}
	})

	t.Run("old-truncated", func(t *testing.T) {
		s, oldPeer, _, np := setup(t, 100)
		oldPeer.Write(genPayload(40))
		oldPeer.Close() // 60 promised bytes never arrive
		waitDone(t, s.pump, "truncated old download")
		if !np.aborted.Load() || !s.app.aborted.Load() {
			t.Fatal("truncated download was not classified as a failure")
		}
	})

	t.Run("new-fin-is-clean", func(t *testing.T) {
		s, _, newPeer, _ := setup(t, 0)
		newPeer.Write(genPayload(10))
		newPeer.(*net.TCPConn).CloseWrite()
		got, err := readAllDeadline(s.u)
		if err != nil || len(got) != 10 {
			t.Fatalf("user got %d bytes err=%v", len(got), err)
		}
		if s.app.aborted.Load() {
			t.Fatal("clean EOF classified as aborted")
		}
	})

	t.Run("new-reset-is-failure", func(t *testing.T) {
		s, _, newPeer, _ := setup(t, 0)
		newPeer.(*net.TCPConn).SetLinger(0)
		newPeer.Close() // RST
		waitDone(t, s.pump, "reset new tunnel")
		if !s.app.aborted.Load() {
			t.Fatal("transport error was classified as a clean EOF")
		}
	})
}

// Whichever direction finishes its transition second, the old tunnel is
// released and the flow completes.
func TestPromotablePumpBothPhaseOrders(t *testing.T) {
	t.Run("upload-switches-first", func(t *testing.T) {
		ctx, _ := testCtx(t)
		a, b := streamPair(t)
		s := startSide(t, ctx, a)
		if r := waitFreeze(t, startFreeze(s.pump, ctx)); r.err != nil {
			t.Fatal(r.err)
		}
		newMine, newPeer := tcpConnPair(t)
		if err := installTunnel(s.pump, newMine, 200); err != nil {
			t.Fatal(err)
		}
		up := genPayload(64)
		s.u.Write(up)
		if got := readExact(t, newPeer, len(up)); !bytes.Equal(got, up) {
			t.Fatal("upload did not switch to the new tunnel")
		}
		// The download still owes 200 bytes on the old tunnel.
		old := genPayload(200)
		b.Write(old)
		if got := readExact(t, s.u, len(old)); !bytes.Equal(got, old) {
			t.Fatal("old prefix not delivered")
		}
		expectEOF(t, b, "old tunnel released after the second direction switched")
		s.u.CloseWrite()
		newPeer.Close()
		waitDone(t, s.pump, "upload-first")
	})
	t.Run("download-switches-first", func(t *testing.T) {
		ctx, _ := testCtx(t)
		a, b := streamPair(t)
		s := startSide(t, ctx, a)
		s.u.Write(genPayload(10))
		readExact(t, b, 10)
		if r := waitFreeze(t, startFreeze(s.pump, ctx)); r.err != nil || r.n != 10 {
			t.Fatalf("freeze = (%d, %v)", r.n, r.err)
		}
		newMine, newPeer := tcpConnPair(t)
		if err := installTunnel(s.pump, newMine, 0); err != nil {
			t.Fatal(err)
		}
		dn := genPayload(33)
		newPeer.Write(dn)
		if got := readExact(t, s.u, len(dn)); !bytes.Equal(got, dn) {
			t.Fatal("download did not switch")
		}
		expectEOF(t, b, "old tunnel released")
		s.u.CloseWrite()
		newPeer.Close()
		waitDone(t, s.pump, "download-first")
	})
}

// The handshake is bounded and classified: before the freeze ack a failure
// leaves the flow plain (and closes the new legs); after it the flow is aborted
// (and the legs closed). Nothing waits forever on a silent peer.
func TestPromotablePumpPromoteHandshake(t *testing.T) {
	build := func() (net.Conn, error) {
		m, _ := tcpConnPair(t)
		return m, nil
	}

	t.Run("stalled-count-peer", func(t *testing.T) {
		ctx, _ := testCtx(t)
		a, _ := streamPair(t)
		s := startSide(t, ctx, a)
		leg, legPeer := tcpConnPair(t)
		pctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
		defer cancel()
		if err := waitErr(t, runPromote(s.pump, pctx, []net.Conn{leg}, build), "promote"); err == nil {
			t.Fatal("promotion succeeded against a silent peer")
		}
		waitDone(t, s.pump, "post-freeze failure must abort the flow")
		// The leg was closed: after our 8-byte count the peer reads to the end.
		if got, err := readAllDeadline(legPeer); isTimeout(err) || len(got) != 8 {
			t.Fatalf("leg peer: %d bytes err=%v, want the count then a closed leg", len(got), err)
		}
		if !s.app.aborted.Load() {
			t.Fatal("abort was not marked")
		}
	})

	t.Run("peer-closes-during-exchange", func(t *testing.T) {
		ctx, _ := testCtx(t)
		a, _ := streamPair(t)
		s := startSide(t, ctx, a)
		leg, legPeer := tcpConnPair(t)
		e := runPromote(s.pump, ctx, []net.Conn{leg}, build)
		readExact(t, legPeer, 8) // it sent its count
		legPeer.Close()
		if err := waitErr(t, e, "promote"); err == nil {
			t.Fatal("promotion succeeded although the peer closed")
		}
		waitDone(t, s.pump, "closure during the exchange")
	})

	t.Run("build-failure-aborts", func(t *testing.T) {
		ctx, _ := testCtx(t)
		a, _ := streamPair(t)
		s := startSide(t, ctx, a)
		leg, legPeer := tcpConnPair(t)
		e := runPromote(s.pump, ctx, []net.Conn{leg}, func() (net.Conn, error) { return nil, errors.New("boom") })
		readExact(t, legPeer, 8)
		legPeer.Write(make([]byte, 8))
		if err := waitErr(t, e, "promote"); err == nil {
			t.Fatal("promotion succeeded although the wrapper failed")
		}
		waitDone(t, s.pump, "build failure")
		expectEOF(t, legPeer, "leg closed")
	})

	t.Run("cancel-during-exchange", func(t *testing.T) {
		ctx, _ := testCtx(t)
		a, _ := streamPair(t)
		s := startSide(t, ctx, a)
		leg, legPeer := tcpConnPair(t)
		pctx, cancel := context.WithCancel(ctx)
		e := runPromote(s.pump, pctx, []net.Conn{leg}, build)
		readExact(t, legPeer, 8)
		cancel()
		if err := waitErr(t, e, "promote"); err == nil {
			t.Fatal("promotion succeeded after cancellation")
		}
		waitDone(t, s.pump, "cancelled exchange")
	})

	t.Run("pre-freeze-failure-stays-plain", func(t *testing.T) {
		ctx, _ := testCtx(t)
		a, b := streamPair(t)
		s := startSide(t, ctx, a)
		release := s.old.arm(t)
		first := genPayload(500)
		s.u.Write(first)
		waitEntered(t, s.old)
		leg, legPeer := tcpConnPair(t)
		pctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		defer cancel()
		if err := waitErr(t, runPromote(s.pump, pctx, []net.Conn{leg}, build), "promote"); err == nil {
			t.Fatal("promotion succeeded while the old write was stuck")
		}
		expectEOF(t, legPeer, "new leg closed")
		release()
		if got := readExact(t, b, len(first)); !bytes.Equal(got, first) {
			t.Fatal("bytes lost")
		}
		select {
		case <-s.pump.DoneWait():
			t.Fatal("a failed pre-freeze attempt must leave the flow running")
		default:
		}
		second := genPayload(20)
		s.u.Write(second)
		if got := readExact(t, b, len(second)); !bytes.Equal(got, second) {
			t.Fatal("flow did not keep running plain")
		}
	})
}
