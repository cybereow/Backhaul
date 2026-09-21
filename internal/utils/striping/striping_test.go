package striping

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// pipePair returns two slices of net.Conn: legs[i] on one side is connected
// to peers[i] on the other, via an in-memory net.Pipe (no real network, but
// exercises the exact same io.Reader/io.Writer contract a TCP conn would).
func pipePair(n int) (legs []net.Conn, peers []net.Conn) {
	for i := 0; i < n; i++ {
		a, b := net.Pipe()
		legs = append(legs, a)
		peers = append(peers, b)
	}
	return legs, peers
}

func TestStripedRoundTrip(t *testing.T) {
	for _, n := range []int{1, 2, 4, 7} {
		n := n
		t.Run("", func(t *testing.T) {
			legs, peers := pipePair(n)
			client := New(legs, 997) // deliberately not a round chunk size
			server := New(peers, 997)
			defer client.Close()
			defer server.Close()

			payload := make([]byte, 5*1024*1024+37) // not a multiple of chunk size
			if _, err := rand.Read(payload); err != nil {
				t.Fatalf("rand.Read: %v", err)
			}

			var wg sync.WaitGroup
			wg.Add(1)
			var writeErr error
			go func() {
				defer wg.Done()
				_, writeErr = client.Write(payload)
			}()

			got, err := io.ReadAll(io.LimitReader(server, int64(len(payload))))
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			wg.Wait()
			if writeErr != nil {
				t.Fatalf("write: %v", writeErr)
			}

			if !bytes.Equal(got, payload) {
				t.Fatalf("legs=%d: reassembled payload mismatch (got %d bytes, want %d)", n, len(got), len(payload))
			}
		})
	}
}

func TestStripedBidirectional(t *testing.T) {
	legs, peers := pipePair(3)
	a := New(legs, 4096)
	b := New(peers, 4096)
	defer a.Close()
	defer b.Close()

	aToB := make([]byte, 200000)
	bToA := make([]byte, 150000)
	rand.Read(aToB)
	rand.Read(bToA)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); a.Write(aToB) }()
	go func() { defer wg.Done(); b.Write(bToA) }()

	gotAtB := make([]byte, 0, len(aToB))
	gotAtA := make([]byte, 0, len(bToA))

	var wg2 sync.WaitGroup
	wg2.Add(2)
	go func() {
		defer wg2.Done()
		buf, _ := io.ReadAll(io.LimitReader(b, int64(len(aToB))))
		gotAtB = buf
	}()
	go func() {
		defer wg2.Done()
		buf, _ := io.ReadAll(io.LimitReader(a, int64(len(bToA))))
		gotAtA = buf
	}()

	wg.Wait()
	wg2.Wait()

	if !bytes.Equal(gotAtB, aToB) {
		t.Fatalf("a->b payload mismatch")
	}
	if !bytes.Equal(gotAtA, bToA) {
		t.Fatalf("b->a payload mismatch")
	}
}

// TestStripedLegStall covers the regression that shipped in the first cut:
// a leg going silently unresponsive (peer never reads, never closes, just
// stops) must not hang Read/Write forever. It has to be detected and torn
// down within stallTimeout, propagating an error to both directions.
func TestStripedLegStall(t *testing.T) {
	const legs = 3
	clientLegs, serverLegs := pipePair(legs)

	client := New(clientLegs, 4096)
	server := New(serverLegs, 4096)
	client.stallTimeout = 200 * time.Millisecond
	server.stallTimeout = 200 * time.Millisecond
	defer client.Close()
	defer server.Close()

	// Drain legs 1 and 2 normally; leg 0's peer is never read from, so any
	// write the client's writeLeg(0) attempts blocks until its deadline -
	// simulating a leg that accepted the TCP connection but then went
	// silent (no RST, no FIN, nothing).
	go func() {
		buf := make([]byte, 4096+headerSize)
		for {
			if _, err := io.ReadFull(serverLegs[1], buf); err != nil {
				return
			}
		}
	}()
	go func() {
		buf := make([]byte, 4096+headerSize)
		for {
			if _, err := io.ReadFull(serverLegs[2], buf); err != nil {
				return
			}
		}
	}()

	readErrCh := make(chan error, 1)
	go func() {
		_, err := io.ReadAll(server)
		readErrCh <- err
	}()

	// With 2 of 3 legs healthy, the work-stealing scheduler routes most
	// chunks away from the stalled leg, so this first Write can legitimately
	// succeed from the caller's point of view (same as a plain TCP Write
	// succeeding just means "accepted", not "peer got it"). What must not
	// happen is the one chunk that *did* land on leg 0 silently vanishing
	// forever - the read side has to notice and fail.
	payload := make([]byte, 64*4096)
	if _, err := client.Write(payload); err != nil {
		t.Logf("client.Write returned an error (acceptable): %v", err)
	}

	select {
	case err := <-readErrCh:
		if err == nil {
			t.Fatal("expected server read to fail once a leg stalled, got nil error")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server read did not return within 3x the stall timeout - failure did not propagate to the other direction")
	}

	// Once the stall is detected, the whole Conn is torn down - a
	// subsequent Write must fail promptly instead of leaking the client
	// side as a half-alive connection nobody will ever clean up.
	select {
	case <-client.closed:
	case <-time.After(3 * time.Second):
		t.Fatal("client was not closed after the stall was detected")
	}
	if _, err := client.Write([]byte("more data")); err == nil {
		t.Fatal("expected client.Write to fail after the connection was torn down")
	}
}

// TestReadDoesNotBusySpinWhenEndArrivesEarly is the regression test for the
// busy-spin: the END marker (which carries the total chunk count) can arrive on
// a fast leg while a data chunk is still in flight on a slower one. Once the
// marker closes totalKnown, that channel stays permanently ready - so a Read
// still waiting on the missing sequence would wake on it every loop iteration,
// pegging a core and starving the stallTimeout timer (which loses the select
// race to the closed channel), so a genuinely missing chunk never times out.
//
// Here the peer announces one data chunk but never sends it and keeps the legs
// open. A correct Read waits on the missing sequence and gives up after
// stallTimeout; the buggy one spins forever and never returns.
func TestReadDoesNotBusySpinWhenEndArrivesEarly(t *testing.T) {
	legs, peers := pipePair(2)
	server := New(legs, 4096)
	server.stallTimeout = 200 * time.Millisecond
	defer server.Close()

	var end [headerSize]byte
	binary.BigEndian.PutUint32(end[0:4], 1) // total = 1 data chunk
	binary.BigEndian.PutUint32(end[4:8], endMarkerLen)
	go func() {
		peers[0].Write(end[:])
		// Never send chunk 0, and never close the legs - so the only thing that
		// can end the Read is the stall timer.
	}()

	readErr := make(chan error, 1)
	go func() {
		buf := make([]byte, 16)
		_, err := server.Read(buf)
		readErr <- err
	}()

	select {
	case err := <-readErr:
		if err == nil {
			t.Fatal("expected a stall error waiting for the missing chunk, got nil")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Read did not return within 3s of a 200ms stall timeout - busy-spin defeated stallTimeout")
	}
}

// delayedConn wraps a net.Conn and adds latency before every Read returns,
// simulating one leg of a striped connection being consistently slower than
// its siblings (e.g. a noisier path through the same CDN).
type delayedConn struct {
	net.Conn
	delay time.Duration
}

func (d *delayedConn) Read(p []byte) (int, error) {
	time.Sleep(d.delay)
	return d.Conn.Read(p)
}

// TestStripedUnevenLegSpeeds checks that data reassembles correctly (no
// corruption, no lost bytes, no hang) when legs drain at very different
// rates - the scenario the original round-robin scheduler handled by
// stalling the reorder buffer on the slow leg's fixed share of the data.
func TestStripedUnevenLegSpeeds(t *testing.T) {
	clientLegs, serverLegs := pipePair(4)

	// Slow down leg 0 from the server's (reading) side.
	serverLegs[0] = &delayedConn{Conn: serverLegs[0], delay: 20 * time.Millisecond}

	client := New(clientLegs, 2048)
	server := New(serverLegs, 2048)
	defer client.Close()
	defer server.Close()

	payload := make([]byte, 2*1024*1024+123)
	rand.Read(payload)

	writeErrCh := make(chan error, 1)
	go func() {
		_, err := client.Write(payload)
		writeErrCh <- err
	}()

	got, err := io.ReadAll(io.LimitReader(server, int64(len(payload))))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := <-writeErrCh; err != nil {
		t.Fatalf("write: %v", err)
	}

	if !bytes.Equal(got, payload) {
		t.Fatalf("reassembled payload mismatch under uneven leg speeds (got %d bytes, want %d)", len(got), len(payload))
	}
}

// sharedLimiter is a token bucket shared across several wrapped conns, so
// their combined write rate - not each one individually - is capped. This
// is what actually distinguishes "N legs with independent capacity" from
// "N legs contending for one real bottleneck": with independent per-leg
// limiters every leg would just proceed at its own pace; a *shared* limiter
// is the only way to reproduce legs genuinely competing for one pipe.
type sharedLimiter struct {
	mu          sync.Mutex
	bytesPerSec float64
	tokens      float64
	last        time.Time
}

func newSharedLimiter(bytesPerSec float64) *sharedLimiter {
	return &sharedLimiter{bytesPerSec: bytesPerSec, tokens: bytesPerSec, last: time.Now()}
}

func (l *sharedLimiter) wait(n int) {
	l.mu.Lock()
	now := time.Now()
	l.tokens += now.Sub(l.last).Seconds() * l.bytesPerSec
	l.last = now
	if l.tokens > l.bytesPerSec {
		l.tokens = l.bytesPerSec // cap burst to ~1s worth
	}

	need := float64(n) - l.tokens
	if need <= 0 {
		l.tokens -= float64(n)
		l.mu.Unlock()
		return
	}
	wait := time.Duration(need / l.bytesPerSec * float64(time.Second))
	l.tokens = 0
	// Fast-forward the token clock to the point where this deficit will
	// have naturally been paid off. Without this, l.last stays stamped at
	// the *start* of this wait, so the next call's elapsed-time credit
	// double-counts the interval this call already spent sleeping to earn
	// it - which doubles the effective rate instead of enforcing it.
	l.last = l.last.Add(wait)
	l.mu.Unlock()
	time.Sleep(wait)
}

type limitedConn struct {
	net.Conn
	limiter *sharedLimiter
}

func (lc *limitedConn) Write(p []byte) (int, error) {
	lc.limiter.wait(len(p))
	return lc.Conn.Write(p)
}

// TestStripedSharedBottleneck exercises the actual scenario the congestion
// scheduler exists for: legs that don't have independent capacity, but all
// contend for one real shared link. It doesn't assert a throughput number
// (that's exactly the kind of thing that's flaky in CI) - it asserts what
// actually matters: the transfer still completes correctly, and it does so
// within a sane bound instead of the scheduler mismanaging itself into a
// stall under contention.
func TestStripedSharedBottleneck(t *testing.T) {
	const legCount = 4
	clientLegs, serverLegs := pipePair(legCount)

	limiter := newSharedLimiter(300 * 1024) // ~300 KB/s combined, however many legs pull from it
	for i := range clientLegs {
		clientLegs[i] = &limitedConn{Conn: clientLegs[i], limiter: limiter}
	}

	client := New(clientLegs, 8*1024)
	server := New(serverLegs, 8*1024)
	defer client.Close()
	defer server.Close()

	payload := make([]byte, 512*1024)
	rand.Read(payload)

	writeErrCh := make(chan error, 1)
	go func() {
		_, err := client.Write(payload)
		writeErrCh <- err
	}()

	readDone := make(chan struct{})
	var got []byte
	var readErr error
	go func() {
		got, readErr = io.ReadAll(io.LimitReader(server, int64(len(payload))))
		close(readDone)
	}()

	select {
	case <-readDone:
	case <-time.After(15 * time.Second):
		t.Fatal("transfer over a shared, rate-limited bottleneck did not complete in time")
	}

	if readErr != nil {
		t.Fatalf("read: %v", readErr)
	}
	if err := <-writeErrCh; err != nil {
		t.Fatalf("write: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("reassembled payload mismatch over shared bottleneck (got %d bytes, want %d)", len(got), len(payload))
	}
}

// slowConn wraps a net.Conn and delays reads, simulating a receiver that
// drains slower than the sender fills - which backs data up in the reassembly
// path and widens the shutdown race window.
type slowConn struct {
	net.Conn
	delay time.Duration
}

func (s *slowConn) Read(p []byte) (int, error) {
	time.Sleep(s.delay)
	return s.Conn.Read(p)
}

// TestStripedWriteThenCloseNoLoss is the regression test for the tail-loss:
// the sender writes a payload and Closes (the real pump's sequence), the
// receiver reads to EOF. Every byte must arrive - a clean shutdown is
// lossless. Repeated many times to catch the intermittent teardown race.
func TestStripedWriteThenCloseNoLoss(t *testing.T) {
	for iter := 0; iter < 400; iter++ {
		legs, peers := pipePair(4)
		client := New(legs, 4096)
		server := New(peers, 4096)

		payload := make([]byte, 1<<20+123) // ~1MB, not a chunk multiple
		if _, err := rand.Read(payload); err != nil {
			t.Fatalf("rand: %v", err)
		}

		go func() {
			client.Write(payload)
			client.Close()
		}()

		got, err := io.ReadAll(server)
		if err != nil {
			t.Fatalf("iter %d: read to EOF failed: %v (got %d/%d bytes)", iter, err, len(got), len(payload))
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("iter %d: short/again read: got %d bytes, want %d", iter, len(got), len(payload))
		}
		server.Close()
	}
}

// TestStripedAbortWriteIsLoud is the regression test for the silent-truncation
// bug: when the byte stream feeding a striped sender is cut short by an
// upstream error, the sender must NOT tell the peer the (partial) stream is
// complete. In the real proxy this happened when a forwarded connection's
// source was reset mid-transfer: the sender had queued only some of the bytes,
// Close announced that partial count as the authoritative total via the
// end-of-stream marker, and the receiver reported a clean io.EOF at the
// truncated boundary - silently passing a short read off as the whole stream.
//
// With AbortWrite the sender suppresses the end-of-stream marker, so the
// receiver's Read surfaces io.ErrUnexpectedEOF (a dropped-connection-style
// error) instead of a clean EOF. Data may be lost when a source is truncated -
// there is no retransmission - but it must be lost loudly, never silently.
func TestStripedAbortWriteIsLoud(t *testing.T) {
	for iter := 0; iter < 100; iter++ {
		legs, peers := pipePair(3)
		sender := New(legs, 4096)
		receiver := New(peers, 4096)

		payload := make([]byte, 80*4096+17) // 80-ish chunks
		if _, err := rand.Read(payload); err != nil {
			t.Fatalf("rand: %v", err)
		}

		go func() {
			sender.Write(payload)
			sender.AbortWrite() // the write source was truncated upstream
			sender.Close()
		}()

		_, err := io.ReadAll(receiver)
		if err == nil {
			t.Fatalf("iter %d: truncated stream reported a clean EOF - silent data loss", iter)
		}
		receiver.Close()
	}
}

// TestStripedCleanCloseStillEOF guards the other half of the contract: a normal
// Write+Close (no AbortWrite) must still deliver every byte and end on a clean
// io.EOF. This makes sure the AbortWrite gate did not make ordinary closes loud.
func TestStripedCleanCloseStillEOF(t *testing.T) {
	legs, peers := pipePair(3)
	sender := New(legs, 4096)
	receiver := New(peers, 4096)
	defer receiver.Close()

	payload := make([]byte, 50*4096+9)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}

	go func() {
		sender.Write(payload)
		sender.Close()
	}()

	got, err := io.ReadAll(receiver)
	if err != nil {
		t.Fatalf("clean close should read to a clean EOF, got %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("clean close lost data: got %d bytes, want %d", len(got), len(payload))
	}
}

// ---- Plan 004: idle vs proven gaps, and the reassembly budget ----

// stripeFrame is one plain-striping wire frame: seq(4) length(4) payload.
func stripeFrame(seq uint32, payload string) []byte {
	b := make([]byte, headerSize, headerSize+len(payload))
	binary.BigEndian.PutUint32(b[0:4], seq)
	binary.BigEndian.PutUint32(b[4:8], uint32(len(payload)))
	return append(b, payload...)
}

// TestStripedTruncatedPayloadIsLoud: a leg that closes cleanly right after a
// chunk header (before any of the announced payload) is truncation, not a clean
// end of stream.
func TestStripedTruncatedPayloadIsLoud(t *testing.T) {
	legs, peers := pipePair(2)
	s := New(legs, 16)
	s.stallTimeout = 2 * time.Second // before any I/O
	defer s.Close()
	drainPeers(peers)
	ev := readEvents(s)

	header := stripeFrame(0, "")
	binary.BigEndian.PutUint32(header[4:8], 8) // announce 8 payload bytes, send none
	for _, p := range peers {
		go func(p net.Conn) {
			p.Write(header)
			p.Close()
		}(p)
	}
	expectErr(t, ev, "", 3*time.Second)
}

// TestStripedWriteUnblocksOnCloseWrite: a Write blocked on a full writeQueue must
// fail once CloseWrite has begun (the write workers stop draining the queue), not
// hold wmu forever and deadlock CloseWrite's END marker. The single leg is gated,
// so the worker stays blocked in its first send and the queue stays full until
// the test releases it: without the flush case in Write's enqueue select the
// Write below cannot return.
func TestStripedWriteUnblocksOnCloseWrite(t *testing.T) {
	legs, peers := pipePair(1)
	gate := make(chan struct{})
	s := New([]net.Conn{&faultLeg{Conn: legs[0], gate: gate}}, 16)
	drainPeers(peers)
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	closeWriteDone := make(chan struct{})
	defer func() {
		release()
		// teardown (not Close): if the fix regresses, Close would block behind the
		// stuck CloseWrite; closing the Conn is also what unsticks that Write.
		s.teardown()
		peers[0].Close()
		select {
		case <-closeWriteDone:
		case <-time.After(5 * time.Second):
			t.Error("CloseWrite did not finish after teardown")
		}
	}()

	writeRes := make(chan error, 1)
	go func() {
		_, err := s.Write(make([]byte, 100*16)) // far more chunks than the queue holds
		writeRes <- err
	}()

	deadline := time.Now().Add(3 * time.Second)
	for len(s.writeQueue) < cap(s.writeQueue) { // worker is stuck on the gate: queue fills
		if time.Now().After(deadline) {
			t.Fatal("writeQueue never filled")
		}
		runtime.Gosched()
	}
	go func() {
		s.CloseWrite()
		close(closeWriteDone)
	}()
	for {
		select {
		case <-s.flush:
		default:
			if time.Now().After(deadline) {
				t.Fatal("CloseWrite never began")
			}
			runtime.Gosched()
			continue
		}
		break
	}

	select {
	case err := <-writeRes:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("Write after CloseWrite began = %v, want net.ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Write still blocked on a full queue after CloseWrite began")
	}
}

// stripeEnd is the END marker announcing total data chunks.
func stripeEnd(total uint32) []byte {
	b := make([]byte, headerSize)
	binary.BigEndian.PutUint32(b[0:4], total)
	binary.BigEndian.PutUint32(b[4:8], endMarkerLen)
	return b
}

// drainPeers gives every raw leg exactly one reader that discards whatever the
// conn under test writes (END markers on Close), so Close never blocks on an
// unread pipe. Writers to a leg (the test) are separate from this reader.
func drainPeers(peers []net.Conn) {
	for _, p := range peers {
		go io.Copy(io.Discard, p)
	}
}

type readEvent struct {
	data string
	err  error
	at   time.Time
}

// readEvents runs one reader goroutine over r and reports every Read result
// until the first error. The goroutine ends when r is closed by the test.
func readEvents(r io.Reader) <-chan readEvent {
	ch := make(chan readEvent, 4096)
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := r.Read(buf)
			ch <- readEvent{string(buf[:n]), err, time.Now()}
			if err != nil {
				return
			}
		}
	}()
	return ch
}

// expectData waits for the next Read result to be exactly want.
func expectData(t *testing.T, ev <-chan readEvent, want string) {
	t.Helper()
	select {
	case e := <-ev:
		if e.err != nil || e.data != want {
			t.Fatalf("Read = %q, %v; want %q, nil", e.data, e.err, want)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("no data %q within 3s", want)
	}
}

// expectQuiet fails if Read returns anything (data or an error) within d.
func expectQuiet(t *testing.T, ev <-chan readEvent, d time.Duration, why string) {
	t.Helper()
	select {
	case e := <-ev:
		t.Fatalf("%s: Read returned %q, %v; it must keep waiting", why, e.data, e.err)
	case <-time.After(d):
	}
}

// expectErr waits for a non-EOF Read error containing substr and returns when
// it was observed. within bounds how long the test waits.
func expectErr(t *testing.T, ev <-chan readEvent, substr string, within time.Duration) time.Time {
	t.Helper()
	deadline := time.After(within)
	for {
		select {
		case e := <-ev:
			if e.err == nil {
				continue // data before the failure
			}
			if e.err == io.EOF || !strings.Contains(e.err.Error(), substr) {
				t.Fatalf("Read error = %v; want a non-EOF error containing %q", e.err, substr)
			}
			return e.at
		case <-deadline:
			t.Fatalf("no %q error within %s", substr, within)
		}
	}
}

// TestStripedIdle: a connection with nothing missing is idle, not stalled -
// like a quiet TCP flow it must never be timed out by Read itself.
func TestStripedIdle(t *testing.T) {
	const stall = 100 * time.Millisecond

	newServer := func(t *testing.T) (*Conn, []net.Conn) {
		legs, peers := pipePair(2)
		s := New(legs, 16)
		s.stallTimeout = stall // before any I/O
		drainPeers(peers)
		t.Cleanup(func() { s.Close(); peers[0].Close(); peers[1].Close() })
		return s, peers
	}

	t.Run("initial", func(t *testing.T) {
		s, peers := newServer(t)
		ev := readEvents(s)
		expectQuiet(t, ev, 5*stall, "healthy initial idle")
		peers[0].Write(stripeFrame(0, "hello"))
		expectData(t, ev, "hello")
	})

	t.Run("afterDeliveredData", func(t *testing.T) {
		s, peers := newServer(t)
		ev := readEvents(s)
		peers[0].Write(stripeFrame(0, "hello"))
		expectData(t, ev, "hello")
		expectQuiet(t, ev, 5*stall, "idle after delivered data")
		peers[1].Write(stripeFrame(1, "world"))
		expectData(t, ev, "world")
	})

	t.Run("silentReverseDirection", func(t *testing.T) {
		legs, peers := pipePair(2)
		a, b := New(legs, 64), New(peers, 64)
		a.stallTimeout, b.stallTimeout = stall, stall
		defer a.Close()
		defer b.Close()
		aRead := readEvents(a) // reverse direction: b writes nothing for a while
		bRead := readEvents(b)

		for i := 0; i < 8; i++ { // forward traffic outlasts several stall timeouts
			if _, err := a.Write([]byte("ping")); err != nil {
				t.Fatalf("write: %v", err)
			}
			time.Sleep(stall / 2)
		}
		got := ""
		for len(got) < 32 {
			select {
			case e := <-bRead:
				if e.err != nil {
					t.Fatalf("forward read: %v", e.err)
				}
				got += e.data
			case <-time.After(3 * time.Second):
				t.Fatalf("forward data incomplete: %q", got)
			}
		}
		expectQuiet(t, aRead, 10*time.Millisecond, "silent reverse direction during forward traffic")
		b.Write([]byte("pong"))
		expectData(t, aRead, "pong")
	})

	t.Run("zeroLengthRead", func(t *testing.T) {
		s, _ := newServer(t)
		done := make(chan struct{})
		go func() {
			if n, err := s.Read(nil); n != 0 || err != nil {
				t.Errorf("Read(nil) = %d, %v", n, err)
			}
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("zero-length Read blocked")
		}
	})

	t.Run("closeUnblocksIdleRead", func(t *testing.T) {
		s, _ := newServer(t)
		ev := readEvents(s)
		expectQuiet(t, ev, 2*stall, "idle")
		s.Close()
		expectErr(t, ev, "", 3*time.Second)
	})
}

// TestStripedGapDeadline: a proven gap has an absolute lifetime from when it
// was first observed; later arrivals must not renew it.
func TestStripedGapDeadline(t *testing.T) {
	newServer := func(t *testing.T, stall time.Duration) (*Conn, []net.Conn) {
		legs, peers := pipePair(2)
		s := New(legs, 16)
		s.stallTimeout = stall // before any I/O
		drainPeers(peers)
		t.Cleanup(func() { s.Close(); peers[0].Close(); peers[1].Close() })
		return s, peers
	}

	t.Run("endBeforeMissingData", func(t *testing.T) {
		const stall = 200 * time.Millisecond
		s, peers := newServer(t, stall)
		ev := readEvents(s)
		peers[0].Write(stripeFrame(0, "aa"))
		expectData(t, ev, "aa")
		start := time.Now()
		peers[0].Write(stripeEnd(2)) // chunk 1 announced, never sent
		at := expectErr(t, ev, "stalled", 3*time.Second)
		if d := at.Sub(start); d < stall-10*time.Millisecond {
			t.Fatalf("gap failed after %s, before its %s deadline", d, stall)
		}
	})

	t.Run("gapClockStartsAtEvidenceNotIdle", func(t *testing.T) {
		const stall = 100 * time.Millisecond
		s, peers := newServer(t, stall)
		ev := readEvents(s)
		expectQuiet(t, ev, 4*stall, "idle before END")
		start := time.Now()
		peers[0].Write(stripeEnd(1))
		at := expectErr(t, ev, "stalled", 3*time.Second)
		if d := at.Sub(start); d < stall-10*time.Millisecond {
			t.Fatalf("idle time counted toward the gap: failed %s after END, want >= %s", d, stall)
		}
	})

	t.Run("laterSequencesDoNotRenew", func(t *testing.T) {
		const stall = 300 * time.Millisecond
		s, peers := newServer(t, stall)
		ev := readEvents(s)
		start := time.Now()
		peers[0].Write(stripeFrame(1, "x")) // sequence 0 is now provably missing
		go func() {
			for seq := uint32(2); seq < 60; seq++ { // sustained later sequences
				time.Sleep(30 * time.Millisecond)
				if _, err := peers[1].Write(stripeFrame(seq, "x")); err != nil {
					return
				}
			}
		}()
		at := expectErr(t, ev, "stalled", 3*time.Second)
		d := at.Sub(start)
		if d < stall-10*time.Millisecond || d > 2*stall {
			t.Fatalf("known gap failed after %s; want about %s, not postponed by later arrivals", d, stall)
		}
	})

	t.Run("followingGapNotGivenFreshTimeout", func(t *testing.T) {
		const stall = 400 * time.Millisecond
		s, peers := newServer(t, stall)
		ev := readEvents(s)
		start := time.Now()
		peers[0].Write(stripeFrame(3, "d")) // 0, 1, 2 missing; gap first observed now
		time.Sleep(stall * 5 / 8)
		peers[0].Write(stripeFrame(0, "a"))
		peers[0].Write(stripeFrame(1, "b")) // gap at 2 was already observable
		expectData(t, ev, "a")
		expectData(t, ev, "b")
		at := expectErr(t, ev, "stalled", 3*time.Second)
		d := at.Sub(start)
		if d < stall-10*time.Millisecond || d > stall+stall/4 {
			t.Fatalf("following gap failed after %s; want about %s from first observation, not a fresh %s", d, stall, stall)
		}
	})
}

// TestStripedReassemblyBudget: retained reassembly memory is bounded; overflow
// fails the flow loudly and promptly, and every charge is released exactly.
func TestStripedReassemblyBudget(t *testing.T) {
	const chunk = 16
	payload := strings.Repeat("p", chunk)

	newServer := func(t *testing.T, limit int64) (*Conn, []net.Conn) {
		legs, peers := pipePair(2)
		s := New(legs, chunk)
		s.budget.limit = limit // before any I/O
		drainPeers(peers)
		t.Cleanup(func() { s.Close(); peers[0].Close(); peers[1].Close() })
		return s, peers
	}
	cost := int64(retainedEntryOverhead + chunk)

	t.Run("gapPlusSustainedLaterChunksOverflows", func(t *testing.T) {
		s, peers := newServer(t, 5*cost)
		ev := readEvents(s)
		go func() {
			for seq := uint32(1); seq < 200; seq++ { // 0 never arrives
				if _, err := peers[0].Write(stripeFrame(seq, payload)); err != nil {
					return
				}
			}
		}()
		expectErr(t, ev, "reassembly budget", 2*time.Second) // default stall is 20s: this is the budget
		if peak := atomic.LoadInt64(&s.budget.peak); peak > 5*cost {
			t.Fatalf("peak retained %d exceeded the %d budget", peak, 5*cost)
		}
	})

	t.Run("singleChunkOverBudgetRejectedPromptly", func(t *testing.T) {
		s, peers := newServer(t, cost-1)
		ev := readEvents(s)
		go peers[0].Write(stripeFrame(0, payload))
		expectErr(t, ev, "reassembly budget", 2*time.Second)
	})

	t.Run("outOfOrderWithinBudgetReleasesExactly", func(t *testing.T) {
		s, peers := newServer(t, 8*cost)
		ev := readEvents(s)
		for _, seq := range []uint32{3, 2, 1, 0} {
			peers[0].Write(stripeFrame(seq, payload))
		}
		got := ""
		for len(got) < 4*chunk {
			select {
			case e := <-ev:
				if e.err != nil {
					t.Fatalf("read: %v", e.err)
				}
				got += e.data
			case <-time.After(3 * time.Second):
				t.Fatalf("only %d/%d bytes", len(got), 4*chunk)
			}
		}
		if used := atomic.LoadInt64(&s.budget.used); used != 0 {
			t.Fatalf("retained bytes after full delivery = %d, want 0", used)
		}
		if peak := atomic.LoadInt64(&s.budget.peak); peak != 4*cost {
			t.Fatalf("peak retained = %d, want exactly %d (three waiting chunks plus the arriving one)", peak, 4*cost)
		}
	})
}

// ---- Plan 023: lossless-or-loud non-FEC leg failure ----

var errInjectedLeg = errors.New("injected leg failure")

// faultLeg wraps one raw leg. wfault (per Write frame) and rfault (per Read
// call) inject failures. endSeen records that this leg has delivered an END frame.
type faultLeg struct {
	net.Conn
	wfault  func(nth int, p []byte) (drop, fail bool)
	rfault  func(nth int, endSeen bool) bool
	// gate, when non-nil, holds every Read and Write until it is closed, so a
	// test can finish configuring a Conn that New already started (stallTimeout
	// is a plain field) with a happens-before edge to its leg goroutines.
	gate    chan struct{}
	mu      sync.Mutex
	nw, nr  int
	endSeen bool
}

func (f *faultLeg) Write(p []byte) (int, error) {
	if f.gate != nil {
		<-f.gate
	}
	f.mu.Lock()
	nth := f.nw
	f.nw++
	f.mu.Unlock()
	if f.wfault != nil {
		drop, fail := f.wfault(nth, p)
		if fail {
			f.Conn.Close()
			return 0, errInjectedLeg
		}
		if drop {
			return len(p), nil // silently lost in flight
		}
	}
	return f.Conn.Write(p)
}

func (f *faultLeg) Read(p []byte) (int, error) {
	if f.gate != nil {
		<-f.gate
	}
	f.mu.Lock()
	nth, endSeen := f.nr, f.endSeen
	f.nr++
	f.mu.Unlock()
	if f.rfault != nil && f.rfault(nth, endSeen) {
		f.Conn.Close()
		return 0, errInjectedLeg
	}
	n, err := f.Conn.Read(p)
	if err == nil && isEndFrame(p[:n]) {
		f.mu.Lock()
		f.endSeen = true
		f.mu.Unlock()
	}
	return n, err
}

func isEndFrame(p []byte) bool {
	return len(p) == headerSize && binary.BigEndian.Uint32(p[4:8]) == endMarkerLen
}

// stripeWorkers counts live plain-striping worker goroutines (readLeg, writeLeg,
// rerouteWatchdog) in this process.
func stripeWorkers() int {
	buf := make([]byte, 1<<20)
	st := string(buf[:runtime.Stack(buf, true)])
	n := 0
	for _, name := range []string{"(*Conn).readLeg", "(*Conn).writeLeg", "(*Conn).rerouteWatchdog"} {
		n += strings.Count(st, name)
	}
	return n
}

// TestStripedLegFailureContract: whatever happens to a raw leg, a non-FEC striped
// flow either delivers the exact ordered payload and a clean EOF, or fails with
// an explicit non-EOF error whose partial output is an exact prefix. It never
// reports clean success with missing, duplicated or corrupted bytes. There is no
// retransmission, so surviving a failure is allowed but not required; cases that
// deterministically lose data (mustFail) must be loud.
func TestStripedLegFailureContract(t *testing.T) {
	const chunk = 512
	payload := make([]byte, 39*chunk+100) // 40 chunks, the last one short
	for i := range payload {
		payload[i] = byte(i % 251) // never 0xFF, so no payload can look like an END header
	}
	dropSeq := func(want uint32) func(leg int) func(int, []byte) (bool, bool) {
		return func(int) func(int, []byte) (bool, bool) {
			return func(_ int, p []byte) (bool, bool) {
				return len(p) >= headerSize && !isEndFrame(p) && binary.BigEndian.Uint32(p[0:4]) == want, false
			}
		}
	}

	cases := []struct {
		name     string
		send     func(leg int) func(nth int, p []byte) (drop, fail bool)
		recv     func(leg int) func(nth int, endSeen bool) bool
		mustFail bool
	}{
		{name: "legDeadBeforeData", send: func(leg int) func(int, []byte) (bool, bool) {
			return func(nth int, _ []byte) (bool, bool) { return false, leg == 0 && nth == 0 }
		}},
		{name: "legDiesMidData", send: func(leg int) func(int, []byte) (bool, bool) {
			return func(nth int, _ []byte) (bool, bool) { return false, leg == 0 && nth == 3 }
		}},
		{name: "legDiesAtEnd", send: func(leg int) func(int, []byte) (bool, bool) {
			return func(_ int, p []byte) (bool, bool) { return false, leg == 0 && isEndFrame(p) }
		}},
		{name: "legSilentlyDropsFromMidData", send: func(leg int) func(int, []byte) (bool, bool) {
			return func(nth int, _ []byte) (bool, bool) { return leg == 0 && nth >= 3, false }
		}},
		{name: "readFailsBeforeData", recv: func(leg int) func(int, bool) bool {
			return func(nth int, _ bool) bool { return leg == 0 && nth == 0 }
		}},
		{name: "readFailsMidData", recv: func(leg int) func(int, bool) bool {
			return func(nth int, _ bool) bool { return leg == 1 && nth == 6 }
		}},
		{name: "readFailsAfterEnd", recv: func(leg int) func(int, bool) bool {
			return func(_ int, endSeen bool) bool { return leg == 0 && endSeen }
		}},
		{name: "midChunkLostEverywhere", send: dropSeq(7), mustFail: true},
		{name: "tailChunkLostEverywhere", send: dropSeq(39), mustFail: true},
		{name: "endLostEverywhere", send: func(int) func(int, []byte) (bool, bool) {
			return func(_ int, p []byte) (bool, bool) { return isEndFrame(p), false }
		}, mustFail: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			const nLegs = 3
			a, b := pipePair(nLegs)
			baseline := stripeWorkers()
			gate := make(chan struct{}) // closed once both Conns are configured
			sl, rl := make([]net.Conn, nLegs), make([]net.Conn, nLegs)
			for i := 0; i < nLegs; i++ {
				s, r := &faultLeg{Conn: a[i], gate: gate}, &faultLeg{Conn: b[i], gate: gate}
				if tc.send != nil {
					s.wfault = tc.send(i)
				}
				if tc.recv != nil {
					r.rfault = tc.recv(i)
				}
				sl[i], rl[i] = s, r
			}
			sender, receiver := New(sl, chunk), New(rl, chunk)
			// Before any I/O: bound both directions well under the test timeout.
			sender.stallTimeout = 2 * time.Second
			receiver.stallTimeout = 300 * time.Millisecond
			close(gate) // leg I/O (and so every reader of stallTimeout) starts only now

			writeErr := make(chan error, 1)
			go func() {
				_, err := sender.Write(payload)
				sender.Close() // flush, END if the write side is still healthy, teardown
				writeErr <- err
			}()

			type result struct {
				got []byte
				err error
			}
			readRes := make(chan result, 1)
			go func() {
				got, err := io.ReadAll(receiver)
				readRes <- result{got, err}
			}()

			var res result
			select {
			case res = <-readRes:
			case <-time.After(5 * time.Second):
				sender.Close()
				receiver.Close()
				t.Fatal("receiver did not finish (neither exact EOF nor explicit error) within 5s")
			}
			receiver.Close()

			switch {
			case res.err == nil:
				if tc.mustFail {
					t.Fatalf("data was lost deterministically but Read reported a clean EOF (%d/%d bytes)", len(res.got), len(payload))
				}
				if !bytes.Equal(res.got, payload) {
					t.Fatalf("clean EOF without the exact payload: got %d bytes, want %d", len(res.got), len(payload))
				}
			case errors.Is(res.err, io.EOF):
				t.Fatalf("Read surfaced io.EOF as an error: %v", res.err)
			default:
				if !bytes.HasPrefix(payload, res.got) {
					t.Fatalf("failed read (%v) returned %d bytes that are not an exact prefix of the payload", res.err, len(res.got))
				}
				t.Logf("explicit failure after %d/%d exact bytes: %v", len(res.got), len(payload), res.err)
			}

			select {
			case err := <-writeErr:
				t.Logf("writer result: %v", err) // an error is acceptable; hanging is not
			case <-time.After(5 * time.Second):
				t.Fatal("writer did not finish within 5s")
			}

			// Every reader, writer and watchdog goroutine of both Conns must be gone
			// once both are closed (polled against a bounded deadline).
			deadline := time.Now().Add(3 * time.Second)
			for stripeWorkers() > baseline {
				if time.Now().After(deadline) {
					t.Fatalf("striping workers still running after Close: %d, baseline %d", stripeWorkers(), baseline)
				}
				runtime.Gosched()
				time.Sleep(time.Millisecond)
			}
		})
	}
}
