package striping

import (
	"bytes"
	"crypto/rand"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// boundedPipe is an in-memory net.Conn pair with a *bounded* internal buffer:
// Write blocks once `capacity` bytes are outstanding, exactly like a real
// transport's flow-control window. This is the ingredient the earlier unbounded
// pipe was missing - with an unbounded buffer the fast legs could run infinitely
// far ahead of a slow one and hide any head-of-line stall.
func boundedPipe(capacity int) (net.Conn, net.Conn) {
	a2b := newBoundedBuf(capacity)
	b2a := newBoundedBuf(capacity)
	return &memConn2{r: b2a, w: a2b}, &memConn2{r: a2b, w: b2a}
}

// latencyConn delays each Write by a fixed amount, and a bandwidth cap by
// sleeping proportional to the bytes written, so one leg can be made both slower
// and higher-latency than the others.
type latencyConn struct {
	net.Conn
	delay       time.Duration
	bytesPerSec int
}

func (c *latencyConn) Write(p []byte) (int, error) {
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	if c.bytesPerSec > 0 {
		time.Sleep(time.Duration(float64(len(p)) / float64(c.bytesPerSec) * float64(time.Second)))
	}
	return c.Conn.Write(p)
}

// stallingConn simulates a leg that periodically freezes, the way a real
// TCP/TLS leg does while it waits out a retransmission timeout on a lossy path:
// throughput is otherwise fine, but every `everyBytes` it blocks for `stallFor`.
// This is the impairment a clean localhost pipe lacks and the most likely reason
// an egress path (with real loss) behaves differently from an ingress one.
type stallingConn struct {
	net.Conn
	everyBytes int
	stallFor   time.Duration
	written    int
}

func (c *stallingConn) Write(p []byte) (int, error) {
	c.written += len(p)
	if c.written >= c.everyBytes {
		c.written = 0
		time.Sleep(c.stallFor)
	}
	return c.Conn.Write(p)
}

// TestStripingStallingLeg is the impairment repro: one leg periodically stalls
// (retransmit-style) while the others run clean. It measures plain striping's
// aggregate throughput to see whether an in-order-reassembled flow collapses
// toward a stalling leg - the suspected mechanism behind the egress-direction
// upload collapse.
func TestStripingStallingLeg(t *testing.T) {
	const (
		nLegs       = 4
		window      = 256 * 1024
		payloadSize = 12 << 20
	)
	serverLegs := make([]net.Conn, nLegs)
	clientLegs := make([]net.Conn, nLegs)
	for i := 0; i < nLegs; i++ {
		srv, cli := boundedPipe(window)
		serverLegs[i] = srv
		clientLegs[i] = cli
	}
	// One leg stalls ~50ms every 256KB it sends: ~5 stalls/MB.
	serverLegs[0] = &stallingConn{Conn: serverLegs[0], everyBytes: 256 * 1024, stallFor: 50 * time.Millisecond}

	server := New(serverLegs, DefaultChunkSize)
	client := New(clientLegs, DefaultChunkSize)

	payload := make([]byte, payloadSize)
	_, _ = rand.Read(payload)

	recv := make(chan []byte, 1)
	go func() { got, _ := io.ReadAll(client); recv <- got }()
	start := time.Now()
	go func() { _, _ = server.Write(payload); server.Close() }()
	got := <-recv
	elapsed := time.Since(start)

	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: got %d, want %d", len(got), len(payload))
	}
	mbps := float64(len(got)) * 8 / elapsed.Seconds() / 1e6
	t.Logf("plain striping with a stalling leg: %.1f Mbps over %s", mbps, elapsed)
}

// TestFECStallingLeg is the FEC counterpart: with 3 data + 1 parity shards, a
// row can be reconstructed from any 3 of the 4 legs, so a single stalling leg
// should never gate the flow (the parity leg covers it). This checks the FEC
// path doesn't wait on the stalling leg it is supposed to be able to skip.
func TestFECStallingLeg(t *testing.T) {
	const (
		dataShards   = 3
		parityShards = 1
		nLegs        = dataShards + parityShards
		window       = 256 * 1024
		payloadSize  = 8 << 20
	)
	serverLegs := make([]net.Conn, nLegs)
	clientLegs := make([]net.Conn, nLegs)
	for i := 0; i < nLegs; i++ {
		srv, cli := boundedPipe(window)
		serverLegs[i] = srv
		clientLegs[i] = cli
	}
	serverLegs[0] = &stallingConn{Conn: serverLegs[0], everyBytes: 256 * 1024, stallFor: 50 * time.Millisecond}

	server, err := NewFEC(serverLegs, DefaultChunkSize, dataShards, parityShards)
	if err != nil {
		t.Fatalf("server NewFEC: %v", err)
	}
	client, err := NewFEC(clientLegs, DefaultChunkSize, dataShards, parityShards)
	if err != nil {
		t.Fatalf("client NewFEC: %v", err)
	}

	payload := make([]byte, payloadSize)
	_, _ = rand.Read(payload)
	recv := make(chan []byte, 1)
	go func() { got, _ := io.ReadAll(client); recv <- got }()
	start := time.Now()
	go func() { _, _ = server.Write(payload); server.Close() }()
	got := <-recv
	elapsed := time.Since(start)

	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: got %d, want %d", len(got), len(payload))
	}
	mbps := float64(len(got)) * 8 / elapsed.Seconds() / 1e6
	t.Logf("FEC (3+1) with a stalling leg: %.1f Mbps over %s", mbps, elapsed)
}

// TestStripingBoundedBufferSlowLeg drives a transfer over four legs, all with the
// same modest flow-control window, one of them throttled and higher-latency. It
// checks the aggregate still clears well above the slow leg's own rate - the
// head-of-line failure is when in-order reassembly plus a bounded window let one
// lagging leg gate the whole flow.
func TestStripingBoundedBufferSlowLeg(t *testing.T) {
	const (
		nLegs       = 4
		window      = 256 * 1024 // per-leg flow-control window
		slowLegBps  = 4 << 20    // 4 MB/s == 32 Mbps
		slowLegRTT  = 40 * time.Millisecond
		payloadSize = 12 << 20
	)

	serverLegs := make([]net.Conn, nLegs)
	clientLegs := make([]net.Conn, nLegs)
	for i := 0; i < nLegs; i++ {
		srv, cli := boundedPipe(window)
		serverLegs[i] = srv
		clientLegs[i] = cli
	}
	serverLegs[0] = &latencyConn{Conn: serverLegs[0], delay: slowLegRTT, bytesPerSec: slowLegBps}

	server := New(serverLegs, DefaultChunkSize)
	client := New(clientLegs, DefaultChunkSize)

	payload := make([]byte, payloadSize)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}

	recv := make(chan []byte, 1)
	recvErr := make(chan error, 1)
	go func() {
		got, err := io.ReadAll(client)
		recvErr <- err
		recv <- got
	}()

	start := time.Now()
	go func() {
		_, _ = server.Write(payload)
		server.Close()
	}()

	got := <-recv
	if err := <-recvErr; err != nil {
		t.Fatalf("read: %v", err)
	}
	elapsed := time.Since(start)

	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: got %d bytes, want %d", len(got), len(payload))
	}

	mbps := float64(len(got)) * 8 / elapsed.Seconds() / 1e6
	slowMbps := float64(slowLegBps) * 8 / 1e6
	t.Logf("bounded-buffer throughput %.1f Mbps over %s (slow leg alone = %.0f Mbps, window=%dKB)", mbps, elapsed, slowMbps, window/1024)

	if mbps < slowMbps*1.5 {
		t.Errorf("aggregate throughput %.1f Mbps collapsed toward the slow leg (%.0f Mbps): head-of-line blocking under a bounded window", mbps, slowMbps)
	}
}

// --- bounded in-memory conn plumbing ---

type memConn2 struct {
	r *boundedBuf
	w *boundedBuf
}

func (c *memConn2) Read(p []byte) (int, error)         { return c.r.Read(p) }
func (c *memConn2) Write(p []byte) (int, error)        { return c.w.Write(p) }
func (c *memConn2) Close() error                       { c.w.close(); c.r.close(); return nil }
func (c *memConn2) LocalAddr() net.Addr                { return pipeAddr2{} }
func (c *memConn2) RemoteAddr() net.Addr               { return pipeAddr2{} }
func (c *memConn2) SetDeadline(t time.Time) error      { return nil }
func (c *memConn2) SetReadDeadline(t time.Time) error  { return nil }
func (c *memConn2) SetWriteDeadline(t time.Time) error { return nil }

type pipeAddr2 struct{}

func (pipeAddr2) Network() string { return "mem" }
func (pipeAddr2) String() string  { return "mem" }

type boundedBuf struct {
	mu       sync.Mutex
	notEmpty *sync.Cond
	notFull  *sync.Cond
	buf      []byte
	capacity int
	closed   bool
}

func newBoundedBuf(capacity int) *boundedBuf {
	b := &boundedBuf{capacity: capacity}
	b.notEmpty = sync.NewCond(&b.mu)
	b.notFull = sync.NewCond(&b.mu)
	return b
}

func (b *boundedBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	written := 0
	for written < len(p) {
		for len(b.buf) >= b.capacity && !b.closed {
			b.notFull.Wait()
		}
		if b.closed {
			return written, io.ErrClosedPipe
		}
		space := b.capacity - len(b.buf)
		n := len(p) - written
		if n > space {
			n = space
		}
		b.buf = append(b.buf, p[written:written+n]...)
		written += n
		b.notEmpty.Signal()
	}
	return written, nil
}

func (b *boundedBuf) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for len(b.buf) == 0 && !b.closed {
		b.notEmpty.Wait()
	}
	if len(b.buf) == 0 && b.closed {
		return 0, io.EOF
	}
	n := copy(p, b.buf)
	b.buf = b.buf[n:]
	b.notFull.Signal()
	return n, nil
}

func (b *boundedBuf) close() {
	b.mu.Lock()
	b.closed = true
	b.notEmpty.Broadcast()
	b.notFull.Broadcast()
	b.mu.Unlock()
}
