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
