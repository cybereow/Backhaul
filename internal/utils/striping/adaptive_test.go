package striping

import (
	"bytes"
	"crypto/rand"
	"io"
	"net"
	"testing"
	"time"
)

// TestStashDropsDuplicates verifies the receiver dedups a re-sent sequence
// number, so the reroute path can safely deliver the same chunk on two legs.
func TestStashDropsDuplicates(t *testing.T) {
	c := &Conn{pending: make(map[uint32]chunk)}

	// A duplicate of an already-delivered seq (< nextSeq) is dropped.
	c.nextSeq = 5
	c.stash(chunk{seq: 3, data: []byte("old")})
	if _, ok := c.pending[3]; ok {
		t.Error("already-delivered duplicate was stashed")
	}

	// The first out-of-order copy is kept; a second copy of the same seq is
	// dropped rather than overwriting.
	c.stash(chunk{seq: 7, data: []byte("first")})
	c.stash(chunk{seq: 7, data: []byte("second")})
	if got := string(c.pending[7].data); got != "first" {
		t.Errorf("duplicate overwrote the pending chunk: got %q, want %q", got, "first")
	}

	// The next-in-line seq becomes the read buffer and advances nextSeq.
	c.stash(chunk{seq: 5, data: []byte("next")})
	if string(c.readBuf) != "next" || c.nextSeq != 6 {
		t.Errorf("in-order chunk mis-stashed: readBuf=%q nextSeq=%d", c.readBuf, c.nextSeq)
	}
}

// throttleConn caps a leg's throughput to bytesPerSec by sleeping proportional
// to each write, modelling a persistently throttled CDN egress leg.
type throttleConn struct {
	net.Conn
	bytesPerSec int
}

func (c *throttleConn) Write(p []byte) (int, error) {
	time.Sleep(time.Duration(float64(len(p)) / float64(c.bytesPerSec) * float64(time.Second)))
	return c.Conn.Write(p)
}

// TestAdaptiveRoutesAroundThrottledLeg is the end-to-end analogue of the
// production report: several fast legs and one persistently throttled leg (the
// ~5x asymmetry seen in the per-CDN speedtest). Each leg takes a chunk when it
// can send one, so the throttled leg carries only its share and the aggregate
// tracks the legs' summed rate rather than collapsing toward the slow one, and
// the payload must arrive intact.
func TestAdaptiveRoutesAroundThrottledLeg(t *testing.T) {
	const (
		nLegs       = 5
		window      = 256 * 1024
		slowBps     = 5 << 20 // 5 MB/s == 40 Mbps, ~1/5 of a fast leg
		payloadSize = 16 << 20
	)
	serverLegs := make([]net.Conn, nLegs)
	clientLegs := make([]net.Conn, nLegs)
	for i := 0; i < nLegs; i++ {
		srv, cli := boundedPipe(window)
		serverLegs[i] = srv
		clientLegs[i] = cli
	}
	serverLegs[0] = &throttleConn{Conn: serverLegs[0], bytesPerSec: slowBps}

	server := New(serverLegs, DefaultChunkSize)
	client := New(clientLegs, DefaultChunkSize)

	payload := make([]byte, payloadSize)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}

	recv := make(chan []byte, 1)
	go func() { got, _ := io.ReadAll(client); recv <- got }()
	start := time.Now()
	go func() { _, _ = server.Write(payload); server.Close() }()
	got := <-recv
	elapsed := time.Since(start)

	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: got %d bytes, want %d", len(got), len(payload))
	}
	mbps := float64(len(got)) * 8 / elapsed.Seconds() / 1e6
	slowMbps := float64(slowBps) * 8 / 1e6
	t.Logf("adaptive striping: %.0f Mbps over %s (throttled leg alone = %.0f Mbps)", mbps, elapsed, slowMbps)
	// 1.5x the throttled leg's own rate is the same race-robust bar the sibling
	// TestStripingBoundedBufferSlowLeg uses: the race detector's timing
	// distortion depresses the absolute number, but a genuine collapse pins the
	// aggregate at ~1x the slow leg, which this still catches.
	if mbps < slowMbps*1.5 {
		t.Errorf("aggregate %.0f Mbps collapsed toward the throttled leg (%.0f Mbps)", mbps, slowMbps)
	}
}
