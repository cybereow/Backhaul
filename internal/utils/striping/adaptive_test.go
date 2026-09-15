package striping

import (
	"bytes"
	"crypto/rand"
	"io"
	"net"
	"testing"
	"time"
)

// TestReevaluateQuarantinesSlowLeg checks the leg-scheduling core: a leg whose
// throughput has fallen below quarantineFraction of the fastest is quarantined
// (kept off the sequential path), faster legs stay active, and an unmeasured leg
// is left active so it can be measured.
func TestReevaluateQuarantinesSlowLeg(t *testing.T) {
	c := &Conn{sched: make([]legSched, 4)}
	c.sched[0].rate = 1000 // fast
	c.sched[1].rate = 900  // fast
	c.sched[2].rate = 100  // < 0.25*1000 -> quarantine
	c.sched[3].rate = 0    // unmeasured -> stay active

	c.schedMu.Lock()
	c.reevaluateLocked()
	c.schedMu.Unlock()

	want := []bool{false, false, true, false}
	for i, w := range want {
		if c.sched[i].quar != w {
			t.Errorf("leg %d quarantined=%v, want %v (rate=%.0f)", i, c.sched[i].quar, w, c.sched[i].rate)
		}
	}
}

// TestReevaluateNeverQuarantinesEveryLeg guards the deadlock-safety invariant:
// even if every leg looks slow relative to some peak, at least one leg must stay
// active so the flow always has a writer.
func TestReevaluateNeverQuarantinesEveryLeg(t *testing.T) {
	c := &Conn{sched: make([]legSched, 3)}
	// Contrived: all three measured very low, but one is the (relative) best.
	c.sched[0].rate = 10
	c.sched[1].rate = 5
	c.sched[2].rate = 1

	c.schedMu.Lock()
	c.reevaluateLocked()
	c.schedMu.Unlock()

	active := 0
	for i := range c.sched {
		if !c.sched[i].quar {
			active++
		}
	}
	if active == 0 {
		t.Fatal("every leg quarantined: the flow would have no writer")
	}
	if c.sched[0].quar {
		t.Error("the fastest leg must never be quarantined")
	}
}

// TestEnsureActivePrefersNonFrozenLeg guards the reactivation rule: when a stall
// leaves every leg quarantined, the leg brought back must not be one frozen
// mid-write (it couldn't service work), even if it has the higher historical
// rate - otherwise the reroute would have no live carrier.
func TestEnsureActivePrefersNonFrozenLeg(t *testing.T) {
	c := &Conn{sched: make([]legSched, 2)}
	// Leg 0: faster, but frozen mid-write (the one scanStuck just quarantined).
	c.sched[0] = legSched{rate: 1000, quar: true, inFly: true}
	// Leg 1: slower, quarantined, but idle and able to carry the reroute.
	c.sched[1] = legSched{rate: 200, quar: true, inFly: false}

	c.schedMu.Lock()
	c.ensureActiveLocked()
	c.schedMu.Unlock()

	if !c.sched[0].quar {
		t.Error("the frozen (in-flight) leg must not be the one reactivated")
	}
	if c.sched[1].quar {
		t.Error("the idle leg should be reactivated so it can carry the reroute")
	}
}

// TestEnsureActiveFallsBackWhenAllFrozen checks the fallback: if every leg is in
// flight, one is still reactivated so the flow never ends up with zero writers.
func TestEnsureActiveFallsBackWhenAllFrozen(t *testing.T) {
	c := &Conn{sched: make([]legSched, 2)}
	c.sched[0] = legSched{rate: 1000, quar: true, inFly: true}
	c.sched[1] = legSched{rate: 200, quar: true, inFly: true}

	c.schedMu.Lock()
	c.ensureActiveLocked()
	c.schedMu.Unlock()

	active := 0
	for i := range c.sched {
		if !c.sched[i].quar {
			active++
		}
	}
	if active == 0 {
		t.Fatal("no active leg after reactivation: the flow would stall")
	}
}

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
// ~5x asymmetry seen in the per-CDN speedtest). The adaptive scheduler should
// quarantine the slow leg so the aggregate tracks the fast legs' summed rate
// rather than collapsing toward the slow one, and the payload must arrive intact.
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
