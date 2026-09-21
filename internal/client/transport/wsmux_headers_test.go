package transport

import (
	"io"
	"net"
	"testing"

	"github.com/musix/backhaul/internal/utils"
	"github.com/sirupsen/logrus"
	"github.com/xtaci/smux"
)

// newHeaderTestTransport returns a transport with just enough state for the
// stripe-group bookkeeping. Nothing here dials a target: addStripeLeg only
// files streams, it never hands them to localDialer.
func newHeaderTestTransport(t *testing.T) *WsMuxTransport {
	t.Helper()
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	c := &WsMuxTransport{
		logger:       logger,
		stripeGroups: make(map[uint32]*stripeGroup),
	}
	t.Cleanup(func() {
		c.stripeGroupsMu.Lock()
		defer c.stripeGroupsMu.Unlock()
		for _, g := range c.stripeGroups {
			g.timer.Stop()
		}
	})
	return c
}

// newTestStream opens a real smux stream over an in-memory net.Pipe pair.
func newTestStream(t *testing.T) *smux.Stream {
	t.Helper()
	a, b := net.Pipe()
	cfg := smux.DefaultConfig()
	cfg.KeepAliveDisabled = true
	srv, err := smux.Server(a, cfg)
	if err != nil {
		t.Fatalf("smux.Server: %v", err)
	}
	cli, err := smux.Client(b, cfg)
	if err != nil {
		t.Fatalf("smux.Client: %v", err)
	}
	t.Cleanup(func() { cli.Close(); srv.Close() })
	st, err := cli.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	return st
}

// streamClosed reports whether the stream was closed by the code under test.
func streamClosed(st *smux.Stream) bool {
	_, err := st.Write([]byte{0})
	return err != nil
}

func groupOf(c *WsMuxTransport, id uint32) *stripeGroup {
	c.stripeGroupsMu.Lock()
	defer c.stripeGroupsMu.Unlock()
	return c.stripeGroups[id]
}

func TestWSMuxHeaderBounds(t *testing.T) {
	cases := []struct {
		name                 string
		index, total, parity uint8
	}{
		{"total zero", 0, 0, 0},
		{"index equals total", 3, 3, 0},
		{"index above total", 200, 3, 0},
		{"parity equals total", 0, 3, 3},
		{"parity above total", 0, 3, 255},
		{"max index max total", 255, 255, 0},
	}
	for _, kind := range []byte{utils.FlowStriped, utils.FlowPromote} {
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				c := newHeaderTestTransport(t)
				st := newTestStream(t)
				g, complete := c.addStripeLeg(st, kind, 1, 42, tc.index, tc.total, tc.parity, "")
				if g != nil || complete {
					t.Fatalf("invalid header accepted: g=%v complete=%v", g, complete)
				}
				if groupOf(c, 42) != nil {
					t.Fatal("invalid header created a group")
				}
				if !streamClosed(st) {
					t.Fatal("rejected stream was not closed")
				}
			})
		}
	}
}

func TestWSMuxHeaderMaxWidthAccepted(t *testing.T) {
	c := newHeaderTestTransport(t)
	st := newTestStream(t)
	if _, complete := c.addStripeLeg(st, utils.FlowStriped, 0, 1, 254, 255, 0, "x:1"); complete {
		t.Fatal("one leg of 255 must not complete the group")
	}
	g := groupOf(c, 1)
	if g == nil || len(g.streams) != 255 || g.streams[254] != st || g.remaining != 254 {
		t.Fatalf("unexpected group state: %+v", g)
	}
}

// pendingGroup files leg 0 of a 3-leg group and returns its stream.
func pendingGroup(t *testing.T, c *WsMuxTransport, kind byte, flowID uint64, id uint32, parity uint8, addr string) *smux.Stream {
	t.Helper()
	st := newTestStream(t)
	if _, complete := c.addStripeLeg(st, kind, flowID, id, 0, 3, parity, addr); complete {
		t.Fatal("first leg completed a 3-leg group")
	}
	return st
}

// assertPendingIntact checks a rejected leg neither closed nor disturbed the
// valid pending group.
func assertPendingIntact(t *testing.T, c *WsMuxTransport, id uint32, first *smux.Stream, rejected *smux.Stream) {
	t.Helper()
	if !streamClosed(rejected) {
		t.Error("rejected stream was not closed")
	}
	g := groupOf(c, id)
	if g == nil {
		t.Fatal("pending group was removed by a rejected leg")
	}
	c.stripeGroupsMu.Lock()
	defer c.stripeGroupsMu.Unlock()
	if g.remaining != 2 || g.streams[0] != first || g.streams[1] != nil || g.streams[2] != nil {
		t.Errorf("pending group mutated: remaining=%d", g.remaining)
	}
	if streamClosed(first) {
		t.Error("valid pending leg was closed by a rejected leg")
	}
}

func TestWSMuxHeaderSameIDShapeMismatch(t *testing.T) {
	cases := []struct {
		name          string
		total, parity uint8
		addr          string
	}{
		{"larger total", 4, 1, "a:1"},
		{"smaller total", 2, 1, "a:1"},
		{"different parity", 3, 0, "a:1"},
		{"different destination", 3, 1, "b:2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newHeaderTestTransport(t)
			first := pendingGroup(t, c, utils.FlowStriped, 0, 7, 1, "a:1")
			bad := newTestStream(t)
			if g, complete := c.addStripeLeg(bad, utils.FlowStriped, 0, 7, 1, tc.total, tc.parity, tc.addr); g != nil || complete {
				t.Fatal("mismatching leg accepted")
			}
			assertPendingIntact(t, c, 7, first, bad)
		})
	}
}

func TestWSMuxHeaderKindMismatch(t *testing.T) {
	t.Run("promote leg into striped group", func(t *testing.T) {
		c := newHeaderTestTransport(t)
		first := pendingGroup(t, c, utils.FlowStriped, 0, 9, 0, "")
		bad := newTestStream(t)
		if g, _ := c.addStripeLeg(bad, utils.FlowPromote, 5, 9, 1, 3, 0, ""); g != nil {
			t.Fatal("kind mismatch accepted")
		}
		assertPendingIntact(t, c, 9, first, bad)
	})
	t.Run("striped leg into promote group", func(t *testing.T) {
		c := newHeaderTestTransport(t)
		first := pendingGroup(t, c, utils.FlowPromote, 5, 9, 0, "")
		bad := newTestStream(t)
		if g, _ := c.addStripeLeg(bad, utils.FlowStriped, 0, 9, 1, 3, 0, ""); g != nil {
			t.Fatal("kind mismatch accepted")
		}
		assertPendingIntact(t, c, 9, first, bad)
	})
}

func TestWSMuxHeaderPromotionFlowMismatch(t *testing.T) {
	c := newHeaderTestTransport(t)
	first := pendingGroup(t, c, utils.FlowPromote, 5, 11, 0, "")
	bad := newTestStream(t)
	if g, _ := c.addStripeLeg(bad, utils.FlowPromote, 6, 11, 1, 3, 0, ""); g != nil {
		t.Fatal("promotion flow mismatch accepted")
	}
	assertPendingIntact(t, c, 11, first, bad)
}

func TestWSMuxHeaderDuplicateLeg(t *testing.T) {
	c := newHeaderTestTransport(t)
	first := pendingGroup(t, c, utils.FlowStriped, 0, 13, 0, "a:1")
	dup := newTestStream(t)
	if g, complete := c.addStripeLeg(dup, utils.FlowStriped, 0, 13, 0, 3, 0, "a:1"); g != nil || complete {
		t.Fatal("duplicate leg accepted")
	}
	assertPendingIntact(t, c, 13, first, dup)
}

func TestWSMuxHeaderTimerIdentity(t *testing.T) {
	c := newHeaderTestTransport(t)

	// Group A completes (two legs) and leaves the map.
	s0, s1 := newTestStream(t), newTestStream(t)
	c.addStripeLeg(s0, utils.FlowStriped, 0, 21, 0, 2, 0, "a:1")
	oldG := groupOf(c, 21)
	if oldG == nil {
		t.Fatal("group A missing")
	}
	if g, complete := c.addStripeLeg(s1, utils.FlowStriped, 0, 21, 1, 2, 0, "a:1"); g != oldG || !complete {
		t.Fatal("group A did not complete")
	}
	oldG.timer.Stop()

	// Group B reuses the numeric ID and is still pending.
	b0 := newTestStream(t)
	c.addStripeLeg(b0, utils.FlowStriped, 0, 21, 0, 2, 0, "b:2")
	newG := groupOf(c, 21)
	if newG == nil || newG == oldG {
		t.Fatal("group B not created as a new instance")
	}

	// A's stale timer fires late: it must not touch B.
	c.abortStripeGroup(21, oldG)
	if groupOf(c, 21) != newG {
		t.Fatal("stale timer removed the newer group")
	}
	if streamClosed(b0) {
		t.Fatal("stale timer closed a leg of the newer group")
	}

	// B's own timeout still works and closes its legs.
	c.abortStripeGroup(21, newG)
	if groupOf(c, 21) != nil {
		t.Fatal("group B was not aborted by its own timer")
	}
	if !streamClosed(b0) {
		t.Fatal("group B leg not closed on abort")
	}
}

func TestWSMuxHeaderValidAssembly(t *testing.T) {
	for _, tc := range []struct {
		name   string
		kind   byte
		flow   uint64
		parity uint8
	}{
		{"striped plain", utils.FlowStriped, 0, 0},
		{"striped fec", utils.FlowStriped, 0, 1},
		{"promote plain", utils.FlowPromote, 77, 0},
		{"promote fec", utils.FlowPromote, 77, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newHeaderTestTransport(t)
			legs := []*smux.Stream{newTestStream(t), newTestStream(t), newTestStream(t)}
			var got *stripeGroup
			for n, i := range []uint8{2, 0, 1} { // arrival order differs from index order
				g, complete := c.addStripeLeg(legs[i], tc.kind, tc.flow, 30, i, 3, tc.parity, "")
				if complete != (n == 2) {
					t.Fatalf("leg %d: complete=%v", i, complete)
				}
				if complete {
					got = g
				}
			}
			if got == nil || got.parity != tc.parity {
				t.Fatalf("assembled group wrong: %+v", got)
			}
			for i, st := range got.streams {
				if st != legs[i] {
					t.Fatalf("leg %d filed in wrong slot", i)
				}
			}
			if groupOf(c, 30) != nil {
				t.Fatal("completed group still in map")
			}
			got.timer.Stop()
		})
	}
}
