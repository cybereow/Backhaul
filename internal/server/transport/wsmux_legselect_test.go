package transport

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestShouldPromoteGatesOnFlowCount guards the fix that keeps promotion from
// collapsing a many-flow upload: promote a single/few heavy flows, but stay
// plain (already spread across CDNs by load-balancing) once many flows are
// active, so promotion can't over-subscribe the pool.
func TestShouldPromoteGatesOnFlowCount(t *testing.T) {
	s := &WsMuxTransport{config: &WsMuxConfig{StripeFactor: 3, StripeParity: 1}} // legsPerFlow = 4
	for i := 0; i < 12; i++ {
		s.sessions = append(s.sessions, &pooledSession{cdn: fmt.Sprintf("cdn%d", i)})
	}
	// budget = distinctCDNs(12) / legsPerFlow(4) = 3.
	atomic.StoreInt32(&s.plainFlows, 2)
	if !s.shouldPromote() {
		t.Error("few flows (2 <= budget 3) should be allowed to promote")
	}
	atomic.StoreInt32(&s.plainFlows, 10)
	if s.shouldPromote() {
		t.Error("many flows (10 > budget 3) should stay plain instead of promoting")
	}
}

// fakeScore lets the selection tests rank sessions without a live smux session
// (legScore reads NumStreams off a real session). Each pooledSession is scored
// by a value stashed in its rtt field, purely as a test handle.
func fakeScore(ps *pooledSession) float64 {
	return float64(ps.rtt.Load())
}

func mkSession(cdn string, score int64) *pooledSession {
	ps := &pooledSession{cdn: cdn}
	ps.rtt.Store(score)
	return ps
}

// cdnsOf returns the CDN of each chosen leg, in order.
func cdnsOf(sessions []*pooledSession) []string {
	out := make([]string, len(sessions))
	for i, ps := range sessions {
		out[i] = ps.cdn
	}
	return out
}

func TestSelectLegsPrefersDistinctCDNs(t *testing.T) {
	// cdnA holds the two cheapest sessions, but a striped flow of 2 legs should
	// still take one from cdnA and one from cdnB rather than doubling up on cdnA.
	avail := []*pooledSession{
		mkSession("cdnA", 1),
		mkSession("cdnA", 2),
		mkSession("cdnB", 5),
		mkSession("cdnC", 9),
	}

	chosen := selectLegs(avail, 2, fakeScore)
	if len(chosen) != 2 {
		t.Fatalf("want 2 legs, got %d", len(chosen))
	}
	got := cdnsOf(chosen)
	if got[0] != "cdnA" {
		t.Errorf("first leg should be the best-scoring CDN (cdnA), got %q", got[0])
	}
	if got[1] == "cdnA" {
		t.Errorf("second leg should come from a different CDN, got another cdnA: %v", got)
	}
	if got[1] != "cdnB" {
		t.Errorf("second leg should be the next-best distinct CDN (cdnB), got %q", got[1])
	}
}

func TestSelectLegsFillsBeyondCDNCount(t *testing.T) {
	// Only two CDNs but the flow wants 3 legs: pass 1 takes one per CDN (2 legs),
	// pass 2 fills the last slot with the next-best session regardless of CDN.
	avail := []*pooledSession{
		mkSession("cdnA", 1),
		mkSession("cdnA", 3),
		mkSession("cdnB", 2),
	}

	chosen := selectLegs(avail, 3, fakeScore)
	if len(chosen) != 3 {
		t.Fatalf("want 3 legs, got %d", len(chosen))
	}
	got := cdnsOf(chosen)
	// Best-first, distinct CDNs first: cdnA(1), cdnB(2), then the leftover cdnA(3).
	want := []string{"cdnA", "cdnB", "cdnA"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("leg %d: want CDN %q, got %q (full order %v)", i, want[i], got[i], got)
		}
	}
}

func TestSelectLegsCapsAtAvailable(t *testing.T) {
	avail := []*pooledSession{
		mkSession("cdnA", 1),
		mkSession("cdnB", 2),
	}
	chosen := selectLegs(avail, 5, fakeScore)
	if len(chosen) != 2 {
		t.Fatalf("want 2 legs (all available), got %d", len(chosen))
	}
}

// TestLegScoreCapacityWeighted guards the capacity-weighted selection: placement
// cost is (load+1)*RTT, so a fast CDN keeps absorbing streams (~1/RTT of them)
// before a slower CDN wins, biasing the spread toward the good paths instead of
// balancing stream count evenly (which diluted aggregate throughput into slow
// CDNs). It must still spread - never concentrate every stream on one leg - and
// still prefer the lower-RTT leg at equal load.
func TestLegScoreCapacityWeighted(t *testing.T) {
	fast := int64(20 * time.Millisecond)
	slow := int64(90 * time.Millisecond) // ~4.5x the RTT

	// A fast CDN already carrying a few streams still outranks a slower idle CDN:
	// we keep feeding the fast path rather than spilling onto the slow one early.
	// (3+1)*20 = 80 < (0+1)*90 = 90.
	if !(legScoreValue(3, fast) < legScoreValue(0, slow)) {
		t.Errorf("a fast CDN with a few streams should still beat a slower idle CDN: fast=%v slow=%v",
			legScoreValue(3, fast), legScoreValue(0, slow))
	}
	// But it must eventually spill to the slower CDN - the cost grows with load,
	// so selection can never pile every stream onto one leg. (5+1)*20 = 120 > 90.
	if !(legScoreValue(5, fast) > legScoreValue(0, slow)) {
		t.Errorf("a fast CDN loaded enough must spill to a slower idle CDN (weighted spread, not concentration): fast=%v slow=%v",
			legScoreValue(5, fast), legScoreValue(0, slow))
	}
	// Among equally-loaded sessions, lower RTT still wins (the least-latency pref).
	if !(legScoreValue(2, fast) < legScoreValue(2, slow)) {
		t.Error("with equal load, the lower-RTT session should win")
	}
}

// TestOpenStripedLegsRequiresFullWidth guards the fix for the silent stripe-width
// reduction: asking for more legs than there are live sessions must error (so the
// caller waits / stays plain), not quietly build a narrower group that leaves the
// two ends disagreeing on the FEC data-shard count.
func TestOpenStripedLegsRequiresFullWidth(t *testing.T) {
	s := &WsMuxTransport{}
	s.sessions = []*pooledSession{{cdn: "a"}} // one live session (nil smux is fine: the width check returns before touching it)

	legs, err := s.openStripedLegs(2)
	if err == nil {
		t.Fatal("expected an error when fewer sessions are live than the requested stripe width")
	}
	if len(legs) != 0 {
		t.Fatalf("expected 0 legs on error, got %d", len(legs))
	}
}

func TestBestPerCDN(t *testing.T) {
	// Two CDNs with two sessions each; bestPerCDN keeps one per CDN, the
	// best-scoring one, ordered best-first. (Scores are stashed in rtt; the real
	// bestPerCDN scores via legScore, but selection order here is exercised
	// through the same distinct-CDN logic selectLegs uses.)
	avail := []*pooledSession{
		mkSession("cdnA", 5),
		mkSession("cdnA", 1), // best A
		mkSession("cdnB", 8),
		mkSession("cdnB", 3), // best B
	}
	got := bestPerCDN(avail, fakeScore)
	if len(got) != 2 {
		t.Fatalf("want one session per distinct CDN (2), got %d", len(got))
	}
	seen := map[string]bool{}
	for _, ps := range got {
		if seen[ps.cdn] {
			t.Fatalf("bestPerCDN returned a duplicate CDN: %v", cdnsOf(got))
		}
		seen[ps.cdn] = true
	}
	if !seen["cdnA"] || !seen["cdnB"] {
		t.Errorf("both CDNs should be represented, got %v", cdnsOf(got))
	}
}

func TestLegScoreValue(t *testing.T) {
	// Lower load and lower RTT must score lower (better).
	rtt := int64(10 * time.Millisecond)
	loaded := legScoreValue(5, rtt)
	idle := legScoreValue(0, rtt)
	if !(idle < loaded) {
		t.Errorf("an idle session should score better than a loaded one: idle=%v loaded=%v", idle, loaded)
	}

	fast := legScoreValue(2, int64(5*time.Millisecond))
	slow := legScoreValue(2, int64(80*time.Millisecond))
	if !(fast < slow) {
		t.Errorf("a lower-RTT session should score better: fast=%v slow=%v", fast, slow)
	}

	// An unprobed session (rtt 0) is charged the neutral default, not treated as
	// zero-latency (which would make it always win).
	unprobed := legScoreValue(0, 0)
	if unprobed != unprobedRTTms {
		t.Errorf("unprobed score = %v, want %v", unprobed, unprobedRTTms)
	}
	if unprobed <= legScoreValue(0, int64(1*time.Millisecond)) {
		t.Errorf("unprobed session should not out-rank a measured 1ms session")
	}
}
