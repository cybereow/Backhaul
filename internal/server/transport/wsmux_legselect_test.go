package transport

import (
	"testing"
	"time"
)

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
	if unprobed != unprobedRTTms*legRTTWeight {
		t.Errorf("unprobed score = %v, want %v", unprobed, unprobedRTTms*legRTTWeight)
	}
	if unprobed <= legScoreValue(0, int64(1*time.Millisecond)) {
		t.Errorf("unprobed session should not out-rank a measured 1ms session")
	}
}
