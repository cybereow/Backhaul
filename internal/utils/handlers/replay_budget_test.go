package handlers

import "testing"

func TestReplayBudgetGrowsOnDemandWithinBudget(t *testing.T) {
	// Room for two starting charges, the growth of the first to twice its limit
	// and half a starting charge more.
	b := NewReplayBudget(2*replayStartCharge + (2*ReplayFlowStart - replayStartCharge) + replayStartCharge/2)
	g1, g2 := b.Open(false), b.Open(false)
	if g1 == nil || g2 == nil {
		t.Fatal("two starting rings should fit")
	}
	// g1 doubles while room lasts: 256K -> 512K is charged in full.
	cur := g1.Limit()
	cur = g1.Grow(cur)
	if cur != 2*ReplayFlowStart {
		t.Fatalf("first growth gave %d", cur)
	}
	if next := g1.Grow(cur); next != cur {
		t.Fatalf("growth beyond the budget gave %d, want %d", next, cur)
	}
	// A flow opening later gets its starting ring only from what is left, and
	// force always succeeds.
	if g3 := b.Open(false); g3 != nil {
		t.Fatal("a third flow should not fit")
	}
	g4 := b.Open(true)
	if g4 == nil || g4.Limit() != ReplayFlowStart {
		t.Fatal("force must grant the starting ring")
	}
	g1.Release()
	g1.Release()
	g2.Release()
	g4.Release()
	if b.used != 0 {
		t.Fatalf("budget leaked %d", b.used)
	}
	var nilGrant *ReplayGrant
	nilGrant.Release() // no-op
}

func TestReplayGrantCapsAtFlowMax(t *testing.T) {
	g := NewReplayBudget(ReplayBudgetTotal).Open(false)
	cur := g.Limit()
	for i := 0; i < 20; i++ {
		cur = g.Grow(cur)
	}
	if cur != ReplayFlowMax {
		t.Fatalf("limit grew to %d, want the cap %d", cur, ReplayFlowMax)
	}
}

func TestReplayBudgetForceIsBounded(t *testing.T) {
	b := NewReplayBudget(4 * replayStartCharge)
	var grants []*ReplayGrant
	for i := 0; i < 20; i++ {
		if g := b.Open(true); g != nil {
			grants = append(grants, g)
		}
	}
	if len(grants) != 5 { // the budget plus a quarter
		t.Fatalf("force granted %d flows, want 5 (budget 4 + 25%%)", len(grants))
	}
	if b.Open(false) != nil {
		t.Fatal("a normal flow got a ring beyond the budget")
	}
}

func TestReplayBudgetFollowsTheHeapLimit(t *testing.T) {
	for _, c := range []struct {
		heap int64
		want int
	}{
		{0, ReplayBudgetTotal},       // no limit: as before
		{143 << 20, (143 << 20) / 4}, // a quarter
		{1 << 30, ReplayBudgetTotal}, // never more than the cap
		{8 << 20, replayBudgetMin},   // never less than a few rings
	} {
		if got := ReplayBudgetFor(c.heap); got != c.want {
			t.Errorf("ReplayBudgetFor(%d MiB) = %d, want %d", c.heap>>20, got, c.want)
		}
	}
	b := NewReplayBudget(ReplayBudgetTotal)
	b.SetTotal(replayBudgetMin)
	if b.Open(false) == nil {
		t.Fatal("a flow could not open a ring within a small budget")
	}
}

// Idle flows are charged next to nothing, so many of them do not keep a busy one
// from growing its ring; and a ring that has grown gives its charge back when it
// is taken down again.
func TestIdleFlowsLeaveRoomToGrow(t *testing.T) {
	b := NewReplayBudget(35 << 20)
	var idle []*ReplayGrant
	for i := 0; i < 140; i++ { // a few hundred kept-alive connections, a quarter-MiB each
		g := b.Open(false)
		if g == nil {
			t.Fatalf("flow %d was refused a ring: idle flows used up the budget", i)
		}
		idle = append(idle, g)
	}
	busy := b.Open(false)
	if busy == nil {
		t.Fatal("a busy flow was refused a ring")
	}
	cur := busy.Limit()
	for i := 0; i < 20; i++ {
		cur = busy.Grow(cur)
	}
	if cur != ReplayFlowMax {
		t.Fatalf("a busy flow could only grow to %d beside the idle ones, want %d", cur, ReplayFlowMax)
	}

	used := b.used
	if lim := busy.Shrink(); lim != ReplayFlowStart {
		t.Fatalf("Shrink returned %d, want the starting limit %d", lim, ReplayFlowStart)
	}
	if got := used - b.used; got != ReplayFlowMax-replayStartCharge {
		t.Fatalf("shrinking gave back %d, want %d", got, ReplayFlowMax-replayStartCharge)
	}
	busy.Shrink() // again: nothing more to give
	busy.Release()
	busy.Shrink() // after release: still nothing, and no negative charge
	for _, g := range idle {
		g.Release()
	}
	if b.used != 0 {
		t.Fatalf("budget leaked %d", b.used)
	}
}

func TestEmptiedRingShrinksOnlyWhenIdle(t *testing.T) {
	r := newReplayRing(4 * ReplayFlowStart)
	r.append(make([]byte, 1000))
	if r.shrinkIfIdle(ReplayFlowStart) {
		t.Fatal("a ring that holds bytes was shrunk")
	}
	r.ackTo(1000)
	if !r.shrinkIfIdle(ReplayFlowStart) || r.getLimit() != ReplayFlowStart {
		t.Fatalf("an emptied ring was not shrunk (limit %d)", r.getLimit())
	}
	if r.shrinkIfIdle(ReplayFlowStart) {
		t.Fatal("a ring at its starting size was shrunk again")
	}
}

// The gauges follow what the rings hold: bytes in on append, out on ACK and on
// discard.
func TestReplayGaugesFollowTheRings(t *testing.T) {
	base, _, _ := ReplayGauges()
	a, b := newReplayRing(1<<20), newReplayRing(1<<20)
	a.append(make([]byte, 5000))
	b.append(make([]byte, 7000))
	if held, _, _ := ReplayGauges(); held-base != 12000 {
		t.Fatalf("rings hold %d, want 12000", held-base)
	}
	a.ackTo(2000)
	a.ackTo(1) // stale: ignored
	if held, _, _ := ReplayGauges(); held-base != 10000 {
		t.Fatalf("after an ACK rings hold %d, want 10000", held-base)
	}
	a.discard()
	b.ackTo(7000)
	if held, _, _ := ReplayGauges(); held != base {
		t.Fatalf("empty rings hold %d more than before", held-base)
	}
}
