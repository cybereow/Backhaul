package handlers

import "testing"

func TestReplayBudgetGrowsOnDemandWithinBudget(t *testing.T) {
	b := NewReplayBudget(3*ReplayFlowStart + ReplayFlowStart/2)
	g1, g2 := b.Open(false), b.Open(false)
	if g1 == nil || g2 == nil {
		t.Fatal("two starting rings should fit")
	}
	// g1 doubles while room lasts: 256K -> 512K costs another 256K, then 1M costs 512K.
	cur := g1.Limit()
	cur = g1.Grow(cur)
	if cur != 2*ReplayFlowStart {
		t.Fatalf("first growth gave %d", cur)
	}
	if next := g1.Grow(cur); next != cur {
		t.Fatalf("growth beyond the budget gave %d, want %d", next, cur)
	}
	// An idle flow opening later still gets its starting ring from what is left,
	// and force always succeeds.
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
	b := NewReplayBudget(4 * ReplayFlowStart)
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
