package handlers

import "testing"

func TestReplayBudget(t *testing.T) {
	b := NewReplayBudget(ReplayFlowLimit + minReplayLimit + minReplayLimit/2)
	l1, r1 := b.Reserve()
	l2, r2 := b.Reserve()
	l3, r3 := b.Reserve()
	if l1 != ReplayFlowLimit || l2 != minReplayLimit || l3 != 0 {
		t.Fatalf("grants %d %d %d", l1, l2, l3)
	}
	r3()
	if lf, rf := b.Force(); lf != minReplayLimit {
		t.Fatalf("Force granted %d", lf)
	} else {
		rf()
		rf() // idempotent
	}
	r1()
	r1()
	if l, r := b.Reserve(); l != ReplayFlowLimit {
		t.Fatalf("after release: %d", l)
	} else {
		r()
	}
	r2()
	if b.used != 0 {
		t.Fatalf("budget leaked %d", b.used)
	}
}
