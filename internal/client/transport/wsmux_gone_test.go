package transport

import (
	"testing"
	"time"
)

// A flow never seen may be opened again, exactly once; one that ran and finished
// may not, and neither may the held-up opening that arrives after the new one.
func TestGoneFlowsAdmitOneReopenAndNoRerun(t *testing.T) {
	now := time.Now()
	var g goneFlows

	if !g.admit(1, now) {
		t.Fatal("a flow nobody has heard of was refused")
	}
	g.finished(1, now)
	if g.neverSeen(1, now.Add(time.Second)) {
		t.Fatal("a flow that ran and finished here was reported as never seen")
	}
	if g.admit(1, now.Add(time.Second)) {
		t.Fatal("a flow that finished here was admitted again: its request would reach the target twice")
	}

	if !g.neverSeen(2, now) {
		t.Fatal("an unknown flow was not reported as never seen")
	}
	if !g.neverSeen(2, now) {
		t.Fatal("a second resume attempt for the same unseen flow got a different answer")
	}
	if !g.admit(2, now) {
		t.Fatal("the opening the server was asked for was refused")
	}
	if g.admit(2, now.Add(time.Minute)) {
		t.Fatal("the held-up original opening was admitted after the new one")
	}
	g.finished(2, now.Add(2*time.Minute))
	if g.admit(2, now.Add(reopenedFor-time.Second)) {
		t.Fatal("finishing shortened how long a reopened flow is remembered")
	}

	if !g.admit(1, now.Add(goneFor+2*time.Second)) || !g.admit(2, now.Add(reopenedFor+time.Second)) {
		t.Fatal("a flow is still remembered after its time")
	}
}
