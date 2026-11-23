package proclimits

import (
	"testing"
	"time"
)

func TestMonitorSeesAProcessThatDidNotRun(t *testing.T) {
	var m Monitor
	t0 := time.Unix(1000, 0)
	m.Observe(t0)
	if m.Starved(t0) {
		t.Fatal("starved before anything was seen")
	}
	// On time, then a hiccup too small to count.
	m.Observe(t0.Add(starveTick))
	m.Observe(t0.Add(2*starveTick + 10*time.Millisecond))
	if m.Starved(t0.Add(time.Second)) {
		t.Fatal("a 10 ms delay was taken for starvation")
	}
	// A quota stop: the next look comes 90 ms late.
	at := t0.Add(2*starveTick + 10*time.Millisecond).Add(starveTick + 90*time.Millisecond)
	m.Observe(at)
	if !m.Starved(at) || !m.Starved(at.Add(StarveHold-time.Millisecond)) {
		t.Fatal("a 90 ms stop was not seen")
	}
	if m.Starved(at.Add(StarveHold)) {
		t.Fatal("starvation outlived its hold")
	}
	if d := m.Delay(); d < 80*time.Millisecond || d > 100*time.Millisecond {
		t.Fatalf("worst delay = %v, want about 90 ms", d)
	}
	if d := m.Delay(); d != 0 {
		t.Fatalf("delay was not reset: %v", d)
	}
}
