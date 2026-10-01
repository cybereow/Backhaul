package transport

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Four pooled tunnels asking for discovery at once cause exactly one sweep, and a
// refresh request right after it reuses that result.
func TestDiscoveryShareRunsOnce(t *testing.T) {
	var d discoveryShare
	var runs atomic.Int32
	run := func(refresh bool) ([]string, error) {
		runs.Add(1)
		time.Sleep(50 * time.Millisecond)
		return []string{"1.1.1.1:53"}, nil
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got, err := d.get(false, run); err != nil || len(got) != 1 {
				t.Errorf("get: %v %v", got, err)
			}
		}()
	}
	wg.Wait()
	for i := 0; i < 4; i++ {
		if _, err := d.get(true, run); err != nil {
			t.Fatal(err)
		}
	}
	if n := runs.Load(); n != 1 {
		t.Fatalf("discovery ran %d times for one pool, want 1", n)
	}
	d.at = time.Now().Add(-2 * refreshMinAge) // the last sweep is old now: a refresh really refreshes
	if _, err := d.get(true, run); err != nil || runs.Load() != 2 {
		t.Fatalf("old result not refreshed: runs=%d err=%v", runs.Load(), err)
	}
}

// A refresh requested while the last sweep is still fresh is not lost: it stays
// pending and runs as soon as the result is old enough, even if the retry that
// follows does not ask for a refresh itself.
func TestDiscoveryRefreshStaysPending(t *testing.T) {
	var d discoveryShare
	var runs atomic.Int32
	run := func(bool) ([]string, error) { runs.Add(1); return []string{"1.1.1.1:53"}, nil }
	if _, err := d.get(false, run); err != nil {
		t.Fatal(err)
	}
	if _, err := d.get(true, run); err != nil || runs.Load() != 1 { // too early: served from the old result
		t.Fatalf("early refresh ran a sweep: runs=%d err=%v", runs.Load(), err)
	}
	d.at = time.Now().Add(-2 * refreshMinAge)
	if _, err := d.get(false, run); err != nil || runs.Load() != 2 {
		t.Fatalf("the pending refresh was forgotten: runs=%d err=%v", runs.Load(), err)
	}
	if _, err := d.get(false, run); err != nil || runs.Load() != 2 {
		t.Fatalf("a completed refresh must clear the request: runs=%d", runs.Load())
	}
}
