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
