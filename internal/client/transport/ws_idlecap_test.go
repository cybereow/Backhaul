package transport

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestIdleCeilingDefaultsScaleWithThePool(t *testing.T) {
	cases := []struct{ pool, configured, want int }{
		{8, 0, 32},   // 4x
		{16, 0, 64},  // 4x
		{2, 0, 18},   // small pool: at least +16
		{64, 0, 256}, // scales up
		{8, 40, 40},  // explicit
		{8, 3, 8},    // never below the pool's own floor
		{0, 0, 16},   // nothing configured
		{16, -1, 64}, // negative = unset
	}
	for _, c := range cases {
		if got := idleCeiling(c.pool, c.configured); got != c.want {
			t.Errorf("idleCeiling(pool=%d, configured=%d) = %d, want %d", c.pool, c.configured, got, c.want)
		}
	}
}

func TestReserveDialStopsAtTheCeiling(t *testing.T) {
	c := &WsTransport{config: &WsConfig{ConnPoolSize: 4, ConnPoolMax: 10}}
	atomic.StoreInt32(&c.poolConnections, 6) // idle
	granted := 0
	for i := 0; i < 20; i++ {
		if c.reserveDial() {
			granted++
		}
	}
	if granted != 4 { // 6 idle + 4 in flight = 10
		t.Fatalf("granted %d dials with 6 idle and a ceiling of 10, want 4", granted)
	}
	if got := atomic.LoadInt32(&c.pendingDials); got != 4 {
		t.Fatalf("pendingDials = %d after refused attempts, want 4 (refusals must give the count back)", got)
	}
	// A connection that starts serving a flow leaves the idle count: room again.
	atomic.AddInt32(&c.poolConnections, -1)
	if !c.reserveDial() {
		t.Fatal("a dial was refused although a connection had left the pool")
	}
}

func TestReserveDialIsSafeUnderConcurrency(t *testing.T) {
	c := &WsTransport{config: &WsConfig{ConnPoolSize: 4, ConnPoolMax: 20}}
	var granted int32
	done := make(chan struct{})
	for i := 0; i < 200; i++ {
		go func() {
			if c.reserveDial() {
				atomic.AddInt32(&granted, 1)
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < 200; i++ {
		<-done
	}
	if granted != 20 {
		t.Fatalf("granted %d concurrent dials under a ceiling of 20", granted)
	}
}

// A request that finds the ceiling reached waits for room instead of being
// dropped: it stands for a flow the server is holding.
func TestAwaitDialSlotWaitsForRoom(t *testing.T) {
	c := &WsTransport{config: &WsConfig{ConnPoolSize: 2, ConnPoolMax: 4}}
	atomic.StoreInt32(&c.poolConnections, 4) // full
	go func() {
		time.Sleep(100 * time.Millisecond)
		atomic.AddInt32(&c.poolConnections, -1) // a connection went to a flow
	}()
	start := time.Now()
	if !c.awaitDialSlot(context.Background(), 2*time.Second) {
		t.Fatal("gave up although room appeared")
	}
	if d := time.Since(start); d < 80*time.Millisecond || d > time.Second {
		t.Fatalf("waited %v, want about 100ms", d)
	}
}

// When nothing ever frees up (connections piling up unused) it gives up, which
// is what bounds the pile.
func TestAwaitDialSlotGivesUpWhenNothingFreesUp(t *testing.T) {
	c := &WsTransport{config: &WsConfig{ConnPoolSize: 2, ConnPoolMax: 4}}
	atomic.StoreInt32(&c.poolConnections, 4)
	start := time.Now()
	if c.awaitDialSlot(context.Background(), 150*time.Millisecond) {
		t.Fatal("got a slot with the pool full and nothing freeing up")
	}
	if d := time.Since(start); d < 140*time.Millisecond {
		t.Fatalf("gave up after %v, before the wait ended", d)
	}
	if got := atomic.LoadInt32(&c.pendingDials); got != 0 {
		t.Fatalf("pendingDials = %d after giving up, want 0", got)
	}
}

func TestAwaitDialSlotStopsWithTheContext(t *testing.T) {
	c := &WsTransport{config: &WsConfig{ConnPoolSize: 2, ConnPoolMax: 4}}
	atomic.StoreInt32(&c.poolConnections, 4)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	start := time.Now()
	if c.awaitDialSlot(ctx, 10*time.Second) {
		t.Fatal("got a slot after the context ended")
	}
	if time.Since(start) > time.Second {
		t.Fatal("did not stop with the context")
	}
}
