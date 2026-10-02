package transport

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/musix/backhaul/config"
)

func lifecycleWSConfig(addr string, pool int) *WsConfig {
	return &WsConfig{
		RemoteAddr:    addr,
		Token:         lifecycleToken,
		Mode:          config.WS,
		Path:          "/",
		ConnPoolSize:  pool,
		DialTimeOut:   2 * time.Second,
		KeepAlive:     30 * time.Second,
		RetryInterval: 100 * time.Millisecond,
	}
}

// TestWSLifecycleIdleCancel: an idle plain-WS pool socket sits in ReadMessage
// waiting for the server to hand it a destination. Cancelling the transport must
// close it (ctx cannot wake a blocked read) and settle the pool counter, which
// the idle loop used to leave incremented.
func TestWSLifecycleIdleCancel(t *testing.T) {
	srv := newLifecycleServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := NewWSClient(ctx, lifecycleWSConfig(srv.addr(), 2), lifecycleLogger())
	c.Start()

	conns := srv.waitTunnels(t, 2)
	waitFor(t, "both pool connections to be counted", func() bool { return atomic.LoadInt32(&c.poolConnections) == 2 })

	cancel()

	for _, conn := range conns {
		expectPeerClosed(t, "idle plain-WS pool socket", conn)
	}
	waitFor(t, "pool counter to settle", func() bool { return atomic.LoadInt32(&c.poolConnections) == 0 })
}

// TestWSLifecycleRestart: a restart closes the idle pool sockets of the old
// generation, returns only after its workers ended, and the new generation
// rebuilds the pool with fresh counters.
func TestWSLifecycleRestart(t *testing.T) {
	srv := newLifecycleServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := NewWSClient(ctx, lifecycleWSConfig(srv.addr(), 2), lifecycleLogger())
	c.Start()

	old := srv.waitTunnels(t, 2)
	waitFor(t, "both pool connections to be counted", func() bool { return atomic.LoadInt32(&c.poolConnections) == 2 })

	oldGen := c.gen
	c.Restart()

	if !oldGen.join(time.Second) {
		t.Fatal("Restart returned while old-generation workers were still running")
	}
	for _, conn := range old {
		expectPeerClosed(t, "old-generation pool socket", conn)
	}
	srv.waitTunnels(t, 4) // the new generation dials its own pool
	waitFor(t, "the new pool to be counted once", func() bool { return atomic.LoadInt32(&c.poolConnections) == 2 })
}

// Only owed connections are refilled, never past the floor, and a debt with no
// deficit is forgiven so the pool does not grow by one per consumed connection.
func TestWsRefillOwed(t *testing.T) {
	c := &WsTransport{config: &WsConfig{ConnPoolSize: 4}}

	if n := c.refillOwed(); n != 0 {
		t.Fatalf("refilled %d with nothing owed", n)
	}

	// A flow consumed a connection: deficit, but nothing owed - the server's own
	// request replaces it.
	atomic.StoreInt32(&c.poolConnections, 3)
	if n := c.refillOwed(); n != 0 {
		t.Fatalf("refilled %d for a consumed (not died) connection", n)
	}

	// An idle connection died: owed and below the floor.
	atomic.StoreInt32(&c.poolConnections, 3)
	c.owe()
	if n := c.refillOwed(); n != 1 {
		t.Fatalf("refilled %d, want 1", n)
	}
	if n := c.refillOwed(); n != 0 {
		t.Fatalf("debt refilled twice: %d", n)
	}

	// In-flight dials count against the deficit.
	atomic.StoreInt32(&c.poolConnections, 3)
	atomic.StoreInt32(&c.pendingDials, 1)
	c.owe()
	if n := c.refillOwed(); n != 0 {
		t.Fatalf("re-dialled while one was still in flight: %d", n)
	}

	// At the floor the debt is forgiven (a rotation replacement arrived first).
	atomic.StoreInt32(&c.pendingDials, 0)
	atomic.StoreInt32(&c.poolConnections, 4)
	c.owe()
	if n := c.refillOwed(); n != 0 || atomic.LoadInt32(&c.owedConns) != 0 {
		t.Fatalf("debt at the floor: refilled %d, owed %d", n, atomic.LoadInt32(&c.owedConns))
	}

	// A long outage cannot build a debt bigger than the floor.
	for i := 0; i < 50; i++ {
		c.owe()
	}
	if got := atomic.LoadInt32(&c.owedConns); got != 4 {
		t.Fatalf("owed %d, want it capped at the floor 4", got)
	}
}
