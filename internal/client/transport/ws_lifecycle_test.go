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
