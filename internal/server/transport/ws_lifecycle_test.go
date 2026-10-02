package transport

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// A control channel that is not the registered one (a stale handler's late
// error, or the client already reattached) must not clear the live one or start
// a reattach wait.
func TestWsOnControlLostIgnoresStaleConn(t *testing.T) {
	s, cancel := setupWsTransport(t)
	defer cancel()
	g := newWsGeneration(context.Background())
	defer g.stop()
	s.gen = g

	live, liveServer := makeWsConnPair(t)
	_, stale := makeWsConnPair(t)
	_ = live
	s.controlChannel = liveServer
	epoch := s.lossEpoch

	s.onControlLost(g, stale)

	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	if s.controlChannel != liveServer {
		t.Fatal("a stale connection's loss cleared the live control channel")
	}
	if s.lossEpoch != epoch {
		t.Fatal("a stale connection's loss started a new loss epoch")
	}
}

// A real loss clears the channel and keeps the transport (no restart) so the
// flows on it survive; an adoption ends the wait.
func TestWsAwaitReattachEndsOnAdoption(t *testing.T) {
	s, cancel := setupWsTransport(t)
	defer cancel()
	g := newWsGeneration(context.Background())
	defer g.stop()
	s.gen = g

	_, server := makeWsConnPair(t)
	s.controlChannel = server
	s.onControlLost(g, server)

	s.controlMu.Lock()
	if s.controlChannel != nil {
		s.controlMu.Unlock()
		t.Fatal("control channel not cleared on loss")
	}
	s.lossEpoch++ // what an adoption does
	s.controlMu.Unlock()

	done := make(chan struct{})
	go func() { g.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the reattach wait did not end after an adoption")
	}
}

// An idle connection is closed once its replacement has joined the pool.
func TestWsRetireIdleTunnelClosesAfterReplacement(t *testing.T) {
	s, cancel := setupWsTransport(t)
	defer cancel()
	s.reqNewConnChan = make(chan struct{}, 4)

	_, srv := makeWsConnPair(t)
	tc := makeTunnelChannel(srv)

	done := make(chan bool, 1)
	go func() { done <- s.retireIdleTunnel(s.ctx, s.tunnelChannel, s.reqNewConnChan, &tc) }()

	select {
	case <-s.reqNewConnChan:
	case <-time.After(2 * time.Second):
		t.Fatal("rotation did not ask for a replacement")
	}
	// The replacement joins the pool.
	_, repl := makeWsConnPair(t)
	s.tunnelChannel <- makeTunnelChannel(repl)

	select {
	case closed := <-done:
		if !closed {
			t.Fatal("idle connection was not retired after its replacement arrived")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("retireIdleTunnel did not finish")
	}
}

// A connection a flow took meanwhile is never closed by rotation.
func TestWsRetireIdleTunnelLeavesHandedOverConn(t *testing.T) {
	s, cancel := setupWsTransport(t)
	defer cancel()
	s.reqNewConnChan = make(chan struct{}, 4)

	_, srv := makeWsConnPair(t)
	tc := makeTunnelChannel(srv)

	done := make(chan bool, 1)
	go func() { done <- s.retireIdleTunnel(s.ctx, s.tunnelChannel, s.reqNewConnChan, &tc) }()
	<-s.reqNewConnChan

	// What handleLoop does on hand-over.
	close(tc.ping)
	tc.mu.Lock()

	select {
	case closed := <-done:
		if closed {
			t.Fatal("rotation closed a connection that was handed to a flow")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("retireIdleTunnel did not return after hand-over")
	}
	if atomic.LoadInt32(&s.activeFlows) != 0 {
		t.Fatal("unexpected flow count")
	}
}
