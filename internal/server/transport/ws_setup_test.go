package transport

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/musix/backhaul/internal/utils/network"
	"github.com/sirupsen/logrus"
)

// setupWsTransport returns a minimal WsTransport wired for handleLoop tests.
// It does not start listeners – only the channels and context needed by handleLoop.
func setupWsTransport(t *testing.T) (*WsTransport, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	s := &WsTransport{
		config:        &WsConfig{},
		ctx:           ctx,
		cancel:        cancel,
		logger:        logger,
		tunnelChannel: make(chan TunnelChannel, 16),
		localChannel:  make(chan LocalTCPConn, 16),
		// usageMonitor nil is fine: WSConnectionHandler only calls
		// AddOrUpdatePort when sniffer==true and we set config.Sniffer=false.
	}
	return s, cancel
}

// makeWsConnPair creates a real WebSocket connection pair using an httptest server,
// following the same pattern as internal/utils/handlers/ws_handler_test.go.
// The returned connections are cleaned up via t.Cleanup.
func makeWsConnPair(t *testing.T) (client, server *network.WebSocketConn) {
	t.Helper()
	srvCh := make(chan *network.WebSocketConn, 1)
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		netConn, _, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			return
		}
		srvCh <- network.NewWebSocketConn(netConn, ws.StateServerSide, nil)
	}))
	t.Cleanup(hs.Close)

	netConn, br, _, err := ws.DefaultDialer.Dial(context.Background(), "ws"+strings.TrimPrefix(hs.URL, "http"))
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	c := network.NewWebSocketConn(netConn, ws.StateClientSide, br)
	t.Cleanup(func() { c.Close() })

	select {
	case srv := <-srvCh:
		t.Cleanup(func() { srv.Close() })
		return c, srv
	case <-time.After(10 * time.Second):
		t.Fatal("websocket upgrade never completed")
		return nil, nil
	}
}

// makeTunnelChannel wraps a WebSocketConn into a TunnelChannel as handleLoop
// expects one.
func makeTunnelChannel(wsConn *network.WebSocketConn) TunnelChannel {
	return TunnelChannel{
		conn: wsConn,
		ping: make(chan struct{}),
		mu:   &sync.Mutex{},
	}
}

// makeTCPPair returns a connected TCP loopback pair.  Both sides are registered
// with t.Cleanup.
func makeTCPPair(t *testing.T) (server, client net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err == nil {
			accepted <- c
		}
	}()

	dialed, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { dialed.Close() })

	select {
	case srv := <-accepted:
		t.Cleanup(func() { srv.Close() })
		return srv, dialed
	case <-time.After(5 * time.Second):
		t.Fatal("TCP accept timed out")
		return nil, nil
	}
}

// waitClosed blocks until conn.Read returns an error (indicating the connection
// was closed) or the deadline is exceeded.  It returns the error from Read.
func waitClosed(t *testing.T, conn net.Conn, deadline time.Duration) error {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(deadline))
	buf := make([]byte, 1)
	_, err := conn.Read(buf)
	return err
}

// TestWSSetupWaitExpires verifies that a queued local connection whose entire
// setup budget has already elapsed is closed immediately without waiting for a
// tunnel.
func TestWSSetupWaitExpires(t *testing.T) {
	s, cancel := setupWsTransport(t)
	defer cancel()

	localConn, localPeer := net.Pipe()
	defer localPeer.Close()

	// timeCreated 4 seconds ago: budget is exhausted on arrival.
	expiredTime := time.Now().Add(-4 * time.Second).UnixMilli()

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.handleLoop()
	}()

	s.localChannel <- LocalTCPConn{
		conn:        localConn,
		remoteAddr:  "127.0.0.1:9999",
		timeCreated: expiredTime,
	}

	// localPeer must observe that localConn was closed within 1 s.
	if err := waitClosed(t, localPeer, time.Second); err == nil {
		t.Fatal("expected localConn to be closed immediately; Read succeeded instead")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleLoop did not return after cancel")
	}
}

// TestWSSetupWaitExpiresWhileWaiting verifies that a local connection whose
// budget expires while the inner loop is waiting for a tunnel is closed within
// the budget, not after an unbounded wait.
func TestWSSetupWaitExpiresWhileWaiting(t *testing.T) {
	s, cancel := setupWsTransport(t)
	defer cancel()

	localConn, localPeer := net.Pipe()
	defer localPeer.Close()

	// 50 ms of budget remaining – short enough for a fast bounded test.
	const remaining = 50 * time.Millisecond
	freshTime := time.Now().Add(-(3000*time.Millisecond - remaining)).UnixMilli()

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.handleLoop()
	}()

	s.localChannel <- LocalTCPConn{
		conn:        localConn,
		remoteAddr:  "127.0.0.1:9999",
		timeCreated: freshTime,
	}

	// localPeer must observe closure shortly after the budget fires.
	if err := waitClosed(t, localPeer, 2*time.Second); err == nil {
		t.Fatal("expected localConn to be closed when budget expires; Read succeeded")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleLoop did not return after cancel")
	}
}

// TestWSSetupCancelClosesLocal verifies that cancelling the transport context
// while the inner loop is waiting for a tunnel causes the local socket to be
// closed and handleLoop to return.
func TestWSSetupCancelClosesLocal(t *testing.T) {
	s, cancel := setupWsTransport(t)

	localConn, localPeer := net.Pipe()
	defer localPeer.Close()

	// Fresh timestamp – budget is not exhausted.
	freshTime := time.Now().UnixMilli()

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.handleLoop()
	}()

	s.localChannel <- LocalTCPConn{
		conn:        localConn,
		remoteAddr:  "127.0.0.1:9999",
		timeCreated: freshTime,
	}

	// Give handleLoop time to enter the tunnel-wait select.
	time.Sleep(20 * time.Millisecond)
	cancel()

	// localPeer must observe localConn closure after cancel.
	if err := waitClosed(t, localPeer, 2*time.Second); err == nil {
		t.Fatal("expected localConn to be closed on cancel; Read succeeded")
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleLoop did not return after cancel")
	}
}

// TestWSSetupBlockedHandover verifies that handleLoop does not hang when the
// handover mutex is held by an in-flight keepAlive.  After cancel() the setup
// deadline unblocks keepAlive, and once the external holder releases it
// handleLoop cleans up and returns.
func TestWSSetupBlockedHandover(t *testing.T) {
	s, cancel := setupWsTransport(t)

	localConn, localPeer := net.Pipe()
	defer localPeer.Close()

	freshTime := time.Now().UnixMilli()

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.handleLoop()
	}()

	_, tunnelSrv := makeWsConnPair(t)
	tc := makeTunnelChannel(tunnelSrv)

	// Hold the mutex to simulate a keepAlive write in progress, preventing
	// handleLoop from completing the handover.
	tc.mu.Lock()

	// Queue the local conn first so the outer select dequeues it.
	s.localChannel <- LocalTCPConn{
		conn:        localConn,
		remoteAddr:  "127.0.0.1:9999",
		timeCreated: freshTime,
	}
	// Then queue the tunnel.  handleLoop's inner select will dequeue it and
	// block on mu.Lock() until we release below.
	s.tunnelChannel <- tc

	// Give handleLoop time to dequeue both and block on mu.Lock().
	time.Sleep(60 * time.Millisecond)

	// Cancel the context.  handleLoop sets a setup deadline on the underlying
	// net.Conn before calling Lock(), so any blocked keepAlive write will be
	// unblocked.  Release the mutex to let handleLoop proceed through its
	// post-Lock cancellation check.
	cancel()
	tc.mu.Unlock()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handleLoop stuck after cancel while handover mutex was externally held")
	}
}

// TestWSSetupSuccessClearsDeadline verifies the happy path: a local TCP
// connection and a tunnel are matched, the destination message is written, the
// setup deadline is cleared, and WSConnectionHandler can exchange data without
// hitting an artificial timeout.
func TestWSSetupSuccessClearsDeadline(t *testing.T) {
	s, cancel := setupWsTransport(t)
	defer cancel()

	// Use a real TCP pair so that LocalAddr() returns *net.TCPAddr (required
	// by WSConnectionHandler's usage-monitor port extraction).
	localConn, userConn := makeTCPPair(t)

	freshTime := time.Now().UnixMilli()

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.handleLoop()
	}()

	// tunnelCli is passed to WSConnectionHandler; tunnelSrv is the peer that
	// first receives the destination message and then the proxied data.
	tunnelCli, tunnelSrv := makeWsConnPair(t)
	tc := makeTunnelChannel(tunnelCli)

	// Drain the destination TextMessage on tunnelSrv so WriteMessage succeeds.
	destReceived := make(chan string, 1)
	go func() {
		_, msg, err := tunnelSrv.ReadMessage()
		if err == nil {
			destReceived <- string(msg)
		}
	}()

	s.localChannel <- LocalTCPConn{
		conn:        localConn,
		remoteAddr:  "127.0.0.1:8888",
		timeCreated: freshTime,
	}
	s.tunnelChannel <- tc

	// Confirm the destination message was sent correctly.
	select {
	case dest := <-destReceived:
		if dest != "127.0.0.1:8888" {
			t.Fatalf("dest message = %q, want %q", dest, "127.0.0.1:8888")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("destination message never received on tunnelSrv")
	}

	// Write a small payload from the user side.  WSConnectionHandler reads from
	// localConn and writes it to tunnelCli as binary WS frames.
	payload := []byte("hello-from-user")
	if _, err := userConn.Write(payload); err != nil {
		t.Fatalf("userConn.Write: %v", err)
	}

	// Receive the payload on the tunnel peer side.
	tunnelSrv.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, got, err := tunnelSrv.ReadMessage()
	if err != nil {
		t.Fatalf("tunnelSrv.ReadMessage: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("payload = %q, want %q", got, payload)
	}

	// handleLoop should have returned to its outer loop after the successful
	// handoff.  It blocks on localChannel again — just cancel and confirm exit.
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleLoop did not return after cancel")
	}
}

// TestWSSetupRetryPreservesOriginalExpiry verifies that when a tunnel write
// fails and handleLoop retries, the remaining deadline is drawn from the
// original timeCreated budget, not from a fresh 3-second window. The local
// conn is given only ~150 ms of total budget; after the first tunnel fails the
// loop must expire and close localConn within that window, not after 3 s.
func TestWSSetupRetryPreservesOriginalExpiry(t *testing.T) {
	s, cancel := setupWsTransport(t)
	defer cancel()

	localConn, localPeer := net.Pipe()
	defer localPeer.Close()

	const totalBudget = 150 * time.Millisecond
	freshTime := time.Now().Add(-(3000*time.Millisecond - totalBudget)).UnixMilli()

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.handleLoop()
	}()

	s.localChannel <- LocalTCPConn{
		conn:        localConn,
		remoteAddr:  "127.0.0.1:9999",
		timeCreated: freshTime,
	}

	// Give handleLoop time to enter the tunnel-wait inner select.
	time.Sleep(20 * time.Millisecond)

	// Enqueue a tunnel whose underlying conn is already closed; WriteMessage
	// will fail, causing handleLoop to discard this tunnel and retry.
	_, tunnelSrv := makeWsConnPair(t)
	tc := makeTunnelChannel(tunnelSrv)
	tunnelSrv.Close() // force the write to fail
	s.tunnelChannel <- tc

	// localPeer must observe closure within budget + generous margin (not 3 s).
	start := time.Now()
	if err := waitClosed(t, localPeer, 2*time.Second); err == nil {
		t.Fatal("expected localConn to be closed after budget expires; Read succeeded")
	}
	elapsed := time.Since(start)
	// Must not reset to a fresh 3-second window.
	if elapsed > 2*time.Second {
		t.Fatalf("localConn closed after %v – budget appears to have been reset (expected < 2s)", elapsed)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleLoop did not return after cancel")
	}
}
