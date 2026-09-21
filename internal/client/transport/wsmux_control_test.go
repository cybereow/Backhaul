package transport

import (
	"context"
	"errors"
	"net"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/musix/backhaul/config"
	"github.com/musix/backhaul/internal/utils"
	"github.com/musix/backhaul/internal/utils/network"
	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
)

const controlTestDeadline = 5 * time.Second

// testConn wraps the client end of a pipe. With a nil gate writes pass through;
// with a gate, Write blocks until the gate is closed and then fails, which lets a
// test hold the handler inside a heartbeat write and then make that write fail.
type testConn struct {
	net.Conn
	gate chan struct{}
}

func (t *testConn) Write(p []byte) (int, error) {
	if t.gate == nil {
		return t.Conn.Write(p)
	}
	<-t.gate
	return 0, errors.New("injected write failure")
}

func newControlTransport(t *testing.T) (*WsMuxTransport, *logtest.Hook) {
	t.Helper()
	// The transport ctx is deliberately NOT cancelled by the tests below; it is
	// only cancelled on cleanup, so a handler that exits does so on its own.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	logger, hook := logtest.NewNullLogger()
	logger.SetLevel(logrus.WarnLevel)
	return &WsMuxTransport{
		ctx:    ctx,
		cancel: cancel,
		logger: logger,
		config: &WsMuxConfig{
			RetryInterval: 10 * time.Millisecond,
			DialTimeOut:   50 * time.Millisecond,
			RemoteAddr:    "127.0.0.1:1", // loopback, refused immediately
			Mode:          config.WSMUX,
		},
		controlFlow:  make(chan struct{}, 100),
		stripeGroups: make(map[uint32]*stripeGroup),
		endpoints:    []wsEndpoint{{addr: "127.0.0.1:1"}},
	}, hook
}

// newControlConn returns the client-side control connection, registered as the
// transport's current control channel (so reconnectControl's identity guard
// passes), and the server end of the pipe.
func newControlConn(t *testing.T, c *WsMuxTransport, gate chan struct{}) (*network.WebSocketConn, net.Conn) {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() { server.Close(); client.Close() })
	conn := network.NewWebSocketConn(&testConn{Conn: client, gate: gate}, ws.StateClientSide, nil)
	c.controlMu.Lock()
	c.controlChannel = conn
	c.controlMu.Unlock()
	return conn, server
}

func runHandler(c *WsMuxTransport, conn *network.WebSocketConn) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.channelHandler(conn)
	}()
	return done
}

func sendHB(server net.Conn) error {
	return wsutil.WriteMessage(server, ws.StateServerSide, ws.OpBinary, []byte{utils.SG_HB})
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	end := time.Now().Add(controlTestDeadline)
	for !cond() {
		if time.Now().After(end) {
			t.Fatalf("timed out waiting for: %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func waitClosed(t *testing.T, what string, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(controlTestDeadline):
		t.Fatalf("timed out waiting for: %s", what)
	}
}

// readerGoroutines counts live control-channel reader goroutines (the func
// literal started by channelHandler), so a leaked reader fails a bounded
// assertion instead of going unnoticed.
func readerGoroutines() int {
	buf := make([]byte, 1<<16)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return strings.Count(string(buf[:n]), "channelHandler.func1")
		}
		buf = make([]byte, 2*len(buf))
	}
}

func countLogs(h *logtest.Hook, substr string) int {
	n := 0
	for _, e := range h.AllEntries() {
		if strings.Contains(e.Message, substr) {
			n++
		}
	}
	return n
}

// expectOneReconnect waits for the reconnect to start and then checks that no
// second one follows: one connection loss must trigger exactly one.
func expectOneReconnect(t *testing.T, h *logtest.Hook) {
	t.Helper()
	waitFor(t, "reconnectControl to start", func() bool { return countLogs(h, "control channel dropped") >= 1 })
	time.Sleep(100 * time.Millisecond)
	if n := countLogs(h, "control channel dropped"); n != 1 {
		t.Fatalf("reconnectControl passed its identity guard %d times, want exactly 1", n)
	}
}

// TestControlHandlerLifetime pins the lifetime of one control connection: the
// handler and its reader goroutine end together, whichever side dies first, and
// a lost connection is reconnected exactly once.
func TestControlHandlerLifetime(t *testing.T) {
	t.Run("server closes socket: handler and reader exit, one reconnect", func(t *testing.T) {
		c, hook := newControlTransport(t)
		conn, server := newControlConn(t, c, nil)
		done := runHandler(c, conn)

		server.Close()

		waitClosed(t, "handler to return", done)
		waitFor(t, "reader goroutine to exit", func() bool { return readerGoroutines() == 0 })
		expectOneReconnect(t, hook)
	})

	t.Run("heartbeat write failure: reader does not leak", func(t *testing.T) {
		c, hook := newControlTransport(t)
		gate := make(chan struct{})
		close(gate) // every write fails at once
		conn, server := newControlConn(t, c, gate)
		done := runHandler(c, conn)

		go func() { _ = sendHB(server) }()

		waitClosed(t, "handler to return", done)
		waitFor(t, "reader goroutine to exit", func() bool { return readerGoroutines() == 0 })
		// The reader saw the handler leave, so only the handler reconnected.
		expectOneReconnect(t, hook)
	})

	t.Run("heartbeat write failure with a restart under way: reader does not leak", func(t *testing.T) {
		// reconnectControl's identity guard rejects the drop (another goroutine
		// owns it), so it does not close the connection. The handler must still
		// take its reader down.
		c, hook := newControlTransport(t)
		gate := make(chan struct{})
		close(gate)
		conn, server := newControlConn(t, c, gate)
		c.controlMu.Lock()
		c.controlChannel = nil
		c.controlMu.Unlock()
		done := runHandler(c, conn)

		go func() { _ = sendHB(server) }()

		waitClosed(t, "handler to return", done)
		waitFor(t, "reader goroutine to exit", func() bool { return readerGoroutines() == 0 })
		time.Sleep(100 * time.Millisecond)
		if n := countLogs(hook, "control channel dropped"); n != 0 {
			t.Fatalf("reconnect ran %d times for a drop that another goroutine owned", n)
		}
	})

	t.Run("full msgChan does not wedge the reader after the handler exits", func(t *testing.T) {
		c, hook := newControlTransport(t)
		gate := make(chan struct{})
		t.Cleanup(func() {
			select {
			case <-gate:
			default:
				close(gate)
			}
		})
		conn, server := newControlConn(t, c, gate)
		done := runHandler(c, conn)

		// The handler takes the first heartbeat and blocks answering it (gate).
		// The rest fill msgChan (cap 1000) and the reader blocks on the send.
		var sent atomic.Int32
		go func() {
			for i := 0; i < 1100; i++ {
				if sendHB(server) != nil {
					return
				}
				sent.Add(1)
			}
		}()
		waitFor(t, "msgChan to fill", func() bool { return sent.Load() >= 1002 })
		// Let the count settle so the reader is parked in the send.
		for prev := int32(-1); prev != sent.Load(); {
			prev = sent.Load()
			time.Sleep(50 * time.Millisecond)
		}

		close(gate) // the blocked heartbeat write now fails

		waitClosed(t, "handler to return", done)
		waitFor(t, "reader goroutine to exit", func() bool { return readerGoroutines() == 0 })
		expectOneReconnect(t, hook)
	})
}

// TestControlReconnectWaitsForOldHandler pins the ownership transfer: the
// replacement control connection is dialled only after the previous handler has
// fully returned.
func TestControlReconnectWaitsForOldHandler(t *testing.T) {
	c, hook := newControlTransport(t)
	old, _ := newControlConn(t, c, nil)
	oldDone := make(chan struct{})

	go c.reconnectControl(old, oldDone)

	waitFor(t, "old control channel to be dropped", func() bool {
		c.controlMu.Lock()
		defer c.controlMu.Unlock()
		return c.controlChannel == nil
	})
	time.Sleep(100 * time.Millisecond)
	if n := countLogs(hook, "control channel re-dial"); n != 0 {
		t.Fatalf("re-dial started %d times before the old handler finished", n)
	}

	close(oldDone)
	waitFor(t, "re-dial after the old handler finished", func() bool { return countLogs(hook, "control channel re-dial") >= 1 })
}
