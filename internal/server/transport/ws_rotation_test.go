package transport

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/musix/backhaul/config"
	client_transport "github.com/musix/backhaul/internal/client/transport"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// startWsE2E runs a ws server and client in-process, with a destination service
// handled by dest, and returns the server's public address.
func startWsE2E(t *testing.T, dest func(net.Conn)) string {
	t.Helper()
	return startWsE2EAged(t, dest, 0)
}

// startWsE2EAged is startWsE2E with idle-connection rotation on: an idle pool
// connection is replaced once it is about maxAge old (see RotationPlan).
func startWsE2EAged(t *testing.T, dest func(net.Conn), maxAge time.Duration) string {
	t.Helper()
	addr, _ := startWsE2EServer(t, dest, maxAge)
	return addr
}

// startWsE2EServer also returns the server, for tests that restart it.
func startWsE2EServer(t *testing.T, dest func(net.Conn), maxAge time.Duration) (string, *WsTransport) {
	t.Helper()
	return startWsE2EFull(t, dest, maxAge, 8, 0)
}

// startWsE2EFull also sets the client's pool size and idle ceiling
// (connection_pool, connection_pool_max).
func startWsE2EFull(t *testing.T, dest func(net.Conn), maxAge time.Duration, pool, poolMax int) (string, *WsTransport) {
	t.Helper()
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	if os.Getenv("RAW_TEST_LOG") != "" {
		logger.SetOutput(os.Stderr)
		logger.SetLevel(logrus.DebugLevel)
		logger.SetFormatter(&logrus.TextFormatter{FullTimestamp: true, TimestampFormat: "05.000"})
	}
	// Its own logger: Restart changes the level of the server's, and in a real
	// deployment the two are different processes.
	clientLogger := logrus.New()
	clientLogger.SetOutput(logger.Out)
	clientLogger.SetLevel(logger.Level)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go dest(c)
		}
	}()

	tunnelPort := testPort(t, "tcp")
	publicPort := testPort(t, "tcp")
	server := NewWSServer(ctx, &WsConfig{
		BindAddr:    fmt.Sprintf("127.0.0.1:%d", tunnelPort),
		Token:       "test_token",
		ChannelSize: 64,
		Heartbeat:   30 * time.Second,
		KeepAlive:   30 * time.Second,
		Nodelay:     true,
		Mode:        config.WS,
		Ports:       []string{fmt.Sprintf("127.0.0.1:%d=%s", publicPort, ln.Addr().String())},
		MaxConnAge:  maxAge,
	}, logger)
	go server.Start()
	time.Sleep(500 * time.Millisecond)

	client := client_transport.NewWSClient(ctx, &client_transport.WsConfig{
		RemoteAddr:   fmt.Sprintf("127.0.0.1:%d", tunnelPort),
		Token:        "test_token",
		KeepAlive:    30 * time.Second,
		DialTimeOut:  10 * time.Second,
		ConnPoolSize: pool,
		ConnPoolMax:  poolMax,
		Nodelay:      true,
		Mode:         config.WS,
		Path:         "/",
	}, clientLogger)
	go client.Start()

	addr := fmt.Sprintf("127.0.0.1:%d", publicPort)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		// A probe flow: the pool is ready once an echo round-trips.
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			c.SetDeadline(time.Now().Add(2 * time.Second))
			if _, err := c.Write([]byte("p")); err == nil {
				c.(*net.TCPConn).CloseWrite()
				b := make([]byte, 1)
				if _, err := io.ReadFull(c, b); err == nil {
					c.Close()
					return addr, server
				}
			}
			c.Close()
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("ws tunnel never carried a probe flow")
	return "", nil
}

func echoDest(c net.Conn) {
	defer c.Close()
	io.Copy(c, c)
}

// eachMode runs f once; kept as a hook so the cases read as a table.
func eachMode(t *testing.T, f func(t *testing.T)) {
	f(t)
}

// Idle pool connections that reached their rotation age are closed by the
// server. They stay queued until a flow picks one; a flow must then be served by
// another connection, not hang on the retired one.
func TestWsFlowsAfterIdleRotation(t *testing.T) {
	eachMode(t, func(t *testing.T) {
		addr := startWsE2EAged(t, echoDest, 2*time.Second)
		time.Sleep(7 * time.Second) // several rotation cycles with no traffic
		for i := 0; i < 12; i++ {
			c, err := net.Dial("tcp", addr)
			require.NoError(t, err)
			c.SetDeadline(time.Now().Add(6 * time.Second))
			msg := []byte(fmt.Sprintf("flow %d", i))
			_, err = c.Write(msg)
			require.NoError(t, err)
			got := make([]byte, len(msg))
			_, err = io.ReadFull(c, got)
			require.NoError(t, err, "flow %d after rotation was not served", i)
			require.Equal(t, msg, got)
			c.Close()
		}
	})
}

// A generation that has had idle connections retired still stops: nothing is
// left waiting on a retired connection's lock, so Restart joins at once instead
// of running into its 30s join timeout and retrying for ever.
func TestWsRestartAfterIdleRotation(t *testing.T) {
	eachMode(t, func(t *testing.T) {
		addr, srv := startWsE2EServer(t, echoDest, 2*time.Second)
		time.Sleep(6 * time.Second) // rotation cycles; retired entries stay queued
		// Make the handle loops dequeue them: a burst of flows.
		for i := 0; i < 8; i++ {
			c, err := net.Dial("tcp", addr)
			require.NoError(t, err)
			c.SetDeadline(time.Now().Add(6 * time.Second))
			c.Write([]byte("x"))
			b := make([]byte, 1)
			_, err = io.ReadFull(c, b)
			require.NoError(t, err)
			c.Close()
		}
		before := srv.gen
		start := time.Now()
		done := make(chan struct{})
		go func() { srv.Restart(); close(done) }()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Fatal("Restart did not return: a worker of the old generation is stuck")
		}
		require.Less(t, time.Since(start), 10*time.Second)
		require.NotSame(t, before, srv.gen, "no new generation was started")
	})
}

// A burst of overlapping flows is served in full, and the idle connections the
// client keeps (as the server sees them queued) stay under the ceiling.
func TestWsIdleCeilingBoundsThePoolWithoutStarvingFlows(t *testing.T) {
	const pool, ceiling = 2, 6
	slow := func(c net.Conn) {
		defer c.Close()
		b := make([]byte, 1)
		if _, err := io.ReadFull(c, b); err != nil {
			return
		}
		time.Sleep(700 * time.Millisecond) // keep the flow open so they overlap
		c.Write(b)
	}
	eachMode(t, func(t *testing.T) {
		addr, srv := startWsE2EFull(t, slow, 0, pool, ceiling)

		var peak int32
		stop := make(chan struct{})
		go func() {
			for {
				select {
				case <-stop:
					return
				case <-time.After(10 * time.Millisecond):
					if n := int32(len(srv.tunnelChannel)); n > atomic.LoadInt32(&peak) {
						atomic.StoreInt32(&peak, n)
					}
				}
			}
		}()
		defer close(stop)

		const flows = 30
		var wg sync.WaitGroup
		errs := make(chan error, flows)
		for i := 0; i < flows; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				c, err := net.Dial("tcp", addr)
				if err != nil {
					errs <- err
					return
				}
				defer c.Close()
				c.SetDeadline(time.Now().Add(20 * time.Second))
				if _, err := c.Write([]byte("x")); err != nil {
					errs <- err
					return
				}
				b := make([]byte, 1)
				if _, err := io.ReadFull(c, b); err != nil {
					errs <- err
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Errorf("a flow of the burst was not served: %v", err)
		}
		time.Sleep(500 * time.Millisecond) // let the pool settle
		if p := atomic.LoadInt32(&peak); p > ceiling {
			t.Errorf("the server saw %d idle connections queued, over the ceiling of %d", p, ceiling)
		}
	})
}
