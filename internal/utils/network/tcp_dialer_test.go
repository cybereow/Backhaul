package network

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTcpDialer_Success(t *testing.T) {
	// Start local TCP listener
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	// Accept in background to allow dialer to complete successfully
	go func() {
		conn, err := l.Accept()
		if err == nil {
			conn.Close()
		}
	}()

	ctx := context.Background()
	conn, err := TcpDialer(ctx, l.Addr().String(), "", 5*time.Second, 10*time.Second, true, 1, 0, 0, 0)
	require.NoError(t, err)
	require.NotNil(t, conn)
	conn.Close()
}

func TestTcpDialer_RetryFail(t *testing.T) {
	ctx := context.Background()

	// Find an unused port by listening and immediately closing
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	l.Close()

	start := time.Now()
	// Retry = 2, so it will attempt, fail, sleep 1s, attempt, fail.
	// Total wait time should be > 1s
	conn, err := TcpDialer(ctx, addr, "", 100*time.Millisecond, 10*time.Second, true, 2, 0, 0, 0)
	duration := time.Since(start)

	require.Error(t, err)
	require.Nil(t, conn)
	require.GreaterOrEqual(t, duration, 1*time.Second)
}

func TestTcpDialer_ContextCancel(t *testing.T) {
	// Start a context that is already canceled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Find an unused port
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	l.Close()

	conn, err := TcpDialer(ctx, addr, "", 5*time.Second, 10*time.Second, true, 1, 0, 0, 0)
	require.Error(t, err)
	require.Nil(t, conn)
	require.ErrorIs(t, err, context.Canceled)
}

func TestTcpDialer_InvalidRemote(t *testing.T) {
	ctx := context.Background()
	// Invalid address format that cannot be resolved
	conn, err := TcpDialer(ctx, "invalid-address-without-port", "", 5*time.Second, 10*time.Second, true, 1, 0, 0, 0)
	require.Error(t, err)
	require.Nil(t, conn)
	require.Contains(t, err.Error(), "DNS resolution")
}

func TestTcpDialer_InvalidLocal(t *testing.T) {
	ctx := context.Background()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	go func() {
		conn, err := l.Accept()
		if err == nil {
			conn.Close()
		}
	}()

	// Invalid local source
	conn, err := TcpDialer(ctx, l.Addr().String(), "invalid-local-ip:1234", 5*time.Second, 10*time.Second, true, 1, 0, 0, 0)
	require.Error(t, err)
	require.Nil(t, conn)
	require.Contains(t, err.Error(), "failed to resolve local address")
}

func TestTcpDialer_WithOptions(t *testing.T) {
	// Start local TCP listener
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	go func() {
		conn, err := l.Accept()
		if err == nil {
			conn.Close()
		}
	}()

	ctx := context.Background()
	// Use arbitrary valid sizes for socket options. SO_RCVBUF, SO_SNDBUF, nodelay = false
	// MSS is set to 0 because TCP_MAXSEG may not be supported to be written on all platforms or loopback.
	conn, err := TcpDialer(ctx, l.Addr().String(), "", 5*time.Second, 10*time.Second, false, 1, 4096, 4096, 0)
	require.NoError(t, err)
	require.NotNil(t, conn)
	conn.Close()
}
