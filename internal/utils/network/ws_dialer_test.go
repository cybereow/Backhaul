package network

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/musix/backhaul/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWebSocketDialer_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))
		assert.Equal(t, "test-agent", r.Header.Get("User-Agent"))
		assert.NotEmpty(t, r.Header.Get("X-User-Id"))
		assert.True(t, strings.HasPrefix(r.URL.Path, "/testpath/"))

		_, _, _, err := ws.UpgradeHTTP(r, w)
		require.NoError(t, err)
	}))
	defer server.Close()

	addr := strings.TrimPrefix(server.URL, "http://")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := WebSocketDialer(
		ctx,
		addr,
		"",
		"/testpath",
		5*time.Second,
		0,
		true,
		"test-token",
		"test-agent",
		config.WS,
		3,
		0, 0, 0, false,
	)

	require.NoError(t, err)
	require.NotNil(t, conn)
	conn.Close()
}

func TestWebSocketDialer_RetryThenSuccess(t *testing.T) {
	var attempts int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 1 {
			// Fail the first attempt
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		_, _, _, err := ws.UpgradeHTTP(r, w)
		require.NoError(t, err)
	}))
	defer server.Close()

	addr := strings.TrimPrefix(server.URL, "http://")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now()
	conn, err := WebSocketDialer(
		ctx,
		addr,
		"",
		"/testpath",
		5*time.Second,
		0,
		true,
		"test-token",
		"test-agent",
		config.WS,
		3,
		0, 0, 0, false,
	)

	require.NoError(t, err)
	require.NotNil(t, conn)

	duration := time.Since(start)
	// Should take at least 1 second due to the backoff for the first retry
	assert.GreaterOrEqual(t, duration, 1*time.Second)
	assert.Equal(t, 2, attempts)

	conn.Close()
}

func TestWebSocketDialer_Failure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	defer server.Close()

	addr := strings.TrimPrefix(server.URL, "http://")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := WebSocketDialer(
		ctx,
		addr,
		"",
		"/testpath",
		5*time.Second,
		0,
		true,
		"test-token",
		"test-agent",
		config.WS,
		1, // Only 1 retry attempt (no wait)
		0, 0, 0, false,
	)

	require.Error(t, err)
	require.Nil(t, conn)
	require.Contains(t, err.Error(), "websocket dial failed")
}

func TestWebSocketDialer_ContextCancellation(t *testing.T) {
	// A server that hangs, but we cancel the context immediately
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
	}))
	defer server.Close()

	addr := strings.TrimPrefix(server.URL, "http://")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel before even dialing

	conn, err := WebSocketDialer(
		ctx,
		addr,
		"",
		"/testpath",
		5*time.Second,
		0,
		true,
		"test-token",
		"test-agent",
		config.WS,
		3,
		0, 0, 0, false,
	)

	require.Error(t, err)
	require.Nil(t, conn)
	// Underlying context cancellation will cause a dial error and retry,
	// but context is checked in the loop select block or the dialer.
	// We just ensure it fails fast without waiting 3+ seconds.
}
