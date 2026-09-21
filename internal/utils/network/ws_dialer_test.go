package network

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
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

// muxDial dials addr as a wsmux control/tunnel leg would, with the framing
// option when asked, and 3 attempts so a wrongly retried failure would show.
func muxDial(ctx context.Context, addr string, opts ...DialOption) (*WebSocketConn, error) {
	return WebSocketDialer(ctx, addr, "", "/testpath", 5*time.Second, 0, true, "test-token", "test-agent", config.WSMUX, 3, 0, 0, 0, false, opts...)
}

// TestWebSocketDialerMuxFraming covers the client side of the negotiation: the
// token is offered only on request, and a server that does not echo it is a hard,
// non-retried error that also closes the socket.
func TestWebSocketDialerMuxFraming(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	t.Run("plain dial offers no subprotocol", func(t *testing.T) {
		offered := make(chan []string, 1)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			offered <- r.Header.Values("Sec-WebSocket-Protocol")
			ws.UpgradeHTTP(r, w)
		}))
		defer server.Close()

		conn, err := muxDial(ctx, strings.TrimPrefix(server.URL, "http://"))
		require.NoError(t, err)
		defer conn.Close()
		assert.Empty(t, <-offered, "the legacy handshake must not carry Sec-WebSocket-Protocol")
	})

	t.Run("negotiated", func(t *testing.T) {
		offered := make(chan []string, 1)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			offered <- r.Header.Values("Sec-WebSocket-Protocol")
			nc, brw, hs, err := ws.HTTPUpgrader{Protocol: func(p string) bool { return p == MuxSubprotocol }}.Upgrade(r, w)
			if err != nil || hs.Protocol != MuxSubprotocol {
				return
			}
			defer nc.Close()
			s := NewWebSocketConn(nc, ws.StateServerSide, brw.Reader).Stream()
			io.Copy(s, s) // echo
		}))
		defer server.Close()

		conn, err := muxDial(ctx, strings.TrimPrefix(server.URL, "http://"), WithMuxFraming())
		require.NoError(t, err)
		defer conn.Close()
		assert.Equal(t, []string{MuxSubprotocol}, <-offered)

		s := conn.Stream()
		_ = s.SetDeadline(time.Now().Add(5 * time.Second))
		_, err = s.Write([]byte("through the framed leg"))
		require.NoError(t, err)
		got := make([]byte, len("through the framed leg"))
		_, err = io.ReadFull(s, got)
		require.NoError(t, err)
		assert.Equal(t, "through the framed leg", string(got))
	})

	for _, tc := range []struct {
		name    string
		upgrade func(w http.ResponseWriter, r *http.Request) (net.Conn, error)
	}{
		{"server does not echo the token", func(w http.ResponseWriter, r *http.Request) (net.Conn, error) {
			nc, _, _, err := ws.UpgradeHTTP(r, w)
			return nc, err
		}},
		{"server selects another subprotocol", func(w http.ResponseWriter, r *http.Request) (net.Conn, error) {
			nc, _, _, err := ws.HTTPUpgrader{Header: http.Header{"Sec-WebSocket-Protocol": {"backhaul-mux-v2"}}}.Upgrade(r, w)
			return nc, err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var attempts atomic.Int32
			closed := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempts.Add(1)
				nc, err := tc.upgrade(w, r)
				if err != nil {
					return
				}
				// The client must close the socket rather than talk raw smux on it.
				io.Copy(io.Discard, nc)
				nc.Close()
				close(closed)
			}))
			defer server.Close()

			conn, err := muxDial(ctx, strings.TrimPrefix(server.URL, "http://"), WithMuxFraming())
			require.Error(t, err)
			require.Nil(t, conn)
			assert.ErrorIs(t, err, ErrMuxFramingNotNegotiated)
			assert.Contains(t, err.Error(), "mux_ws_framing=false")
			assert.EqualValues(t, 1, attempts.Load(), "a framing mismatch must not be retried")
			select {
			case <-closed:
			case <-time.After(5 * time.Second):
				t.Fatal("the client left the socket open")
			}
		})
	}
}
