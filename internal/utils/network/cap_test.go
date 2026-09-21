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

func TestOffersCapability(t *testing.T) {
	hdr := func(vs ...string) http.Header {
		h := http.Header{}
		for _, v := range vs {
			h.Add(CapHeader, v)
		}
		return h
	}
	for _, tc := range []struct {
		name string
		h    http.Header
		want bool
	}{
		{"absent", http.Header{}, false},
		{"exact", hdr("halfclose-v1"), true},
		{"in a list", hdr("a, halfclose-v1 ,b"), true},
		{"repeated header", hdr("a", "halfclose-v1"), true},
		{"other version", hdr("halfclose-v2"), false},
		{"prefix", hdr("halfclose"), false},
		{"suffix", hdr("halfclose-v1x"), false},
		{"the framing subprotocol is a different header", http.Header{"Sec-Websocket-Protocol": {"halfclose-v1"}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, OffersCapability(tc.h, CapHalfCloseV1))
		})
	}
	assert.Equal(t, "X-Backhaul-Cap", CapHeader)
	assert.Equal(t, "halfclose-v1", CapHalfCloseV1)
}

// TestWebSocketDialerHalfCloseOffer: the header is sent only when the option is
// given, is independent of the framing subprotocol, and plain ws dials (no
// options) send none.
func TestWebSocketDialerHalfCloseOffer(t *testing.T) {
	type seen struct{ cap, proto string }
	dial := func(t *testing.T, opts ...DialOption) seen {
		t.Helper()
		got := make(chan seen, 1)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got <- seen{r.Header.Get(CapHeader), r.Header.Get("Sec-WebSocket-Protocol")}
			up := ws.HTTPUpgrader{Protocol: func(p string) bool { return p == MuxSubprotocol }}
			_, _, _, err := up.Upgrade(r, w)
			assert.NoError(t, err)
		}))
		defer server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn, err := WebSocketDialer(ctx, strings.TrimPrefix(server.URL, "http://"), "", "/p/tunnel",
			5*time.Second, 0, true, "tok", "ua", config.WSMUX, 1, 0, 0, 0, false, opts...)
		require.NoError(t, err)
		conn.Close()
		return <-got
	}

	assert.Equal(t, seen{}, dial(t), "a plain dial must send neither header")
	assert.Equal(t, seen{CapHalfCloseV1, ""}, dial(t, WithHalfCloseOffer()))
	assert.Equal(t, seen{"", MuxSubprotocol}, dial(t, WithMuxFraming()))
	assert.Equal(t, seen{CapHalfCloseV1, MuxSubprotocol}, dial(t, WithMuxFraming(), WithHalfCloseOffer()))
}
