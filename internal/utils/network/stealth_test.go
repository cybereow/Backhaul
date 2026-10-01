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

func TestStealthMuxSubprotocol(t *testing.T) {
	a, b := StealthMuxSubprotocol("token-a"), StealthMuxSubprotocol("token-a")
	assert.Equal(t, a, b, "both ends must derive the same value")
	assert.Contains(t, stealthSubprotocols, a)
	for _, p := range stealthSubprotocols {
		assert.NotContains(t, strings.ToLower(p), "backhaul")
	}
}

func TestStealthCapValue(t *testing.T) {
	v1, err := stealthCapValue("tok")
	require.NoError(t, err)
	v2, err := stealthCapValue("tok")
	require.NoError(t, err)
	assert.Len(t, v1, 32)
	assert.NotEqual(t, v1, v2, "a fresh nonce per request")
	assert.True(t, validStealthCap("tok", v1))
	assert.False(t, validStealthCap("other", v1), "wrong token")
	flipped := "0"
	if v1[31] == '0' {
		flipped = "1" // change the last tag nibble whatever it was
	}
	assert.False(t, validStealthCap("tok", v1[:31]+flipped), "tampered tag")
	assert.False(t, validStealthCap("tok", "nothex"))
	assert.False(t, validStealthCap("tok", ""))
}

func TestMatchMuxSubprotocolAndOffersHalfClose(t *testing.T) {
	const tok = "secret"
	stealth := StealthMuxSubprotocol(tok)

	h := http.Header{}
	_, ok := MatchMuxSubprotocol(h, tok)
	assert.False(t, ok)

	h.Set("Sec-WebSocket-Protocol", "chat-x, "+stealth)
	p, ok := MatchMuxSubprotocol(h, tok)
	assert.True(t, ok)
	assert.Equal(t, stealth, p)

	h.Set("Sec-WebSocket-Protocol", MuxSubprotocol)
	p, ok = MatchMuxSubprotocol(h, tok)
	assert.True(t, ok, "legacy peers are still accepted")
	assert.Equal(t, MuxSubprotocol, p)

	assert.False(t, OffersHalfClose(http.Header{}, tok))
	legacy := http.Header{}
	legacy.Set(CapHeader, CapHalfCloseV1)
	assert.True(t, OffersHalfClose(legacy, tok))
	good := http.Header{}
	v, _ := stealthCapValue(tok)
	good.Set(stealthCapHeader, v)
	assert.True(t, OffersHalfClose(good, tok))
	forged := http.Header{}
	forged.Set(stealthCapHeader, strings.Repeat("a", 32))
	assert.False(t, OffersHalfClose(forged, tok))
}

// A stealth dial must put no project-named string on the wire and still
// negotiate framing and the capability against the server-side matchers.
func TestWebSocketDialerStealthHandshake(t *testing.T) {
	const tok = "stealth-token"
	type seen struct {
		hdr   http.Header
		proto string
	}
	got := make(chan seen, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proto, ok := MatchMuxSubprotocol(r.Header, tok)
		if !ok || !OffersHalfClose(r.Header, tok) {
			http.Error(w, "bad", http.StatusBadRequest)
			return
		}
		nc, _, hs, err := ws.HTTPUpgrader{Protocol: func(p string) bool { return p == proto }}.Upgrade(r, w)
		if err != nil {
			return
		}
		defer nc.Close()
		got <- seen{r.Header.Clone(), hs.Protocol}
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := WebSocketDialer(ctx, strings.TrimPrefix(srv.URL, "http://"), "", "/p", 5*time.Second, 0, true,
		tok, "ua", config.WSMUX, 1, 0, 0, 0, false,
		WithMuxFraming(), WithHalfCloseOffer(), WithStealthHandshake())
	require.NoError(t, err)
	conn.Close()

	s := <-got
	assert.Equal(t, StealthMuxSubprotocol(tok), s.proto)
	assert.Empty(t, s.hdr.Get(CapHeader))
	for k, vs := range s.hdr {
		for _, v := range append([]string{k}, vs...) {
			assert.NotContains(t, strings.ToLower(v), "backhaul", "header %s leaks the project name", k)
		}
	}
}

func TestOriginFor(t *testing.T) {
	assert.Equal(t, "http://example.com", originFor(config.WS, "example.com:80"))
	assert.Equal(t, "http://example.com:8080", originFor(config.WSMUX, "example.com:8080"))
	assert.Equal(t, "https://example.com", originFor(config.WSS, "example.com:443"))
	assert.Equal(t, "https://example.com:8443", originFor(config.WSSMUX, "example.com:8443"))
	assert.Equal(t, "https://[::1]:8443", originFor(config.WSS, "[::1]:8443"))
}
