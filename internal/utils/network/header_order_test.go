package network

import (
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/musix/backhaul/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// headerNames returns the header names of a raw request head, in wire order.
func headerNames(head string) []string {
	var names []string
	for _, l := range strings.Split(strings.TrimSuffix(head, "\r\n\r\n"), "\r\n")[1:] {
		k, _, _ := strings.Cut(l, ":")
		names = append(names, k)
	}
	return names
}

func TestReorderUpgradeHead(t *testing.T) {
	// The order gobwas writes: its fixed headers, then the caller's.
	in := "GET /p/1 HTTP/1.1\r\n" +
		"Host: h\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: k==\r\nSec-WebSocket-Protocol: mqtt\r\n" +
		"Authorization: Bearer t\r\nUser-Agent: ua\r\nOrigin: https://h\r\nAccept-Language: en\r\n" +
		"Accept-Encoding: gzip\r\nCache-Control: no-cache\r\nPragma: no-cache\r\nX-Request-Id: abc\r\n\r\n"

	got := string(reorderUpgradeHead([]byte(in)))
	assert.True(t, strings.HasPrefix(got, "GET /p/1 HTTP/1.1\r\n"))
	assert.Equal(t, []string{
		"Host", "Connection", "Pragma", "Cache-Control", "User-Agent", "Upgrade", "Origin",
		"Sec-WebSocket-Version", "Accept-Encoding", "Accept-Language",
		"Authorization", "X-Request-Id", // not browser headers: the cookie slot, before the key
		"Sec-WebSocket-Key", "Sec-WebSocket-Protocol",
	}, headerNames(got))
	assert.True(t, strings.HasSuffix(got, "\r\n\r\n"))
	assert.Len(t, got, len(in), "no header may be added, dropped or changed")

	for _, bad := range []string{
		"", "GET / HTTP/1.1\r\n\r\n", "POST / HTTP/1.1\r\nHost: h\r\n\r\n",
		"GET / HTTP/1.1\r\nno-colon-line\r\n\r\n", "GET / HTTP/1.1\r\n Host: h\r\n\r\n",
		"GET / HTTP/1.1\r\nHost: h\r\n", // head not terminated
	} {
		assert.Equal(t, bad, string(reorderUpgradeHead([]byte(bad))), "malformed input must pass through: %q", bad)
	}
}

// Writes that split the head, and bytes after it, must come out as one reordered
// head followed by the rest, and later writes must pass straight through.
func TestOrderedUpgradeConnWrites(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	c := &orderedUpgradeConn{Conn: a}

	head := "GET / HTTP/1.1\r\nSec-WebSocket-Key: k\r\nHost: h\r\n\r\n"
	got := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 4096)
		var all []byte
		for len(all) < len("GET / HTTP/1.1\r\nHost: h\r\nSec-WebSocket-Key: k\r\n\r\nBODY|later") {
			n, err := b.Read(buf)
			if err != nil {
				break
			}
			all = append(all, buf[:n]...)
		}
		got <- all
	}()

	for _, part := range []string{head[:10], head[10:30], head[30:] + "BODY"} {
		n, err := c.Write([]byte(part))
		require.NoError(t, err)
		assert.Equal(t, len(part), n)
	}
	_, err := c.Write([]byte("|later"))
	require.NoError(t, err)

	select {
	case all := <-got:
		assert.Equal(t, "GET / HTTP/1.1\r\nHost: h\r\nSec-WebSocket-Key: k\r\n\r\nBODY|later", string(all))
	case <-time.After(3 * time.Second):
		t.Fatal("timed out reading the reordered request")
	}
}

// The order asserted here is the one captured from Chromium 141 (see header_order.go),
// restricted to the headers the dialer sends. It also proves the reordered request
// still completes a real gobwas handshake, subprotocol included.
func TestWebSocketDialerHeaderOrderOnTheWire(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	raw := make(chan string, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		var seen bytes.Buffer
		// gobwas reads the request off the wire itself; tee what it reads.
		rw := struct {
			io.Reader
			io.Writer
		}{io.TeeReader(c, &seen), c}
		want := StealthMuxSubprotocol("tok")
		up := ws.Upgrader{Protocol: func(p []byte) bool { return string(p) == want }}
		if _, err := up.Upgrade(rw); err != nil {
			raw <- "upgrade error: " + err.Error()
			return
		}
		raw <- seen.String()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := WebSocketDialer(ctx, ln.Addr().String(), "", "/p", 5*time.Second, 0, true,
		"tok", "ua", config.WSMUX, 1, 0, 0, 0, false,
		WithMuxFraming(), WithHalfCloseOffer(), WithStealthHandshake())
	require.NoError(t, err, "the reordered request must still complete a real handshake")
	conn.Close()

	select {
	case head := <-raw:
		require.False(t, strings.HasPrefix(head, "upgrade error"), head)
		head = head[:strings.Index(head, "\r\n\r\n")+4]
		assert.Equal(t, []string{
			"Host", "Connection", "Pragma", "Cache-Control", "User-Agent", "Upgrade", "Origin",
			"Sec-WebSocket-Version", "Accept-Encoding", "Accept-Language",
			"Authorization", "X-Request-Id",
			"Sec-WebSocket-Key", "Sec-WebSocket-Protocol",
		}, headerNames(head))
	case <-time.After(5 * time.Second):
		t.Fatal("server never saw the request")
	}
}
