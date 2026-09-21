package network

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gobwas/ws"
	"github.com/musix/backhaul/config"
)

// ErrMuxFramingNotNegotiated is returned by a dial that asked for standards
// framed mux (WithMuxFraming) when the server did not confirm the
// MuxSubprotocol. There is no fallback to raw mode: the caller must surface it.
var ErrMuxFramingNotNegotiated = errors.New("server did not confirm the " + MuxSubprotocol + " websocket subprotocol: upgrade the server, or set mux_ws_framing=false on both ends")

// CapHeader carries a comma-separated list of capability tokens a wsmux/wssmux
// client offers on every upgrade request; CapHalfCloseV1 announces support for
// the FlowPlainHC half-close envelope (plan 024). It is separate from the
// Sec-WebSocket-Protocol token above and touches no frame or smux version.
const (
	CapHeader      = "X-Backhaul-Cap"
	CapHalfCloseV1 = "halfclose-v1"
)

// OffersCapability reports whether an upgrade request lists the exact capability
// token. Unknown tokens are ignored; a near miss counts as absent.
func OffersCapability(h http.Header, token string) bool {
	for _, v := range h.Values(CapHeader) {
		for _, tok := range strings.Split(v, ",") {
			if strings.TrimSpace(tok) == token {
				return true
			}
		}
	}
	return false
}

// DialOption tunes one WebSocketDialer call. The default (no options) is the
// plain ws/wss handshake, byte for byte.
type DialOption func(*dialOptions)

type dialOptions struct{ muxFraming, offerHalfClose bool }

// WithMuxFraming offers MuxSubprotocol in the upgrade request and requires the
// server to echo it; otherwise the dial fails with ErrMuxFramingNotNegotiated.
// Only the wsmux/wssmux endpoints use it.
func WithMuxFraming() DialOption { return func(o *dialOptions) { o.muxFraming = true } }

// WithHalfCloseOffer adds the CapHeader: CapHalfCloseV1 capability offer to the
// upgrade request. It is independent of WithMuxFraming (the subprotocol): each
// is checked, and can be rejected, on its own. Only wsmux/wssmux clients use it.
func WithHalfCloseOffer() DialOption { return func(o *dialOptions) { o.offerHalfClose = true } }

func WebSocketDialer(ctx context.Context, addr string, edgeIP string, path string, timeout time.Duration, keepalive time.Duration, nodelay bool, token string, userAgent string, mode config.TransportType, retry int, SO_RCVBUF int, SO_SNDBUF int, mss int, tlsVerify bool, opts ...DialOption) (*WebSocketConn, error) {
	var tunnelWSConn *WebSocketConn
	var err error

	var o dialOptions
	for _, opt := range opts {
		opt(&o)
	}

	retries := retry           // Number of retries
	backoff := 1 * time.Second // Initial backoff duration

	for i := 0; i < retries; i++ {
		// Attempt to dial the WebSocket
		tunnelWSConn, err = attemptDialWebSocket(ctx, addr, edgeIP, path, timeout, keepalive, nodelay, token, userAgent, mode, SO_RCVBUF, SO_SNDBUF, mss, tlsVerify, o.muxFraming, o.offerHalfClose)
		if err == nil {
			// If successful, return the connection
			return tunnelWSConn, nil
		}

		// A server that will not speak the framing is a configuration mismatch,
		// not a transient failure: retrying with backoff would only hide it.
		if errors.Is(err, ErrMuxFramingNotNegotiated) {
			return nil, err
		}

		// If this is the last retry, return the error
		if i == retries-1 {
			break
		}

		// Wait before retrying, but abandon the wait if the transport is
		// already shutting down. An unconditional Sleep here held a restart for
		// up to 1+2=3 seconds per in-flight dial (longer with a higher retry
		// count), and every pool connection dialing at once meant a restart
		// waited on all of them.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2 // Exponential backoff (double the wait time after each failure)
	}

	return nil, err
}

func attemptDialWebSocket(ctx context.Context, addr string, edgeIP string, path string, timeout time.Duration, keepalive time.Duration, nodelay bool, token string, userAgent string, mode config.TransportType, SO_RCVBUF int, SO_SNDBUF int, mss int, tlsVerify bool, muxFraming bool, offerHalfClose bool) (*WebSocketConn, error) {
	// Generate a random X-user-id
	n, err := rand.Int(rand.Reader, big.NewInt(1<<31))
	if err != nil {
		return nil, fmt.Errorf("failed to generate random user ID: %w", err)
	}
	randomUserID := int32(n.Int64())

	// Setup headers with authorization and X-user-id
	headers := http.Header{}
	headers.Add("Authorization", fmt.Sprintf("Bearer %v", token))
	headers.Add("X-User-Id", fmt.Sprintf("%d", randomUserID))
	headers.Add("User-Agent", userAgent)
	if offerHalfClose {
		headers.Add(CapHeader, CapHalfCloseV1)
	}

	var wsURL string
	dialer := ws.Dialer{Header: ws.HandshakeHeaderHTTP(http.Header{})}

	// Handle edgeIP assignment
	if edgeIP != "" {
		_, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, fmt.Errorf("invalid address format, failed to parse: %w", err)
		}

		// ponytail: JoinHostPort brackets IPv6 bare literals; Sprintf("%s:%s") would not.
		edgeIP = net.JoinHostPort(edgeIP, port)
	} else {
		edgeIP = addr
	}

	// path generation; only the tunnel endpoint gets a random suffix, the
	// control channel path is used as-is (with or without a custom base path)
	if !strings.HasSuffix(path, "/channel") {
		path = fmt.Sprintf("%s/%s", path, strconv.Itoa(int(randomUserID)))
	}

	switch mode {
	case config.WS, config.WSMUX:
		wsURL = fmt.Sprintf("ws://%s%s", addr, path)

		dialer = ws.Dialer{
			Header:  ws.HandshakeHeaderHTTP(headers),
			Timeout: 45 * time.Second,
			NetDial: func(ctx context.Context, _, addr string) (net.Conn, error) {
				conn, err := TcpDialer(ctx, edgeIP, "", timeout, keepalive, nodelay, 1, SO_RCVBUF, SO_SNDBUF, mss)
				if err != nil {
					return nil, err
				}
				return conn, nil
			},
		}
	case config.WSS, config.WSSMUX:
		wsURL = fmt.Sprintf("ws://%s%s", addr, path) // gobwas will double-wrap if wss:// is used

		sniHost, _, err := net.SplitHostPort(addr)
		if err != nil {
			sniHost = addr
		}

		dialer = ws.Dialer{
			Header:  ws.HandshakeHeaderHTTP(headers),
			Timeout: 45 * time.Second,
			NetDial: func(ctx context.Context, _, _ string) (net.Conn, error) {
				// insecureSkipVerify is the inverse of the operator's tls_verify:
				// off by default (self-signed friendly), but an on-path party can
				// then MITM the token-bearing handshake, so tls_verify=true is
				// available to pin the certificate.
				return UtlsDialTLS(ctx, edgeIP, sniHost, !tlsVerify, []string{"http/1.1"}, timeout, keepalive, nodelay, SO_RCVBUF, SO_SNDBUF, mss)
			},
		}
	}

	if muxFraming {
		dialer.Protocols = []string{MuxSubprotocol}
	}

	// Dial to the WebSocket server
	conn, br, hs, err := dialer.Dial(ctx, wsURL)
	if err != nil {
		if muxFraming && errors.Is(err, ws.ErrHandshakeBadSubProtocol) {
			// The server selected some other subprotocol than the one offered.
			err = ErrMuxFramingNotNegotiated
		}
		return nil, fmt.Errorf("websocket dial failed: %w", err)
	}
	if muxFraming && hs.Protocol != MuxSubprotocol {
		// The upgrade succeeded but the server (an old build, or one running
		// legacy raw mode) did not echo the token. Its post-upgrade bytes would
		// be raw smux, so proceeding would corrupt the stream: fail loudly.
		conn.Close()
		return nil, fmt.Errorf("websocket dial failed: %w", ErrMuxFramingNotNegotiated)
	}
	return NewWebSocketConn(conn, ws.StateClientSide, br), nil
}
