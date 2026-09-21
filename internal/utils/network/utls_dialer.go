package network

import (
	"context"
	"fmt"
	"net"
	"time"

	utls "github.com/refraction-networking/utls"
)

// sessionCache is shared across every uTLS dial in the process so that
// reconnects and pool connections can resume a prior TLS session instead of
// always doing a full handshake. Real Chrome does the same for repeat
// connections to a host - a full handshake on every single reconnect is both
// slower and one more thing a DPI system watching handshake shape can key
// on. 128 entries comfortably covers one or a handful of remote hosts plus
// edge IP rotation.
var sessionCache = utls.NewLRUClientSessionCache(128)

// UtlsDialTLS dials a TCP connection (with the same tuning knobs as TcpDialer)
// and performs the TLS handshake using uTLS with a Chrome ClientHello, so the
// outbound TLS fingerprint (JA3/JA4) blends in with real browser traffic
// instead of standing out as Go's crypto/tls default, which is a known DPI
// signal distinct from the CDN's usual traffic.
func UtlsDialTLS(ctx context.Context, dialAddr string, serverName string, insecureSkipVerify bool, nextProtos []string, timeout time.Duration, keepAlive time.Duration, nodelay bool, SO_RCVBUF int, SO_SNDBUF int, mss int) (net.Conn, error) {
	rawConn, err := TcpDialer(ctx, dialAddr, "", timeout, keepAlive, nodelay, 1, SO_RCVBUF, SO_SNDBUF, mss)
	if err != nil {
		return nil, err
	}

	uConfig := &utls.Config{
		ServerName:         serverName,
		InsecureSkipVerify: insecureSkipVerify,
		NextProtos:         nextProtos,
		ClientSessionCache: sessionCache,
		// HelloChrome_Auto's canned spec doesn't carry a PSK/session-ticket
		// extension, so uTLS has nowhere to place resumption data even
		// though ClientSessionCache is set. Without this, uTLS panics
		// instead of just falling back to a full handshake - resumption
		// still kicks in on specs that do carry the extension.
		PreferSkipResumptionOnNilExtension: true,
	}

	// The Chrome preset's canned spec carries its own hardcoded ALPN
	// extension (real Chrome always offers ["h2", "http/1.1"]), which
	// overrides Config.NextProtos when the preset is applied as-is. wss/
	// wssmux ride gobwas/ws, which only understands HTTP/1.1 - if
	// the server picks "h2" off that hardcoded list, gobwas's plain
	// HTTP/1.1 Upgrade request lands on an HTTP/2 connection and fails.
	// Building the spec explicitly and overwriting its ALPN extension
	// keeps every other part of the Chrome fingerprint intact while
	// constraining negotiation to what the caller actually asked for.
	spec, err := utls.UTLSIdToSpec(utls.HelloChrome_Auto)
	if err != nil {
		rawConn.Close()
		return nil, fmt.Errorf("utls spec generation failed: %w", err)
	}
	for _, ext := range spec.Extensions {
		if alpn, ok := ext.(*utls.ALPNExtension); ok {
			alpn.AlpnProtocols = nextProtos
		}
	}

	uConn := utls.UClient(rawConn, uConfig, utls.HelloCustom)
	if err := uConn.ApplyPreset(&spec); err != nil {
		rawConn.Close()
		return nil, fmt.Errorf("utls preset application failed: %w", err)
	}
	if err := uConn.HandshakeContext(ctx); err != nil {
		rawConn.Close()
		return nil, fmt.Errorf("utls handshake failed: %w", err)
	}

	return uConn, nil
}
