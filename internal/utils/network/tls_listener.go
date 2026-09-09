package network

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"syscall"
)

// TLS engine names for the server's wss/wssmux `tls_engine` option.
const (
	TLSEngineGo      = "go"      // Go crypto/tls - the default; keeps pure-Go static builds
	TLSEngineOpenSSL = "openssl" // system OpenSSL via cgo - requires a binary built with -tags openssl
)

// ResolveCertPairs merges the single-cert (tls_cert/tls_key) and multi-cert
// (tls_certs/tls_keys) config into one aligned pair of slices. The multi-cert
// list wins when present; otherwise the single pair is used. This keeps the
// existing single-domain config working unchanged while enabling SNI-based
// multi-domain termination when a list is supplied.
func ResolveCertPairs(certFile, keyFile string, certFiles, keyFiles []string) ([]string, []string) {
	if len(certFiles) > 0 {
		return certFiles, keyFiles
	}
	return []string{certFile}, []string{keyFile}
}

// NewTLSListener returns a listener that terminates TLS with the chosen engine.
// The listener yields already-decrypted net.Conns, so callers hand it straight
// to http.Server.Serve.
//
// certFiles/keyFiles are aligned by index. With one pair it's ordinary
// single-cert termination; with several, the server selects a certificate by
// the client's SNI (Go's stack does this automatically from each leaf's SANs;
// the OpenSSL engine does it via a servername callback). certFiles[0] is the
// default served when no SNI matches.
//
// engine "" or "go" uses Go's crypto/tls. engine "openssl" uses the system
// OpenSSL library, whose server-side handshake fingerprint (TLS 1.3 cipher
// choice, ServerHello extension order) matches a same-version nginx far more
// closely than Go's stack can be made to - which matters when the origin's IP
// is directly reachable and a censor can fingerprint it. The OpenSSL path only
// exists in binaries built with the "openssl" build tag; otherwise it returns
// an explanatory error instead of silently falling back.
func NewTLSListener(engine, addr string, certFiles, keyFiles []string, rcvBuf, sndBuf int) (net.Listener, error) {
	if len(certFiles) == 0 || len(certFiles) != len(keyFiles) {
		return nil, fmt.Errorf("tls: need matching cert/key file lists (got %d certs, %d keys)", len(certFiles), len(keyFiles))
	}
	switch engine {
	case "", TLSEngineGo:
		certs := make([]tls.Certificate, 0, len(certFiles))
		for i := range certFiles {
			cert, err := tls.LoadX509KeyPair(certFiles[i], keyFiles[i])
			if err != nil {
				return nil, fmt.Errorf("load tls keypair %q/%q: %w", certFiles[i], keyFiles[i], err)
			}
			certs = append(certs, cert)
		}
		cfg := &tls.Config{
			// Go builds an SNI map from the leaves' SANs and falls back to
			// certs[0] when nothing matches.
			Certificates: certs,
			MinVersion:   tls.VersionTLS12,
		}
		inner, err := listenTCPForced(addr, rcvBuf, sndBuf)
		if err != nil {
			return nil, err
		}
		return tls.NewListener(inner, cfg), nil
	case TLSEngineOpenSSL:
		return newOpenSSLListener(addr, certFiles, keyFiles, rcvBuf, sndBuf)
	default:
		return nil, fmt.Errorf("unknown tls_engine %q (want %q or %q)", engine, TLSEngineGo, TLSEngineOpenSSL)
	}
}

// listenTCPForced builds the raw TCP listener that a TLS engine wraps, forcing
// its socket receive/send buffers to rcvBuf/sndBuf (0 = leave the OS default).
// On Linux an accepted connection inherits the listening socket's buffer sizes,
// so this lifts every wssmux leg the server accepts out of send-buffer
// autotuning. That mattered for a real asymmetry: the client's dialed legs
// already force their buffers (TcpDialer), so download (client -> server) ran at
// full window, but the server's accepted legs relied on tcp_wmem autotuning,
// which capped the reverse direction - upload (server -> client) - well below
// line rate. Forcing the listener's buffers makes the server side symmetric.
func listenTCPForced(addr string, rcvBuf, sndBuf int) (net.Listener, error) {
	lc := &net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var setErr error
			if err := c.Control(func(fd uintptr) {
				if rcvBuf > 0 {
					if e := setRecvBuf(int(fd), rcvBuf); e != nil {
						setErr = fmt.Errorf("set SO_RCVBUF: %w", e)
						return
					}
				}
				if sndBuf > 0 {
					if e := setSendBuf(int(fd), sndBuf); e != nil {
						setErr = fmt.Errorf("set SO_SNDBUF: %w", e)
						return
					}
				}
			}); err != nil {
				return err
			}
			return setErr
		},
	}
	return lc.Listen(context.Background(), "tcp", addr)
}
