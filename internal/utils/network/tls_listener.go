package network

import (
	"context"
	"fmt"
	"net"
	"syscall"
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

// listenTCPForced builds the raw TCP listener that the OpenSSL listener wraps. It sets
// SO_REUSEADDR/SO_REUSEPORT (so a restart can re-bind the address while the old
// listener is still closing, instead of failing with "address already in use")
// and forces the socket receive/send buffers to rcvBuf/sndBuf (0 = leave the OS
// default).
//
// On Linux an accepted connection inherits the listening socket's buffer sizes,
// so forcing them here lifts every wssmux leg the server accepts out of
// send-buffer autotuning. That mattered for a real asymmetry: the client's
// dialed legs already force their buffers (TcpDialer), so download
// (client -> server) ran at full window, but the server's accepted legs relied
// on tcp_wmem autotuning, which capped the reverse direction - upload
// (server -> client). Forcing the listener's buffers makes the server side
// symmetric. The local port listeners already go through ListenWithBuffers,
// which sets the reuse options too; the TLS listener previously did not, which
// is why a config-change or control-loss restart could hit EADDRINUSE on :443.
//
// sndForce marks sndBuf as a *derived default* rather than an operator-set size:
// it is applied only via SO_SNDBUFFORCE and, if that is unavailable (no
// CAP_NET_ADMIN), left untouched so the socket keeps autotuning instead of being
// pinned to a wmem_max-clamped (possibly tiny) buffer. See setSendBufForce.
func listenTCPForced(addr string, rcvBuf, sndBuf int, sndForce bool) (net.Listener, error) {
	lc := &net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			// Reuse the address/port first, so a restart's fresh listener can
			// bind before the previous generation's socket has fully closed.
			if err := ReusePortControl(network, address, c); err != nil {
				return err
			}
			var setErr error
			if err := c.Control(func(fd uintptr) {
				if rcvBuf > 0 {
					if e := setRecvBuf(int(fd), rcvBuf); e != nil {
						setErr = fmt.Errorf("set SO_RCVBUF: %w", e)
						return
					}
				}
				if sndBuf > 0 {
					if sndForce {
						// Best-effort: a derived default must never pin a clamped
						// buffer, so a FORCE failure just leaves autotuning on.
						_ = setSendBufForce(int(fd), sndBuf)
					} else if e := setSendBuf(int(fd), sndBuf); e != nil {
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
