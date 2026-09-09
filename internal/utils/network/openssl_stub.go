//go:build !openssl

package network

import (
	"errors"
	"net"
)

// newOpenSSLListener is the stub compiled into the default (pure-Go, static)
// build. Selecting the OpenSSL engine there is a configuration error rather
// than a silent downgrade, so the operator learns they need the tagged build.
func newOpenSSLListener(addr string, certFiles, keyFiles []string, rcvBuf, sndBuf int) (net.Listener, error) {
	return nil, errors.New(`tls_engine = "openssl" requires a binary built with the "openssl" build tag (CGO_ENABLED=1 go build -tags openssl)`)
}
