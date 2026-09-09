//go:build openssl

package network

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"os"

	openssl "github.com/libp2p/go-openssl"
)

// newOpenSSLListener terminates TLS with the system OpenSSL library instead of
// Go's crypto/tls. The SSL_CTX is configured to mirror a stock nginx:
//
//   - TLS 1.2 + 1.3 only (SSLv2/3 disabled).
//   - Server cipher preference on - nginx's `ssl_prefer_server_ciphers on`. On
//     OpenSSL 3.0 this makes TLS 1.3 negotiate AES-256-GCM first, the same
//     choice nginx makes and the one Go's stack cannot be configured to make
//     (Go hardcodes AES-128-GCM first when AES-NI is present, and does not
//     expose TLS 1.3 ciphersuite ordering at all).
//   - ALPN advertising http/1.1.
//
// Because this is the same OpenSSL that nginx links, the ServerHello it emits -
// extension order included - matches nginx's, so a direct probe of the origin
// can't distinguish them on the TLS handshake. That is the whole reason this
// path exists.
//
// Caveats, stated plainly:
//   - Match nginx's OpenSSL *major* version for the closest fingerprint; build
//     against and run on the same one.
//   - nginx's `http2` directive also advertises h2 in ALPN. This listener
//     speaks HTTP/1.1 only (the WebSocket tunnel needs it), so for the tightest
//     parity drop `http2` from any nginx you benchmark against.
//   - The TLS 1.3 ciphersuite list is left at OpenSSL's default on purpose - it
//     already leads with AES-256-GCM exactly as nginx does.
//
// newConfiguredCtx builds an SSL_CTX for one cert/key pair, tuned to mirror a
// stock nginx (see the type comment above). Each SNI-selectable certificate
// gets its own ctx; they are configured identically apart from the keypair.
func newConfiguredCtx(certFile, keyFile string) (*openssl.Ctx, error) {
	ctx, err := openssl.NewCtxFromFiles(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("openssl ctx from files: %w", err)
	}
	if !ctx.SetMinProtoVersion(openssl.TLS1_2_VERSION) {
		return nil, fmt.Errorf("openssl: failed to set min proto version")
	}
	if !ctx.SetMaxProtoVersion(openssl.TLS1_3_VERSION) {
		return nil, fmt.Errorf("openssl: failed to set max proto version")
	}
	ctx.SetOptions(openssl.CipherServerPreference | openssl.NoSSLv2 | openssl.NoSSLv3)
	if err := ctx.SetNextProtos([]string{"http/1.1"}); err != nil {
		return nil, fmt.Errorf("openssl set alpn: %w", err)
	}
	return ctx, nil
}

// leafFromPEM parses the first CERTIFICATE block of a PEM file into an
// x509.Certificate, used only to match a client's SNI against the cert's names.
func leafFromPEM(certFile string) (*x509.Certificate, error) {
	data, err := os.ReadFile(certFile)
	if err != nil {
		return nil, err
	}
	for len(data) > 0 {
		var block *pem.Block
		block, data = pem.Decode(data)
		if block == nil {
			break
		}
		if block.Type == "CERTIFICATE" {
			return x509.ParseCertificate(block.Bytes)
		}
	}
	return nil, fmt.Errorf("no CERTIFICATE block found in %q", certFile)
}

// newOpenSSLListener terminates TLS with OpenSSL. With a single cert/key pair
// it behaves exactly as before. With several, it selects a certificate by the
// client's SNI via a servername callback: the first cert whose leaf validates
// for the requested host wins; certFiles[0] is the fallback when nothing
// matches. Every cert gets an identically-tuned ctx, so the nginx-matching
// fingerprint is preserved whichever one is chosen.
func newOpenSSLListener(addr string, certFiles, keyFiles []string, rcvBuf, sndBuf int) (net.Listener, error) {
	base, err := newConfiguredCtx(certFiles[0], keyFiles[0])
	if err != nil {
		return nil, err
	}

	if len(certFiles) > 1 {
		type sniCert struct {
			leaf *x509.Certificate
			ctx  *openssl.Ctx
		}
		certs := make([]sniCert, 0, len(certFiles))
		for i := range certFiles {
			ctx := base
			if i > 0 {
				ctx, err = newConfiguredCtx(certFiles[i], keyFiles[i])
				if err != nil {
					return nil, err
				}
			}
			leaf, err := leafFromPEM(certFiles[i])
			if err != nil {
				return nil, fmt.Errorf("parse cert %q: %w", certFiles[i], err)
			}
			certs = append(certs, sniCert{leaf: leaf, ctx: ctx})
		}
		// SSL_set_SSL_CTX only swaps the certificate; the SSL keeps the base
		// ctx's proto/cipher/ALPN tuning (which every ctx shares anyway), so
		// the handshake fingerprint is unchanged by the selection.
		base.SetTLSExtServernameCallback(func(ssl *openssl.SSL) openssl.SSLTLSExtErr {
			if name := ssl.GetServername(); name != "" {
				for _, c := range certs {
					if c.leaf.VerifyHostname(name) == nil {
						ssl.SetSSLCtx(c.ctx)
						break
					}
				}
			}
			return openssl.SSLTLSExtErrOK
		})
	}

	// Build the raw TCP listener ourselves so its socket buffers are forced
	// (accepted conns inherit them), then let OpenSSL wrap each accepted conn.
	inner, err := listenTCPForced(addr, rcvBuf, sndBuf)
	if err != nil {
		return nil, err
	}
	return openssl.NewListener(inner, base), nil
}
