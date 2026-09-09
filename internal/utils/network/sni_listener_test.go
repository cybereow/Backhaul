package network

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeNamedCert drops a throwaway self-signed cert/key for a specific hostname
// (as a SAN) to disk and returns their paths. Used to exercise SNI selection.
func writeNamedCert(t *testing.T, host string) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: host},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{host},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	dir := t.TempDir()
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")

	certOut, _ := os.Create(certPath)
	_ = pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: der})
	certOut.Close()

	keyDER, _ := x509.MarshalECPrivateKey(key)
	keyOut, _ := os.Create(keyPath)
	_ = pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	keyOut.Close()

	return certPath, keyPath
}

// serverCertCommonNameForSNI dials the listener with the given SNI and returns
// the CommonName of the leaf certificate the server presented.
func serverCertCommonNameForSNI(t *testing.T, addr, serverName string) string {
	t.Helper()
	conn, err := tls.Dial("tcp", addr, &tls.Config{
		ServerName:         serverName,
		InsecureSkipVerify: true, // self-signed; we only inspect which cert came back
		MinVersion:         tls.VersionTLS12,
	})
	if err != nil {
		t.Fatalf("dial (sni %q): %v", serverName, err)
	}
	defer conn.Close()
	certs := conn.ConnectionState().PeerCertificates
	if len(certs) == 0 {
		t.Fatalf("sni %q: server presented no certificate", serverName)
	}
	return certs[0].Subject.CommonName
}

// TestNewTLSListenerSNISelectsByServerName verifies the Go engine serves the
// certificate matching the client's SNI, and falls back to the first cert when
// no name matches.
func TestNewTLSListenerSNISelectsByServerName(t *testing.T) {
	certA, keyA := writeNamedCert(t, "alpha.test")
	certB, keyB := writeNamedCert(t, "beta.test")

	ln, err := NewTLSListener(TLSEngineGo, "127.0.0.1:0",
		[]string{certA, certB}, []string{keyA, keyB}, 0, 0)
	if err != nil {
		t.Fatalf("NewTLSListener: %v", err)
	}
	defer ln.Close()

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			// Drive the handshake, then drop it.
			if tc, ok := c.(*tls.Conn); ok {
				_ = tc.Handshake()
			}
			c.Close()
		}
	}()

	addr := ln.Addr().String()
	cases := map[string]string{
		"alpha.test":   "alpha.test", // exact match -> alpha cert
		"beta.test":    "beta.test",  // exact match -> beta cert
		"unknown.test": "alpha.test", // no match -> fallback to first cert
	}
	for sni, wantCN := range cases {
		if got := serverCertCommonNameForSNI(t, addr, sni); got != wantCN {
			t.Errorf("SNI %q: served cert CN=%q, want %q", sni, got, wantCN)
		}
	}
}
