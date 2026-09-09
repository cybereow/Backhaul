//go:build openssl

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
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeSelfSignedCert drops a throwaway cert/key pair to disk and returns their
// paths, for feeding the file-based listener constructors.
func writeSelfSignedCert(t *testing.T) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test.local"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"test.local"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	dir := t.TempDir()
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")

	certOut, _ := os.Create(certPath)
	pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: der})
	certOut.Close()

	keyDER, _ := x509.MarshalECPrivateKey(key)
	keyOut, _ := os.Create(keyPath)
	pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	keyOut.Close()

	return certPath, keyPath
}

// negotiatedTLS13Cipher stands up a listener with the given engine, dials it
// with Go's TLS client, and reports the TLS 1.3 cipher the server chose.
func negotiatedTLS13Cipher(t *testing.T, engine, certPath, keyPath string) uint16 {
	t.Helper()
	ln, err := NewTLSListener(engine, "127.0.0.1:0", []string{certPath}, []string{keyPath}, 0, 0)
	if err != nil {
		t.Fatalf("NewTLSListener(%q): %v", engine, err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		buf := make([]byte, 1) // first read drives the server-side handshake
		conn.Read(buf)
		conn.Close()
	}()

	client, err := tls.Dial("tcp", ln.Addr().String(), &tls.Config{
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS13,
		MaxVersion:         tls.VersionTLS13,
	})
	if err != nil {
		t.Fatalf("dial %q: %v", engine, err)
	}
	defer client.Close()
	// Handshake is complete after Dial; nudge a byte so the server's read returns.
	client.Write([]byte{0})
	return client.ConnectionState().CipherSuite
}

// TestOpenSSLMatchesNginxTLS13Cipher is the concrete proof the OpenSSL engine
// exists for: given the identical client, the OpenSSL server negotiates
// TLS_AES_256_GCM_SHA384 - exactly what nginx (also OpenSSL) picks - while Go's
// own stack picks TLS_AES_128_GCM_SHA256 and cannot be configured otherwise.
// That single, directly observable difference is what a censor fingerprinting
// the origin would see.
func TestOpenSSLMatchesNginxTLS13Cipher(t *testing.T) {
	certPath, keyPath := writeSelfSignedCert(t)

	goCipher := negotiatedTLS13Cipher(t, TLSEngineGo, certPath, keyPath)
	if goCipher != tls.TLS_AES_128_GCM_SHA256 {
		t.Logf("note: Go engine negotiated 0x%04x (expected TLS_AES_128_GCM_SHA256); Go default may have shifted", goCipher)
	}

	opensslCipher := negotiatedTLS13Cipher(t, TLSEngineOpenSSL, certPath, keyPath)
	if opensslCipher != tls.TLS_AES_256_GCM_SHA384 {
		t.Fatalf("OpenSSL engine negotiated 0x%04x, want TLS_AES_256_GCM_SHA384 (0x%04x) to match nginx",
			opensslCipher, tls.TLS_AES_256_GCM_SHA384)
	}

	if goCipher == opensslCipher {
		t.Fatalf("expected the engines to diverge on the TLS 1.3 cipher, both chose 0x%04x", goCipher)
	}
	t.Logf("go engine cipher=0x%04x  openssl engine cipher=0x%04x (nginx-like)", goCipher, opensslCipher)
}

// TestOpenSSLListenerServesTLS is a basic liveness check: the OpenSSL listener
// accepts a real TLS connection and yields a usable net.Conn.
func TestOpenSSLListenerServesTLS(t *testing.T) {
	certPath, keyPath := writeSelfSignedCert(t)
	ln, err := NewTLSListener(TLSEngineOpenSSL, "127.0.0.1:0", []string{certPath}, []string{keyPath}, 0, 0)
	if err != nil {
		t.Fatalf("NewTLSListener: %v", err)
	}
	defer ln.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		var _ net.Conn = conn // must satisfy net.Conn
		buf := make([]byte, 5)
		conn.Read(buf)
		conn.Write([]byte("pong"))
		conn.Close()
	}()

	client, err := tls.Dial("tcp", ln.Addr().String(), &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()
	client.Write([]byte("hello"))
	buf := make([]byte, 4)
	if _, err := client.Read(buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != "pong" {
		t.Fatalf("got %q, want pong", buf)
	}
	<-done
}

// TestOpenSSLListenerSNISelectsByServerName verifies the OpenSSL engine's
// servername callback serves the certificate matching the client's SNI, and
// falls back to the first cert when nothing matches.
func TestOpenSSLListenerSNISelectsByServerName(t *testing.T) {
	certA, keyA := writeNamedCert(t, "alpha.test")
	certB, keyB := writeNamedCert(t, "beta.test")

	ln, err := NewTLSListener(TLSEngineOpenSSL, "127.0.0.1:0",
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
			// A read drives the OpenSSL handshake (and its SNI callback).
			buf := make([]byte, 1)
			c.Read(buf)
			c.Close()
		}
	}()

	addr := ln.Addr().String()
	cases := map[string]string{
		"alpha.test":   "alpha.test",
		"beta.test":    "beta.test",
		"unknown.test": "alpha.test", // no match -> fallback to first cert
	}
	for sni, wantCN := range cases {
		if got := serverCertCommonNameForSNI(t, addr, sni); got != wantCN {
			t.Errorf("SNI %q: served cert CN=%q, want %q", sni, got, wantCN)
		}
	}
}
