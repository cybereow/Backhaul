package network

import (
	"crypto/tls"
	"net"
	"testing"
)

// negotiatedTLS13Cipher stands up a listener, dials it
// with Go's TLS client, and reports the TLS 1.3 cipher the server chose.
func negotiatedTLS13Cipher(t *testing.T, certPath, keyPath string) uint16 {
	t.Helper()
	ln, err := NewTLSListener("127.0.0.1:0", []string{certPath}, []string{keyPath}, 0, 0, false)
	if err != nil {
		t.Fatalf("NewTLSListener: %v", err)
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
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()
	// Handshake is complete after Dial; nudge a byte so the server's read returns.
	client.Write([]byte{0})
	return client.ConnectionState().CipherSuite
}

// TestOpenSSLMatchesNginxTLS13Cipher is the concrete proof the OpenSSL listener
// exists for: given an ordinary client, the server negotiates
// TLS_AES_256_GCM_SHA384 - exactly what nginx (also OpenSSL) picks - where Go's
// own stack would pick TLS_AES_128_GCM_SHA256 and cannot be configured
// otherwise. That directly observable difference is what a censor fingerprinting
// the origin would see.
func TestOpenSSLMatchesNginxTLS13Cipher(t *testing.T) {
	certPath, keyPath := writeNamedCert(t, "test.local")
	if got := negotiatedTLS13Cipher(t, certPath, keyPath); got != tls.TLS_AES_256_GCM_SHA384 {
		t.Fatalf("negotiated 0x%04x, want TLS_AES_256_GCM_SHA384 (0x%04x) to match nginx",
			got, tls.TLS_AES_256_GCM_SHA384)
	}
}

// TestOpenSSLListenerServesTLS is a basic liveness check: the OpenSSL listener
// accepts a real TLS connection and yields a usable net.Conn.
func TestOpenSSLListenerServesTLS(t *testing.T) {
	certPath, keyPath := writeNamedCert(t, "test.local")
	ln, err := NewTLSListener("127.0.0.1:0", []string{certPath}, []string{keyPath}, 0, 0, false)
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

// TestOpenSSLListenerSNISelectsByServerName verifies the servername callback serves the certificate matching the client's SNI, and
// falls back to the first cert when nothing matches.
func TestOpenSSLListenerSNISelectsByServerName(t *testing.T) {
	certA, keyA := writeNamedCert(t, "alpha.test")
	certB, keyB := writeNamedCert(t, "beta.test")

	ln, err := NewTLSListener("127.0.0.1:0",
		[]string{certA, certB}, []string{keyA, keyB}, 0, 0, false)
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
