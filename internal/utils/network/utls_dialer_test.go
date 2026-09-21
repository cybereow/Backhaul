package network

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/musix/backhaul/config"
	utls "github.com/refraction-networking/utls"
	"github.com/stretchr/testify/require"
)

// These tests check that wss/wssmux verify the server certificate chain and
// hostname by default (normal PKI validation, not pinning) and that only an
// explicit tls_verify=false opts out. Every identity is generated here, in
// memory; no real certificate, key or token is used and the machine trust
// store is never touched.
//
// A trusted-success case needs a trusted root without a production hook, so it
// runs in a child test process (TestTLSVerificationHelper) whose own
// SSL_CERT_FILE / SSL_CERT_DIR point at the generated CA. That is Linux-only
// (Go reads those variables only on Unix), so it is skipped elsewhere. Cases
// that need no trust (untrusted CA fails, opt-out succeeds, cancel) run
// in-process.

const helperEnv = "BACKHAUL_TLS_VERIFICATION_HELPER"

func randSerial(t *testing.T) *big.Int {
	t.Helper()
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 100))
	require.NoError(t, err)
	return n
}

func newTestCA(t *testing.T, cn string) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          randSerial(t),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return cert, key
}

// newTestLeaf issues a server certificate valid for "localhost" and 127.0.0.1.
func newTestLeaf(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: randSerial(t),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	require.NoError(t, err)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

type wsRequestSeen struct{ host, sni, alpn string }

type tlsFixture struct {
	addr  string // 127.0.0.1:port
	port  string
	sniCh chan string        // SNI of every ClientHello
	reqCh chan wsRequestSeen // HTTP Host / SNI / ALPN of every upgrade request
}

// startTLSServer serves cert over HTTP/1.1-only TLS and accepts websocket
// upgrades. It records what the client presented so tests can assert the SNI,
// Host header and negotiated ALPN.
func startTLSServer(t *testing.T, cert tls.Certificate) *tlsFixture {
	t.Helper()
	f := &tlsFixture{
		sniCh: make(chan string, 8),
		reqCh: make(chan wsRequestSeen, 8),
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case f.reqCh <- wsRequestSeen{host: r.Host, sni: r.TLS.ServerName, alpn: r.TLS.NegotiatedProtocol}:
		default:
		}
		conn, _, _, err := ws.UpgradeHTTP(r, w)
		if err == nil {
			conn.Close()
		}
	}))
	srv.Config.ErrorLog = log.New(io.Discard, "", 0) // failed handshakes are expected
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"http/1.1"},
		GetConfigForClient: func(chi *tls.ClientHelloInfo) (*tls.Config, error) {
			select {
			case f.sniCh <- chi.ServerName:
			default:
			}
			return nil, nil
		},
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	f.addr = strings.TrimPrefix(srv.URL, "https://")
	_, f.port, _ = net.SplitHostPort(f.addr)
	return f
}

func recvWithin[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}

func requireLinuxTrustFixture(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("trusted-CA fixture needs SSL_CERT_FILE/SSL_CERT_DIR, which Go honours only on Linux/Unix")
	}
}

// runTrustedChild re-executes this test binary as TestTLSVerificationHelper
// with the generated CA as its only trust root, and returns the helper's
// RESULT line. The parent owns the server; the child has a finite timeout and
// cannot recurse (the helper only runs when helperEnv is set and is not
// matched by the parent tests' names).
func runTrustedChild(t *testing.T, ca *x509.Certificate, env ...string) string {
	t.Helper()
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	emptyDir := filepath.Join(dir, "empty")
	require.NoError(t, os.Mkdir(emptyDir, 0o700))
	require.NoError(t, os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.Raw}), 0o600))

	exe, err := os.Executable()
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestTLSVerificationHelper$", "-test.count=1", "-test.timeout=45s")
	cmd.Env = append(os.Environ(), "SSL_CERT_FILE="+caFile, "SSL_CERT_DIR="+emptyDir, helperEnv+"=1")
	cmd.Env = append(cmd.Env, env...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	require.NoError(t, cmd.Run(), "helper failed\nstdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String())
	for _, line := range strings.Split(stdout.String(), "\n") {
		if strings.HasPrefix(line, "RESULT:") {
			return strings.TrimSpace(line)
		}
	}
	t.Fatalf("helper printed no RESULT line\nstdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String())
	return ""
}

// TestTLSVerificationHelper is the child half of the trusted-CA cases. It is a
// no-op unless launched by runTrustedChild.
func TestTLSVerificationHelper(t *testing.T) {
	if os.Getenv(helperEnv) != "1" {
		t.Skip("subprocess helper; launched by TestUtlsVerification / TestWebSocketTLSVerification")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	fail := func(err error) {
		fmt.Printf("RESULT:err:%s\n", strings.ReplaceAll(err.Error(), "\n", " "))
	}
	dial, host, edge := os.Getenv("TLSH_DIAL"), os.Getenv("TLSH_HOST"), os.Getenv("TLSH_EDGE")

	switch mode := os.Getenv("TLSH_MODE"); mode {
	case "utls":
		conn, err := UtlsDialTLS(ctx, dial, host, false, []string{"http/1.1"}, 5*time.Second, 0, true, 0, 0, 0)
		if err != nil {
			fail(err)
			return
		}
		defer conn.Close()
		fmt.Printf("RESULT:ok:%s\n", conn.(*utls.UConn).ConnectionState().NegotiatedProtocol)
	case string(config.WSS), string(config.WSSMUX):
		conn, err := WebSocketDialer(ctx, host, edge, "/ws", 5*time.Second, 0, true, "test-token", "test-agent", config.TransportType(mode), 1, 0, 0, 0, true)
		if err != nil {
			fail(err)
			return
		}
		conn.Close()
		fmt.Println("RESULT:ok:")
	default:
		t.Fatalf("unknown TLSH_MODE %q", mode)
	}
}

func TestUtlsVerification(t *testing.T) {
	ca, caKey := newTestCA(t, "trusted test CA")
	trusted := startTLSServer(t, newTestLeaf(t, ca, caKey))
	otherCA, otherKey := newTestCA(t, "untrusted test CA")
	untrusted := startTLSServer(t, newTestLeaf(t, otherCA, otherKey))
	alpn := []string{"http/1.1"}

	t.Run("trusted CA and matching hostname succeeds over http/1.1", func(t *testing.T) {
		requireLinuxTrustFixture(t)
		got := runTrustedChild(t, ca, "TLSH_MODE=utls", "TLSH_DIAL="+trusted.addr, "TLSH_HOST=localhost")
		require.Equal(t, "RESULT:ok:http/1.1", got)
		require.Equal(t, "localhost", recvWithin(t, trusted.sniCh, "ClientHello SNI"))
	})

	t.Run("hostname mismatch fails", func(t *testing.T) {
		requireLinuxTrustFixture(t)
		got := runTrustedChild(t, ca, "TLSH_MODE=utls", "TLSH_DIAL="+trusted.addr, "TLSH_HOST=wrong.example")
		require.Contains(t, got, "RESULT:err:")
		require.Contains(t, got, "not wrong.example") // hostname error, not unknown-authority
		require.Equal(t, "wrong.example", recvWithin(t, trusted.sniCh, "ClientHello SNI"))
	})

	t.Run("untrusted CA fails", func(t *testing.T) {
		conn, err := UtlsDialTLS(context.Background(), untrusted.addr, "localhost", false, alpn, 5*time.Second, 0, true, 0, 0, 0)
		require.Error(t, err)
		require.Nil(t, conn)
		require.Contains(t, err.Error(), "x509")
	})

	// Explicit insecure override: a generated, untrusted identity is accepted
	// only because insecureSkipVerify (tls_verify=false) is passed on purpose.
	t.Run("explicit opt-out accepts untrusted CA", func(t *testing.T) {
		conn, err := UtlsDialTLS(context.Background(), untrusted.addr, "localhost", true, alpn, 5*time.Second, 0, true, 0, 0, 0)
		require.NoError(t, err)
		require.Equal(t, "http/1.1", conn.(*utls.UConn).ConnectionState().NegotiatedProtocol)
		conn.Close()
	})

	t.Run("canceled handshake returns and closes the socket", func(t *testing.T) {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer l.Close()
		closed := make(chan struct{})
		go func() {
			c, err := l.Accept()
			if err != nil {
				return
			}
			defer c.Close()
			io.Copy(io.Discard, c) // swallow the ClientHello, never answer
			close(closed)
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		start := time.Now()
		conn, err := UtlsDialTLS(ctx, l.Addr().String(), "localhost", false, alpn, 5*time.Second, 0, true, 0, 0, 0)
		require.Error(t, err)
		require.Nil(t, conn)
		require.Less(t, time.Since(start), 3*time.Second)
		recvWithin(t, closed, "server to observe the closed socket")
	})
}

func TestWebSocketTLSVerification(t *testing.T) {
	ca, caKey := newTestCA(t, "trusted test CA")
	trusted := startTLSServer(t, newTestLeaf(t, ca, caKey))
	otherCA, otherKey := newTestCA(t, "untrusted test CA")
	untrusted := startTLSServer(t, newTestLeaf(t, otherCA, otherKey))

	for _, mode := range []config.TransportType{config.WSS, config.WSSMUX} {
		t.Run(string(mode), func(t *testing.T) {
			// The logical address keeps its hostname for SNI/Host while the
			// edge IP override only changes where the TCP connection goes.
			t.Run("trusted CA keeps logical hostname as SNI and Host", func(t *testing.T) {
				requireLinuxTrustFixture(t)
				got := runTrustedChild(t, ca, "TLSH_MODE="+string(mode), "TLSH_HOST=localhost:"+trusted.port, "TLSH_EDGE=127.0.0.1")
				require.Equal(t, "RESULT:ok:", got)
				seen := recvWithin(t, trusted.reqCh, "upgrade request")
				require.Equal(t, wsRequestSeen{host: "localhost:" + trusted.port, sni: "localhost", alpn: "http/1.1"}, seen)
			})

			t.Run("hostname mismatch fails", func(t *testing.T) {
				requireLinuxTrustFixture(t)
				got := runTrustedChild(t, ca, "TLSH_MODE="+string(mode), "TLSH_HOST=wrong.example:"+trusted.port, "TLSH_EDGE=127.0.0.1")
				require.Contains(t, got, "RESULT:err:")
				require.Contains(t, got, "not wrong.example") // hostname error, not unknown-authority
			})

			t.Run("untrusted CA fails", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				conn, err := WebSocketDialer(ctx, "localhost:"+untrusted.port, "127.0.0.1", "/ws", 5*time.Second, 0, true, "test-token", "test-agent", mode, 1, 0, 0, 0, true)
				require.Error(t, err)
				require.Nil(t, conn)
				require.Contains(t, err.Error(), "x509")
			})

			// Explicit insecure override (tls_verify=false), generated identity only.
			t.Run("explicit opt-out accepts untrusted CA", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				conn, err := WebSocketDialer(ctx, "localhost:"+untrusted.port, "127.0.0.1", "/ws", 5*time.Second, 0, true, "test-token", "test-agent", mode, 1, 0, 0, 0, false)
				require.NoError(t, err)
				conn.Close()
				seen := recvWithin(t, untrusted.reqCh, "upgrade request")
				require.Equal(t, wsRequestSeen{host: "localhost:" + untrusted.port, sni: "localhost", alpn: "http/1.1"}, seen)
			})
		})
	}
}
