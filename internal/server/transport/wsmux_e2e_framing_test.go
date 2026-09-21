package transport

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/musix/backhaul/config"
	client_transport "github.com/musix/backhaul/internal/client/transport"
	"github.com/musix/backhaul/internal/utils/network"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/xtaci/smux"
)

func TestWsMuxPlainFramingE2E(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)

	conf := &WsMuxConfig{
		StripeFactor: 1, // Start as plain
		MuxVersion:   2,
		ChannelSize:  100,
		MuxCon:       10,
	}

	server := NewWSMuxServer(context.Background(), conf, logger)

	localConn := LocalTCPConn{
		remoteAddr:  "127.0.0.1:8080",
		timeCreated: time.Now().UnixMilli(),
	}

	assert.False(t, server.shouldStripe(localConn))
}

// ---------------------------------------------------------------------------
// Negotiation matrix: what every client/server pairing does on the wire.
// ---------------------------------------------------------------------------

func framedServer(c *WsMuxConfig) { c.WSFraming = true }

// dialWith is h.dial with dial options (h.dial always makes a legacy handshake).
func (h *lcHarness) dialWith(path string, opts ...network.DialOption) (*network.WebSocketConn, error) {
	h.t.Helper()
	conn, err := network.WebSocketDialer(context.Background(), h.addr, "", path, 2*time.Second, 30*time.Second, true, lcToken, "lifecycle-test", config.WSMUX, 5, 0, 0, 0, false, opts...)
	if err == nil {
		h.t.Cleanup(func() { conn.Close() })
	}
	return conn, err
}

func (h *lcHarness) framedDial(path string) *network.WebSocketConn {
	h.t.Helper()
	conn, err := h.dialWith(path, network.WithMuxFraming())
	if err != nil {
		h.t.Fatalf("framed dial %s: %v", path, err)
	}
	return conn
}

func (h *lcHarness) framedControl() *network.WebSocketConn { return h.framedDial("/channel") }

// framedPool is pool() for a framed leg: the client end runs smux over the
// WebSocket byte stream, as the real client does.
func (h *lcHarness) framedPool() *poolPeer {
	h.t.Helper()
	conn := h.framedDial("/tunnel")
	tc := &trackedConn{Conn: conn.Stream(), readClosed: make(chan struct{})}
	sess, err := smux.Server(tc, lcSmuxConfig())
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { sess.Close() })
	p := &poolPeer{sess: sess, readClosed: tc.readClosed}
	go p.serveEcho()
	return p
}

// upgradeStatus makes one upgrade request offering the given subprotocols and
// returns the HTTP status error and the response body of a refusal.
func upgradeStatus(t *testing.T, addr, path, token string, protocols ...string) (error, string) {
	t.Helper()
	var body string
	d := ws.Dialer{
		Protocols: protocols,
		Timeout:   5 * time.Second,
		Header:    ws.HandshakeHeaderHTTP(http.Header{"Authorization": {"Bearer " + token}}),
		OnStatusError: func(status int, reason []byte, resp io.Reader) {
			// resp is the raw response: status line, headers and body (the
			// connection stays open, so the body must be read by its length).
			if r, err := http.ReadResponse(bufio.NewReader(resp), nil); err == nil {
				b, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
				body = string(b)
			}
		},
	}
	conn, _, _, err := d.Dial(context.Background(), "ws://"+addr+path)
	if err == nil {
		conn.Close()
	}
	return err, body
}

func TestWSMuxFramingMatrix(t *testing.T) {
	t.Run("framed client and framed server", func(t *testing.T) {
		h := newLCHarness(t, framedServer)
		h.framedControl()
		h.framedPool()
		h.framedPool()
		lcWaitFor(t, "two admitted framed sessions", func() bool { return h.sessions() == 2 })
		user := h.user()
		lcEcho(t, user, "hello over framed legs")
		lcEcho(t, user, "and again")
	})

	t.Run("legacy client and legacy server still round-trip", func(t *testing.T) {
		h := newLCHarness(t)
		h.control()
		h.pool()
		lcWaitFor(t, "an admitted session", func() bool { return h.sessions() == 1 })
		lcEcho(t, h.user(), "hello over raw legs")
	})

	t.Run("framed client and legacy server fails loudly", func(t *testing.T) {
		h := newLCHarness(t)
		for _, path := range []string{"/channel", "/tunnel"} {
			conn, err := h.dialWith(path, network.WithMuxFraming())
			if conn != nil || !errors.Is(err, network.ErrMuxFramingNotNegotiated) {
				t.Fatalf("%s: dial = %v, %v; want ErrMuxFramingNotNegotiated", path, conn, err)
			}
		}
		// (That the client closes the socket it refused to use is checked at the
		// socket level in the network package: a legacy server admits the
		// upgrade as usual and smux does not notice a dead leg before its
		// keepalive, so the pool counters cannot show it.)
	})

	t.Run("legacy client and framed server is rejected with the fix in the message", func(t *testing.T) {
		h := newLCHarness(t, framedServer)
		for _, path := range []string{"/channel", "/tunnel/7"} {
			err, body := upgradeStatus(t, h.addr, path, lcToken)
			if err == nil {
				t.Fatalf("%s: a client that offers no subprotocol was upgraded by a framing server", path)
			}
			if !strings.Contains(err.Error(), "400") {
				t.Fatalf("%s: error %v, want the HTTP 400 refusal", path, err)
			}
			if !strings.Contains(body, "mux_ws_framing=false") {
				t.Fatalf("%s: body %q does not name the fix", path, body)
			}
		}
		// The rejection is at the HTTP layer: nothing reached the tunnel.
		if n := len(h.s.tunnelChannel) + h.sessions(); n != 0 {
			t.Fatalf("%d sessions from rejected upgrades", n)
		}
	})

	t.Run("unsupported or garbled tokens count as absent", func(t *testing.T) {
		h := newLCHarness(t, framedServer)
		for _, protos := range [][]string{{"backhaul-mux-v2"}, {"backhaul-mux"}, {"some-other", "protocol"}} {
			err, body := upgradeStatus(t, h.addr, "/tunnel/1", lcToken, protos...)
			if err == nil || !strings.Contains(err.Error(), "400") || !strings.Contains(body, "mux_ws_framing=false") {
				t.Fatalf("offering %v: err=%v body=%q, want the 400 refusal", protos, err, body)
			}
		}
	})

	t.Run("a legacy server never echoes the token", func(t *testing.T) {
		h := newLCHarness(t)
		// A legacy client that (wrongly) offers the token is served legacy raw.
		var got string
		d := ws.Dialer{
			Protocols: []string{network.MuxSubprotocol},
			Header:    ws.HandshakeHeaderHTTP(http.Header{"Authorization": {"Bearer " + lcToken}}),
		}
		conn, _, hs, err := d.Dial(context.Background(), "ws://"+h.addr+"/tunnel/1")
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		conn.Close()
		got = hs.Protocol
		if got != "" {
			t.Fatalf("a mux_ws_framing=false server selected %q", got)
		}
	})

	t.Run("authorization is checked before framing", func(t *testing.T) {
		h := newLCHarness(t, framedServer)
		// Wrong token and a non-tunnel path keep their existing 401, not the
		// framing refusal (which would tell a prober the option exists).
		for _, tc := range []struct{ path, token string }{{"/tunnel/1", "wrong"}, {"/", lcToken}, {"/elsewhere", "wrong"}} {
			err, body := upgradeStatus(t, h.addr, tc.path, tc.token)
			if err == nil || !strings.Contains(err.Error(), "401") {
				t.Fatalf("%s with token %q: err=%v, want the unchanged 401", tc.path, tc.token, err)
			}
			if strings.Contains(body, "mux_ws_framing") {
				t.Fatalf("%s: the framing hint leaked to an unauthorized request: %q", tc.path, body)
			}
		}
	})
}

// TestWSMuxFramingClientFailsLoudly: the real client transport in framed mode
// against a legacy server never settles into a broken raw stream and never
// hides the reason: every attempt logs the error that names the fix.
func TestWSMuxFramingClientFailsLoudly(t *testing.T) {
	h := newLCHarness(t) // legacy server: never echoes the subprotocol
	logger, hook := logrustest.NewNullLogger()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	client := client_transport.NewWSMuxClient(ctx, &client_transport.WsMuxConfig{
		RemoteAddr: h.addr, Token: lcToken, Mode: config.WSMUX, Path: "/", MuxVersion: 2,
		ConnPoolSize: 1, DialTimeOut: 2 * time.Second, RetryInterval: 20 * time.Millisecond,
		KeepAlive: 30 * time.Second, MaxFrameSize: 32768, MaxReceiveBuffer: 4194304,
		MaxStreamBuffer: 65536, StripeFactor: 1, WSFraming: true,
	}, logger)
	go client.Start()

	lcWaitFor(t, "three logged attempts, each naming the fix", func() bool {
		n := 0
		for _, e := range hook.AllEntries() {
			if e.Level == logrus.ErrorLevel && strings.Contains(e.Message, "mux_ws_framing=false") {
				n++
			}
		}
		return n >= 3
	})
}

// TestWSMuxFramingBufferedUpgrade: a client that pipelines the first bytes of its
// smux stream right behind the upgrade request. Those bytes sit in the HTTP
// server's read buffer when the connection is hijacked; the legacy path drops
// them, the framed path must not (or smux would desynchronise and the leg die).
func TestWSMuxFramingBufferedUpgrade(t *testing.T) {
	h := newLCHarness(t, framedServer)
	h.framedControl()

	conn, err := net.Dial("tcp", h.addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(lcDeadline))

	key := make([]byte, 16)
	rand.Read(key)
	req := fmt.Sprintf("GET /tunnel/9 HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: %s\r\nAuthorization: Bearer %s\r\n\r\n",
		h.addr, base64.StdEncoding.EncodeToString(key), network.MuxSubprotocol, lcToken)

	// An smux v2 NOP frame (ver, cmd, len, sid): its first three bytes ride in
	// the same TCP write as the request, the other five follow after the 101.
	nop := []byte{2, 3, 0, 0, 0, 0, 0, 0}
	first := frameOf(t, true, ws.OpBinary, nop[:3])
	if _, err := conn.Write(append([]byte(req), first...)); err != nil {
		t.Fatal(err)
	}

	br := bufio.NewReader(conn)
	status, err := br.ReadString('\n')
	if err != nil || !strings.Contains(status, "101") {
		t.Fatalf("handshake: %q, %v", status, err)
	}
	echoed := false
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line == "\r\n" {
			break
		}
		if strings.Contains(strings.ToLower(line), "sec-websocket-protocol: "+network.MuxSubprotocol) {
			echoed = true
		}
	}
	if !echoed {
		t.Fatal("the framing server did not echo the subprotocol")
	}

	st := network.NewWebSocketConn(conn, ws.StateClientSide, br).Stream()
	if _, err := st.Write(nop[3:]); err != nil {
		t.Fatal(err)
	}
	sess, err := smux.Server(st, lcSmuxConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sess.Close() })
	go (&poolPeer{sess: sess}).serveEcho()

	// The session is in sync, so it is admitted and carries a flow.
	lcWaitFor(t, "the session to be admitted", func() bool { return h.sessions() == 1 })
	lcEcho(t, h.user(), "still in sync")
}

func frameOf(t *testing.T, masked bool, op ws.OpCode, payload []byte) []byte {
	t.Helper()
	f := ws.NewFrame(op, true, append([]byte(nil), payload...))
	if masked {
		f = ws.MaskFrameInPlace(f)
	}
	var b bytes.Buffer
	if err := ws.WriteFrame(&b, f); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

// TestWSMuxFramingLifecycle: the generation still owns and closes a framed leg -
// queued, admitted, or carrying a flow - when it is stopped.
func TestWSMuxFramingLifecycle(t *testing.T) {
	t.Run("queued framed session", func(t *testing.T) {
		h := newLCHarness(t, framedServer)
		queued := h.framedPool() // no control channel yet: it waits in the tunnel channel
		lcWaitFor(t, "the session to be queued", func() bool { return len(h.s.tunnelChannel) == 1 })

		old := h.s.gen
		h.s.Restart()
		lcWaitClosed(t, "the queued framed session's socket to be closed", queued.readClosed)
		if !old.join(time.Second) {
			t.Fatal("Restart returned while old-generation workers were still running")
		}
	})

	t.Run("admitted framed sessions and a running flow", func(t *testing.T) {
		h := newLCHarness(t, framedServer)
		h.framedControl()
		peers := []*poolPeer{h.framedPool(), h.framedPool()}
		lcWaitFor(t, "two admitted sessions", func() bool { return h.sessions() == 2 })
		user := h.user()
		lcEcho(t, user, "hello")

		old := h.s.gen
		h.s.Restart()
		if !old.join(time.Second) {
			t.Fatal("Restart returned while old-generation workers were still running")
		}
		for i, p := range peers {
			lcWaitClosed(t, fmt.Sprintf("framed pool socket %d to be closed", i), p.readClosed)
		}
		lcExpectEOF(t, "the running flow's user connection", user)

		// The next generation works, framed, on the same ports.
		h.framedControl()
		h.framedPool()
		lcWaitFor(t, "a session in the new generation", func() bool { return h.sessions() == 1 })
		lcEcho(t, h.user(), "again")
	})
}

// ---------------------------------------------------------------------------
// End to end: real client and server transports with every outer frame on the
// wire parsed by gobwas.
// ---------------------------------------------------------------------------

// e2eTap sits between the client and the server as a TCP (for wss: TLS
// terminating) relay. It forwards every byte unchanged while parsing the
// WebSocket handshake and every frame after it with gobwas, so the test checks
// what is really on the wire rather than what the two ends agree on.
type e2eTap struct {
	mu    sync.Mutex
	conns []*tapConn
}

type tapConn struct {
	mu                     sync.Mutex
	done                   chan struct{} // closed once the relay for this connection has ended
	path                   string
	offered, echoed        bool
	c2sFrames, c2sMasked   int
	s2cFrames, s2cUnmask   int
	c2sPayload, s2cPayload int
	smuxFrames             int
	errs                   []string
}

func (c *tapConn) fail(format string, a ...any) {
	c.mu.Lock()
	c.errs = append(c.errs, fmt.Sprintf(format, a...))
	c.mu.Unlock()
}

func readHead(br *bufio.Reader) ([]byte, error) {
	var head []byte
	for {
		line, err := br.ReadBytes('\n')
		if err != nil {
			return head, err
		}
		head = append(head, line...)
		if len(line) == 2 && line[0] == '\r' {
			return head, nil
		}
	}
}

// smuxChecker validates that the payload bytes, concatenated across frames, are a
// well-formed smux v2 frame sequence.
type smuxChecker struct {
	hdr    [8]byte
	have   int
	skip   int
	frames int
}

func (s *smuxChecker) feed(p []byte) (int, error) {
	for len(p) > 0 {
		if s.skip > 0 {
			n := min(s.skip, len(p))
			s.skip -= n
			p = p[n:]
			continue
		}
		n := copy(s.hdr[s.have:], p)
		s.have += n
		p = p[n:]
		if s.have < len(s.hdr) {
			continue
		}
		s.have = 0
		if s.hdr[0] != 2 || s.hdr[1] > 4 {
			return s.frames, fmt.Errorf("not an smux v2 frame header: % x", s.hdr)
		}
		s.skip = int(binary.LittleEndian.Uint16(s.hdr[2:4]))
		s.frames++
	}
	return s.frames, nil
}

func (tap *e2eTap) handle(cc net.Conn, dialServer func() (net.Conn, error)) {
	defer cc.Close()
	sc, err := dialServer()
	if err != nil {
		return
	}
	defer sc.Close()

	tc := &tapConn{done: make(chan struct{})}
	cbr, sbr := bufio.NewReader(cc), bufio.NewReader(sc)
	reqHead, err := readHead(cbr)
	if err != nil {
		return
	}
	if fields := strings.Fields(string(reqHead)); len(fields) > 1 {
		tc.path = fields[1]
	}
	tc.offered = strings.Contains(strings.ToLower(string(reqHead)), "sec-websocket-protocol: "+network.MuxSubprotocol)
	if _, err := sc.Write(reqHead); err != nil {
		return
	}
	respHead, err := readHead(sbr)
	if err != nil {
		return
	}
	tc.echoed = strings.Contains(strings.ToLower(string(respHead)), "sec-websocket-protocol: "+network.MuxSubprotocol)
	if _, err := cc.Write(respHead); err != nil {
		return
	}
	tap.mu.Lock()
	tap.conns = append(tap.conns, tc)
	tap.mu.Unlock()
	defer close(tc.done)

	isTunnel := strings.HasPrefix(tc.path, "/tunnel")
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { // client -> server: frames must be masked
		defer wg.Done()
		defer func() { cc.Close(); sc.Close() }()
		tc.pump(cbr, sc, ws.StateServerSide, true, isTunnel)
	}()
	go func() { // server -> client: frames must not be masked
		defer wg.Done()
		defer func() { cc.Close(); sc.Close() }()
		tc.pump(sbr, cc, ws.StateClientSide, false, isTunnel)
	}()
	wg.Wait()
}

// pump forwards src to dst unchanged and checks every frame that passes.
func (tc *tapConn) pump(src io.Reader, dst io.Writer, state ws.State, fromClient, isTunnel bool) {
	tee := io.TeeReader(src, dst)
	var chk smuxChecker
	fragmented := false
	giveUp := func() { io.Copy(dst, src) } // keep forwarding, stop judging
	for {
		hdr, err := ws.ReadHeader(tee)
		if err != nil {
			return
		}
		st := state
		if fragmented {
			st = st.Set(ws.StateFragmented)
		}
		if err := ws.CheckHeader(hdr, st); err != nil {
			tc.fail("invalid frame header %+v: %v", hdr, err)
			giveUp()
			return
		}
		if hdr.Length > 1<<20 {
			tc.fail("frame of %d bytes", hdr.Length)
			giveUp()
			return
		}
		payload := make([]byte, hdr.Length)
		if _, err := io.ReadFull(tee, payload); err != nil {
			return
		}
		if hdr.Masked {
			ws.Cipher(payload, hdr.Mask, 0)
		}
		tc.mu.Lock()
		if fromClient {
			tc.c2sFrames++
			tc.c2sPayload += len(payload)
			if hdr.Masked {
				tc.c2sMasked++
			}
		} else {
			tc.s2cFrames++
			tc.s2cPayload += len(payload)
			if !hdr.Masked {
				tc.s2cUnmask++
			}
		}
		tc.mu.Unlock()

		switch hdr.OpCode {
		case ws.OpText:
			tc.fail("text frame on a mux connection")
		case ws.OpBinary, ws.OpContinuation:
			fragmented = !hdr.Fin
			if isTunnel {
				n, err := chk.feed(payload)
				if err != nil {
					tc.fail("payload is not smux: %v", err)
					giveUp()
					return
				}
				tc.mu.Lock()
				if n > tc.smuxFrames {
					tc.smuxFrames = n
				}
				tc.mu.Unlock()
			}
		}
	}
}

func (tap *e2eTap) snapshot() []*tapConn {
	tap.mu.Lock()
	defer tap.mu.Unlock()
	return append([]*tapConn(nil), tap.conns...)
}

// ---- local PKI: a CA the client verifies against, and a 127.0.0.1 leaf ----

var (
	pkiOnce sync.Once
	pkiDir  string
	pkiCA   *x509.CertPool
	pkiLeaf tls.Certificate
	pkiErr  error
)

// testPKI makes a CA and a leaf for 127.0.0.1 once per process and points
// SSL_CERT_FILE at the CA. The client's TLS stack (uTLS) verifies against the
// system roots and offers no way to inject a pool, and Go reads the system roots
// once per process - which is why the CA is created once and why this must be
// the first use of the system roots in the test binary. If it is not, the
// verified handshake fails loudly; verification is never turned off.
func testPKI(t *testing.T) (certFile, keyFile string, roots *x509.CertPool, leaf tls.Certificate) {
	t.Helper()
	pkiOnce.Do(func() {
		pkiDir, pkiErr = os.MkdirTemp("", "backhaul-framing-pki")
		if pkiErr != nil {
			return
		}
		caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		caTmpl := &x509.Certificate{
			SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "backhaul test CA"},
			NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
			IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		}
		caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
		if err != nil {
			pkiErr = err
			return
		}
		caCert, _ := x509.ParseCertificate(caDER)
		pkiCA = x509.NewCertPool()
		pkiCA.AddCert(caCert)

		leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		leafTmpl := &x509.Certificate{
			SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "127.0.0.1"},
			NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
			KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		}
		leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caCert, &leafKey.PublicKey, caKey)
		if err != nil {
			pkiErr = err
			return
		}
		keyDER, _ := x509.MarshalECPrivateKey(leafKey)
		certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER})
		keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
		caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
		for name, data := range map[string][]byte{"leaf.pem": certPEM, "leaf.key": keyPEM, "ca.pem": caPEM} {
			if pkiErr = os.WriteFile(filepath.Join(pkiDir, name), data, 0o600); pkiErr != nil {
				return
			}
		}
		pkiLeaf, pkiErr = tls.X509KeyPair(certPEM, keyPEM)
	})
	if pkiErr != nil {
		t.Fatalf("test PKI: %v", pkiErr)
	}
	t.Setenv("SSL_CERT_FILE", filepath.Join(pkiDir, "ca.pem"))
	return filepath.Join(pkiDir, "leaf.pem"), filepath.Join(pkiDir, "leaf.key"), pkiCA, pkiLeaf
}

// runFramingE2E brings up a real server and client transport with the tap between
// them, moves data both ways through a user connection and a backend echo, and
// returns what the tap saw. secure selects wssmux with a verified certificate.
func runFramingE2E(t *testing.T, secure, framing bool) ([]*tapConn, context.CancelFunc) {
	t.Helper()
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// Backend the client side forwards to: an echo.
	backend, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { backend.Close() })
	go func() {
		for {
			c, err := backend.Accept()
			if err != nil {
				return
			}
			go func() { defer c.Close(); io.Copy(c, c) }()
		}
	}()

	serverAddr, userAddr := lcFreeAddr(t), lcFreeAddr(t)
	srvCfg := &WsMuxConfig{
		BindAddr: serverAddr, Token: lcToken, Mode: config.WSMUX, Path: "/", MuxVersion: 2, MuxCon: 8,
		ChannelSize: 100, MaxFrameSize: 32768, MaxReceiveBuffer: 4194304, MaxStreamBuffer: 65536,
		KeepAlive: 30 * time.Second, Heartbeat: 30 * time.Second, Nodelay: true, StripeFactor: 1,
		Ports: []string{userAddr + "=" + backend.Addr().String()}, WSFraming: framing,
	}
	cliMode := config.WSMUX
	dialServer := func() (net.Conn, error) { return net.Dial("tcp", serverAddr) }
	wrapClient := func(c net.Conn) (net.Conn, error) { return c, nil }
	if secure {
		certFile, keyFile, roots, leaf := testPKI(t)
		srvCfg.Mode, srvCfg.TLSCertFile, srvCfg.TLSKeyFile = config.WSSMUX, certFile, keyFile
		cliMode = config.WSSMUX
		// The tap terminates the client's TLS with the leaf and re-dials the
		// server with full verification: no InsecureSkipVerify anywhere.
		dialServer = func() (net.Conn, error) {
			return tls.Dial("tcp", serverAddr, &tls.Config{RootCAs: roots, ServerName: "127.0.0.1"})
		}
		wrapClient = func(c net.Conn) (net.Conn, error) {
			tc := tls.Server(c, &tls.Config{Certificates: []tls.Certificate{leaf}, NextProtos: []string{"http/1.1"}})
			return tc, tc.Handshake()
		}
	}

	server := NewWSMuxServer(ctx, srvCfg, logger)
	server.Start()
	lcWaitFor(t, "the server to accept", func() bool {
		c, err := dialServer()
		if err != nil {
			return false
		}
		c.Close()
		return true
	})

	tap := &e2eTap{}
	tapLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tapLn.Close() })
	go func() {
		for {
			c, err := tapLn.Accept()
			if err != nil {
				return
			}
			go func() {
				wc, err := wrapClient(c)
				if err != nil {
					c.Close()
					return
				}
				tap.handle(wc, dialServer)
			}()
		}
	}()

	clientCtx, stopClient := context.WithCancel(ctx)
	client := client_transport.NewWSMuxClient(clientCtx, &client_transport.WsMuxConfig{
		RemoteAddr: tapLn.Addr().String(), Token: lcToken, Mode: cliMode, Path: "/", MuxVersion: 2,
		ConnPoolSize: 3, DialTimeOut: 5 * time.Second, RetryInterval: 100 * time.Millisecond,
		KeepAlive: 30 * time.Second, Nodelay: true, MaxFrameSize: 32768, MaxReceiveBuffer: 4194304,
		MaxStreamBuffer: 65536, StripeFactor: 1, TLSVerify: secure, WSFraming: framing,
	}, logger)
	go client.Start()

	// Once the control channel and the pool are up the user port carries flows.
	var user net.Conn
	lcWaitFor(t, "a flow through the tunnel", func() bool {
		c, err := net.DialTimeout("tcp", userAddr, time.Second)
		if err != nil {
			return false
		}
		_ = c.SetDeadline(time.Now().Add(2 * time.Second))
		if _, err := c.Write([]byte("probe")); err != nil {
			c.Close()
			return false
		}
		got := make([]byte, 5)
		if _, err := io.ReadFull(c, got); err != nil || string(got) != "probe" {
			c.Close()
			return false
		}
		user = c
		return true
	})
	t.Cleanup(func() { user.Close() })

	// 1 MiB each way through both smux legs and the backend echo.
	payload := make([]byte, 1<<20)
	rand.Read(payload)
	_ = user.SetDeadline(time.Now().Add(20 * time.Second))
	werr := make(chan error, 1)
	go func() { _, err := user.Write(payload); werr <- err }()
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(user, got); err != nil {
		t.Fatalf("read the echo: %v", err)
	}
	if err := <-werr; err != nil {
		t.Fatalf("write: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("echoed data differs from what was sent")
	}
	return tap.snapshot(), stopClient
}

func TestWSMuxFramingE2E(t *testing.T) {
	for _, secure := range []bool{false, true} {
		name := map[bool]string{false: "WS", true: "WSS"}[secure]
		t.Run(name, func(t *testing.T) {
			conns, stopClient := runFramingE2E(t, secure, true)

			var channels, tunnels, smuxFrames int
			for _, c := range conns {
				c.mu.Lock()
				desc := fmt.Sprintf("%s (offered=%v echoed=%v)", c.path, c.offered, c.echoed)
				if !c.offered || !c.echoed {
					t.Errorf("%s: the subprotocol was not negotiated", desc)
				}
				for _, e := range c.errs {
					t.Errorf("%s: %s", desc, e)
				}
				if c.c2sMasked != c.c2sFrames {
					t.Errorf("%s: %d of %d client->server frames masked", desc, c.c2sMasked, c.c2sFrames)
				}
				if c.s2cUnmask != c.s2cFrames {
					t.Errorf("%s: %d of %d server->client frames unmasked", desc, c.s2cUnmask, c.s2cFrames)
				}
				switch {
				case strings.HasPrefix(c.path, "/tunnel"):
					tunnels++
					smuxFrames += c.smuxFrames // an idle pool leg legitimately carries none
				case c.path == "/channel":
					channels++
				}
				c.mu.Unlock()
			}
			if smuxFrames == 0 {
				t.Errorf("no smux frame was reassembled from the WebSocket payloads of any tunnel leg")
			}
			if channels == 0 || tunnels == 0 {
				t.Fatalf("tap saw %d control and %d tunnel connections", channels, tunnels)
			}
			// The megabyte moved through some tunnel leg as WebSocket frames.
			var moved int
			for _, c := range conns {
				c.mu.Lock()
				if strings.HasPrefix(c.path, "/tunnel") {
					moved += c.c2sPayload + c.s2cPayload
				}
				c.mu.Unlock()
			}
			if moved < 2<<20 {
				t.Fatalf("only %d payload bytes seen in tunnel frames, want at least 2 MiB (1 MiB each way)", moved)
			}

			// Stopping the client must close every socket it holds, framed legs
			// and control channel alike: the tap sees each relay end.
			stopClient()
			for _, c := range conns {
				lcWaitClosed(t, "the client to close "+c.path, c.done)
			}
		})
	}

	// Negative control for the oracle: in legacy raw mode the very same tap must
	// flag the tunnel legs, because raw smux bytes are not WebSocket frames. If it
	// did not, the checks above would prove nothing.
	t.Run("oracle rejects legacy raw legs", func(t *testing.T) {
		conns, _ := runFramingE2E(t, false, false)
		flagged := 0
		for _, c := range conns {
			c.mu.Lock()
			if strings.HasPrefix(c.path, "/tunnel") {
				if c.offered || c.echoed {
					t.Errorf("legacy leg negotiated the subprotocol: %+v", c)
				}
				if len(c.errs) > 0 {
					flagged++
				}
			}
			c.mu.Unlock()
		}
		if flagged == 0 {
			t.Fatal("the frame oracle accepted raw smux bytes as WebSocket frames")
		}
	})
}
