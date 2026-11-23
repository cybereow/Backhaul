package transport

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/musix/backhaul/internal/smux"
	"github.com/musix/backhaul/internal/utils"
)

func TestBindForwardPort(t *testing.T) {
	cases := []struct {
		name     string
		bind     string
		ports    []string
		fallback string
		want     int
		ok       bool
	}{
		{"empty ports", "0.0.0.0:443", nil, "", 443, true},
		{"empty slice", "127.0.0.1:8443", []string{}, "", 8443, true},
		{"ports listed", "0.0.0.0:443", []string{"80"}, "", 0, false},
		{"fallback set", "0.0.0.0:443", nil, "127.0.0.1:8080", 0, false},
		{"no port", "0.0.0.0", nil, "", 0, false},
		{"bad port", "0.0.0.0:abc", nil, "", 0, false},
		{"port zero", "0.0.0.0:0", nil, "", 0, false},
	}
	for _, c := range cases {
		got, ok := bindForwardPort(c.bind, c.ports, c.fallback)
		if got != c.want || ok != c.ok {
			t.Errorf("%s: got (%d, %v), want (%d, %v)", c.name, got, ok, c.want, c.ok)
		}
	}
}

// With no ports and no fallback, a plain HTTP request to the bind port must come
// out on the client as a flow addressed to that same port.
func TestBindForwardThroughTunnel(t *testing.T) {
	h := newLCHarness(t, func(c *WsMuxConfig) { c.Ports = nil })
	h.control()
	// A pool leg without the harness echo, so only this test's client answers.
	conn := h.dial("/tunnel")
	sess, err := smux.Server(conn.NetConn(), lcSmuxConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sess.Close() })
	p := &poolPeer{sess: sess}

	_, portStr, _ := net.SplitHostPort(h.addr)
	gotAddr := make(chan string, 1)

	// The client side: answer every flow with a canned HTTP response.
	go func() {
		for {
			st, err := p.sess.AcceptStream()
			if err != nil {
				return
			}
			go func() {
				defer st.Close()
				kind, err := utils.ReadFlowKind(st)
				if err != nil {
					return
				}
				switch kind {
				case utils.FlowPing:
					if nonce, err := utils.ReceiveFlowPing(st); err == nil {
						_ = utils.EchoFlowPing(st, nonce)
					}
				case utils.FlowPlain:
					_, remote, err := utils.ReceiveFlowPlain(st)
					if err != nil {
						return
					}
					gotAddr <- remote
					req, err := http.ReadRequest(bufio.NewReader(st))
					if err != nil {
						return
					}
					body := "hello " + req.URL.Path
					fmt.Fprintf(st, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", len(body), body)
				}
			}()
		}
	}()

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get("http://" + h.addr + "/some/path")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || string(b) != "hello /some/path" {
		t.Fatalf("got %d %q", resp.StatusCode, b)
	}
	select {
	case a := <-gotAddr:
		if a != portStr {
			t.Fatalf("client was asked for %q, want %q", a, portStr)
		}
	case <-time.After(time.Second):
		t.Fatal("client never received the flow")
	}
}

// An explicit fallback keeps winning: no tunnel flow is created.
func TestBindForwardDisabledByPorts(t *testing.T) {
	h := newLCHarness(t)
	resp, err := http.Get("http://" + h.addr + "/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("got %d, want 401", resp.StatusCode)
	}
}

// An upload must stream: the echo of the request head comes back before the body
// has been sent, and the body arrives byte for byte (Content-Length and chunked).
func TestBindForwardStreamsBodies(t *testing.T) {
	for _, chunked := range []bool{false, true} {
		h := newLCHarness(t, func(c *WsMuxConfig) { c.Ports = nil })
		h.control()
		h.pool() // the harness peer echoes every plain flow byte for byte

		const size = 2 << 20
		body := make([]byte, size)
		for i := range body {
			body[i] = byte(i * 7)
		}
		head := "POST /up HTTP/1.1\r\nHost: x\r\n"
		wire := body
		if chunked {
			head += "Transfer-Encoding: chunked\r\n\r\n"
			wire = append([]byte(fmt.Sprintf("%x\r\n", size)), body...)
			wire = append(wire, []byte("\r\n0\r\n\r\n")...)
		} else {
			head += fmt.Sprintf("Content-Length: %d\r\n\r\n", size)
		}

		c, err := net.Dial("tcp", h.addr)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(10 * time.Second))
		if _, err := c.Write([]byte(head)); err != nil {
			t.Fatal(err)
		}
		// Head only so far: it must already be echoed back through the tunnel.
		got := make([]byte, 10)
		if _, err := io.ReadFull(c, got); err != nil || string(got[:5]) != "POST " {
			t.Fatalf("chunked=%v: head not forwarded before the body: %q %v", chunked, got, err)
		}
		go c.Write(wire)
		var echoed []byte
		buf := make([]byte, 64<<10)
		for !bytes.Contains(echoed, body[size-4096:]) {
			n, err := c.Read(buf)
			echoed = append(echoed, buf[:n]...)
			if err != nil {
				break
			}
		}
		if !bytes.Contains(echoed, body[size-4096:]) {
			t.Fatalf("chunked=%v: body tail missing after %d echoed bytes", chunked, len(echoed))
		}
	}
}

// The user's socket must keep its half-close: after CloseWrite the peer sees EOF
// but can still be written to, exactly as on a mapped port.
func TestPrefixConnKeepsHalfClose(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	peer, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	srv, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	var c net.Conn = &prefixConn{Conn: srv, pre: bytes.NewBufferString("head")}
	cw, ok := c.(interface{ CloseWrite() error })
	if !ok {
		t.Fatal("prefixConn has no CloseWrite")
	}
	if err := cw.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	_ = peer.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := peer.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("peer should see EOF after CloseWrite, got %v", err)
	}
	if _, err := peer.Write([]byte("still open")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 4+10)
	if _, err := io.ReadFull(c, got); err != nil || string(got) != "headstill open" {
		t.Fatalf("read after CloseWrite: %q %v", got, err)
	}
}
