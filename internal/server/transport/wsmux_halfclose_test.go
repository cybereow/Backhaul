package transport

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/musix/backhaul/config"
	client_transport "github.com/musix/backhaul/internal/client/transport"
	"github.com/musix/backhaul/internal/utils"
	"github.com/musix/backhaul/internal/utils/network"
	"github.com/sirupsen/logrus"
	"github.com/xtaci/smux"
)

// Plan 024: mux_half_close, the halfclose-v1 capability, and the FlowPlainHC
// flow kind. The half-close envelope itself is tested in the handlers package.

func halfCloseServer(c *WsMuxConfig) { c.HalfClose = true }

// capStatus makes one upgrade request offering the given capability header value
// (empty = no header) and subprotocols, and returns the refusal error and body.
func capStatus(t *testing.T, addr, path, token, capValue string, protocols ...string) (error, string) {
	t.Helper()
	hdr := http.Header{"Authorization": {"Bearer " + token}}
	if capValue != "" {
		hdr.Set(network.CapHeader, capValue)
	}
	var body string
	d := ws.Dialer{
		Protocols: protocols,
		Timeout:   5 * time.Second,
		Header:    ws.HandshakeHeaderHTTP(hdr),
		OnStatusError: func(status int, reason []byte, resp io.Reader) {
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

// TestHalfCloseUpgradeGate is the compatibility matrix at the HTTP upgrade:
// old client (no header) against mux_half_close true and false, new client
// against both, and the independence of this check from plan 006's subprotocol.
func TestHalfCloseUpgradeGate(t *testing.T) {
	const fix = "mux_half_close=false"

	t.Run("old client is rejected by a mux_half_close server, control and tunnel", func(t *testing.T) {
		h := newLCHarness(t, halfCloseServer)
		for _, path := range []string{"/channel", "/tunnel/7"} {
			err, body := capStatus(t, h.addr, path, lcToken, "")
			if err == nil || !strings.Contains(err.Error(), "400") {
				t.Fatalf("%s: err = %v, want the HTTP 400 refusal", path, err)
			}
			if !strings.Contains(body, fix) {
				t.Fatalf("%s: body %q does not name the fix", path, body)
			}
		}
		if n := len(h.s.tunnelChannel) + h.sessions(); n != 0 {
			t.Fatalf("%d sessions from rejected upgrades", n)
		}
	})

	t.Run("new client is accepted by a mux_half_close server", func(t *testing.T) {
		h := newLCHarness(t, halfCloseServer)
		for _, path := range []string{"/channel", "/tunnel/7"} {
			if err, body := capStatus(t, h.addr, path, lcToken, network.CapHalfCloseV1); err != nil {
				t.Fatalf("%s: err = %v (%q), want an accepted upgrade", path, err, body)
			}
		}
	})

	t.Run("tokens are matched exactly, unknown ones ignored", func(t *testing.T) {
		h := newLCHarness(t, halfCloseServer)
		for _, v := range []string{"halfclose-v2", "halfclose", "halfclose-v1x", "xhalfclose-v1", "other"} {
			if err, body := capStatus(t, h.addr, "/tunnel/1", lcToken, v); err == nil || !strings.Contains(body, fix) {
				t.Fatalf("offering %q: err=%v body=%q, want the 400 refusal", v, err, body)
			}
		}
		for _, v := range []string{"future-cap, halfclose-v1", "halfclose-v1,future-cap", " halfclose-v1 "} {
			if err, body := capStatus(t, h.addr, "/tunnel/1", lcToken, v); err != nil {
				t.Fatalf("offering %q: err=%v body=%q, want an accepted upgrade", v, err, body)
			}
		}
	})

	t.Run("a server with mux_half_close=false ignores the offer and needs none", func(t *testing.T) {
		h := newLCHarness(t)
		for _, v := range []string{"", network.CapHalfCloseV1, "junk"} {
			for _, path := range []string{"/channel", "/tunnel/7"} {
				if err, body := capStatus(t, h.addr, path, lcToken, v); err != nil {
					t.Fatalf("%s offering %q: err=%v body=%q, want an accepted (legacy) upgrade", path, v, err, body)
				}
			}
		}
	})

	t.Run("authorization is checked before the capability", func(t *testing.T) {
		h := newLCHarness(t, halfCloseServer)
		for _, tc := range []struct{ path, token string }{{"/tunnel/1", "wrong"}, {"/", lcToken}, {"/elsewhere", "wrong"}} {
			err, body := capStatus(t, h.addr, tc.path, tc.token, "")
			if err == nil || !strings.Contains(err.Error(), "401") {
				t.Fatalf("%s with token %q: err=%v, want the unchanged 401", tc.path, tc.token, err)
			}
			if strings.Contains(body, "mux_half_close") {
				t.Fatalf("%s: the half-close hint leaked to an unauthorized request: %q", tc.path, body)
			}
		}
	})

	// The capability header and plan 006's subprotocol are separate checks with
	// separate errors; a client offers both when framing is on.
	t.Run("independent of the framing subprotocol", func(t *testing.T) {
		h := newLCHarness(t, framedServer, halfCloseServer)
		sub := network.MuxSubprotocol
		for _, path := range []string{"/channel", "/tunnel/7"} {
			// Only the subprotocol: the half-close check rejects, naming its own fix.
			err, body := capStatus(t, h.addr, path, lcToken, "", sub)
			if err == nil || !strings.Contains(err.Error(), "400") || !strings.Contains(body, fix) || strings.Contains(body, "mux_ws_framing") {
				t.Fatalf("%s subprotocol only: err=%v body=%q, want the half-close refusal only", path, err, body)
			}
			// Only the capability: the framing check rejects, naming its own fix.
			err, body = capStatus(t, h.addr, path, lcToken, network.CapHalfCloseV1)
			if err == nil || !strings.Contains(err.Error(), "400") || !strings.Contains(body, "mux_ws_framing=false") || strings.Contains(body, "mux_half_close") {
				t.Fatalf("%s capability only: err=%v body=%q, want the framing refusal only", path, err, body)
			}
			// Neither: framing is checked first.
			err, body = capStatus(t, h.addr, path, lcToken, "")
			if err == nil || !strings.Contains(body, "mux_ws_framing=false") {
				t.Fatalf("%s neither: err=%v body=%q, want the framing refusal", path, err, body)
			}
			// Both: accepted.
			if err, body := capStatus(t, h.addr, path, lcToken, network.CapHalfCloseV1, sub); err != nil {
				t.Fatalf("%s both: err=%v body=%q, want an accepted upgrade", path, err, body)
			}
		}
	})

	t.Run("the dialer options offer both tokens", func(t *testing.T) {
		h := newLCHarness(t, framedServer, halfCloseServer)
		h.dialWith("/channel", network.WithMuxFraming(), network.WithHalfCloseOffer())
		h.dialWith("/tunnel", network.WithMuxFraming(), network.WithHalfCloseOffer())
	})
}

// flowSeen is what a fake pool peer read off a newly opened flow stream.
type flowSeen struct {
	kind   byte
	flowID uint64
	addr   string
}

// kindPeer is a pool connection whose client end only records the header of each
// plain flow the server opens (RTT probes are echoed so the session stays
// healthy) and then holds the stream open.
func (h *lcHarness) kindPeer(opts ...network.DialOption) <-chan flowSeen {
	h.t.Helper()
	conn, err := h.dialWith("/tunnel", opts...)
	if err != nil {
		h.t.Fatalf("pool dial: %v", err)
	}
	sess, err := smux.Server(conn.NetConn(), lcSmuxConfig())
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { sess.Close() })
	seen := make(chan flowSeen, 16)
	go func() {
		for {
			st, err := sess.AcceptStream()
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
				case utils.FlowPlain, utils.FlowPlainHC:
					id, addr, err := utils.ReceiveFlowPlain(st)
					if err != nil {
						return
					}
					seen <- flowSeen{kind, id, addr}
					io.Copy(io.Discard, st)
				}
			}()
		}
	}()
	return seen
}

func nextFlow(t *testing.T, seen <-chan flowSeen) flowSeen {
	t.Helper()
	select {
	case f := <-seen:
		return f
	case <-time.After(lcDeadline):
		t.Fatal("no plain flow was opened on the pool session")
	}
	return flowSeen{}
}

// TestHalfCloseFlowKind: which flow kind the server opens, per configuration.
func TestHalfCloseFlowKind(t *testing.T) {
	run := func(t *testing.T, opts []func(*WsMuxConfig), dial ...network.DialOption) flowSeen {
		t.Helper()
		h := newLCHarness(t, opts...)
		if _, err := h.dialWith("/channel", dial...); err != nil {
			t.Fatalf("control dial: %v", err)
		}
		seen := h.kindPeer(dial...)
		lcWaitFor(t, "an admitted session", func() bool { return h.sessions() == 1 })
		h.user()
		return nextFlow(t, seen)
	}
	offer := network.WithHalfCloseOffer()

	t.Run("mux_half_close and a capable client: FlowPlainHC", func(t *testing.T) {
		f := run(t, []func(*WsMuxConfig){halfCloseServer}, offer)
		if f.kind != utils.FlowPlainHC || f.flowID != 0 || f.addr != "1" {
			t.Fatalf("flow = %+v, want FlowPlainHC with flowID 0 and the mapped address", f)
		}
	})

	t.Run("promote_bytes on a pure-plain deployment is not promotable: still FlowPlainHC", func(t *testing.T) {
		f := run(t, []func(*WsMuxConfig){halfCloseServer, func(c *WsMuxConfig) { c.PromoteBytes = 1 << 20 }}, offer)
		if f.kind != utils.FlowPlainHC || f.flowID != 0 {
			t.Fatalf("flow = %+v, want FlowPlainHC with flowID 0", f)
		}
	})

	t.Run("mux_half_close=false: the offer is ignored, FlowPlain", func(t *testing.T) {
		f := run(t, nil, offer)
		if f.kind != utils.FlowPlain || f.flowID != 0 {
			t.Fatalf("flow = %+v, want FlowPlain with flowID 0", f)
		}
	})

	t.Run("mux_half_close=false and an old client: FlowPlain", func(t *testing.T) {
		f := run(t, nil)
		if f.kind != utils.FlowPlain || f.flowID != 0 {
			t.Fatalf("flow = %+v, want FlowPlain with flowID 0", f)
		}
	})

	// A promotable flow (mux_version >= 2, promote_bytes > 0, a striped group
	// wider than one leg, this port not striped) keeps FlowPlain and its legacy
	// full-close semantics even on a half-close server.
	t.Run("promotable flows stay FlowPlain", func(t *testing.T) {
		f := run(t, []func(*WsMuxConfig){halfCloseServer, func(c *WsMuxConfig) {
			c.PromoteBytes = 1 << 20
			c.StripeFactor = 2
			c.StripePorts = []string{"9"} // per-port striping: the mapped port "1" stays plain
		}}, offer)
		if f.kind != utils.FlowPlain || f.flowID == 0 {
			t.Fatalf("flow = %+v, want FlowPlain with a non-zero (promotable) flowID", f)
		}
	})
}

// hcTarget is a real TCP target: it reads the request to EOF, reports that it
// saw the EOF, waits for release, then sends its reply and closes its write side.
type hcTarget struct {
	ln      net.Listener
	sawEOF  chan []byte
	release chan struct{}
	done    chan struct{}
}

func newHCTarget(t *testing.T, reply []byte) *hcTarget {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tg := &hcTarget{ln: ln, sawEOF: make(chan []byte, 1), release: make(chan struct{}), done: make(chan struct{})}
	t.Cleanup(func() { ln.Close() })
	go func() {
		defer close(tg.done)
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(30 * time.Second))
		req, _ := io.ReadAll(c)
		tg.sawEOF <- req
		<-tg.release
		c.Write(reply)
		c.(*net.TCPConn).CloseWrite()
		io.Copy(io.Discard, c)
	}()
	return tg
}

func hcPayload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*131 + i/251)
	}
	return b
}

// startHCClient runs the real wsmux client transport against the harness server.
func startHCClient(t *testing.T, h *lcHarness, framing bool) {
	t.Helper()
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	client := client_transport.NewWSMuxClient(ctx, &client_transport.WsMuxConfig{
		RemoteAddr: h.addr, Token: lcToken, Mode: config.WSMUX, Path: "/", MuxVersion: 2,
		ConnPoolSize: 1, DialTimeOut: 2 * time.Second, RetryInterval: 20 * time.Millisecond,
		KeepAlive: 30 * time.Second, MaxFrameSize: 32768, MaxReceiveBuffer: 4194304,
		MaxStreamBuffer: 65536, StripeFactor: 1, WSFraming: framing,
	}, logger)
	go client.Start()
	lcWaitFor(t, "the client's pool session to be admitted", func() bool { return h.sessions() >= 1 })
}

// toTarget points the harness's single port mapping at a real target.
func toTarget(addr string) func(*WsMuxConfig) {
	return func(c *WsMuxConfig) { c.Ports[0] = strings.Split(c.Ports[0], "=")[0] + "=" + addr }
}

// TestHalfCloseE2E is the plan's oracle end to end over the real wsmux client
// and server: a real TCP application sends a request and CloseWrites, the real
// target observes EOF and answers later, and the application must receive the
// exact reply (the legacy full-close loses it, see the handlers package's
// characterization). It runs with plan 006's framing on and off.
func TestHalfCloseE2E(t *testing.T) {
	for _, framing := range []bool{false, true} {
		t.Run(fmt.Sprintf("framing=%v", framing), func(t *testing.T) {
			req, reply := hcPayload(100000), hcPayload(70001)
			target := newHCTarget(t, reply)
			h := newLCHarness(t, halfCloseServer, func(c *WsMuxConfig) { c.WSFraming = framing }, toTarget(target.ln.Addr().String()))
			startHCClient(t, h, framing)

			app := h.user().(*net.TCPConn)
			_ = app.SetDeadline(time.Now().Add(20 * time.Second))
			if _, err := app.Write(req); err != nil {
				t.Fatalf("app write: %v", err)
			}
			if err := app.CloseWrite(); err != nil {
				t.Fatalf("app CloseWrite: %v", err)
			}
			select {
			case got := <-target.sawEOF:
				if !bytes.Equal(got, req) {
					t.Fatalf("target request = %d bytes, want the exact %d", len(got), len(req))
				}
			case <-time.After(lcDeadline * 2):
				t.Fatal("the target never observed EOF on the request")
			}
			close(target.release) // the reply is sent only after the target saw EOF
			got, err := io.ReadAll(app)
			if err != nil || !bytes.Equal(got, reply) {
				t.Fatalf("application got %d bytes, err %v; want the exact %d-byte reply", len(got), err, len(reply))
			}
			lcWaitClosed(t, "the target to finish", target.done)
		})
	}
}

// TestHalfCloseE2ELegacyUnchanged: with mux_half_close=false (default) a new
// client and the server still carry an ordinary request/response flow.
func TestHalfCloseE2ELegacyUnchanged(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				io.Copy(c, c)
			}()
		}
	}()
	for _, framing := range []bool{false, true} {
		t.Run(fmt.Sprintf("framing=%v", framing), func(t *testing.T) {
			h := newLCHarness(t, func(c *WsMuxConfig) { c.WSFraming = framing }, toTarget(ln.Addr().String()))
			startHCClient(t, h, framing)
			lcEcho(t, h.user(), "legacy plain flow")
			lcEcho(t, h.user(), "and a second flow")
		})
	}
}
