package transport

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/musix/backhaul/internal/utils/network"
)

// A client may put its first tunnel bytes in the same TCP segment as the HTTP
// upgrade request. net/http reads the request through a bufio.Reader, so those
// trailing bytes are already off the socket and sitting in that buffer by the
// time ws.UpgradeHTTP returns - reachable only through the buffer, never again
// from the raw net.Conn.
//
// wsmux hands the upgraded connection to smux. If it hands over the RAW conn
// from ws.UpgradeHTTP, those buffered bytes are dropped and the mux session
// starts mid-frame. NewWebSocketConn is what folds the buffered bytes back in
// front of the socket, so the conn it wraps - the one NetConn returns - is the
// only correct thing to give smux.
//
// The "raw" subtest documents the defect; the "wrapped" subtest is the contract
// the production handoff relies on.
func TestUpgradeHandoffPreservesPipelinedBytes(t *testing.T) {
	const pipelined = "PIPELINED-FIRST-MUX-FRAME-BYTES-THAT-MUST-NOT-BE-DROPPED"

	for _, tc := range []struct {
		name string
		// useRaw mirrors the pre-fix handoff: give away the bare conn from
		// ws.UpgradeHTTP and lose whatever the upgrade already buffered.
		useRaw   bool
		wantSeen bool
	}{
		{name: "raw conn drops them", useRaw: true, wantSeen: false},
		{name: "wrapped conn keeps them", useRaw: false, wantSeen: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("listen: %v", err)
			}
			defer ln.Close()

			type result struct {
				buffered int
				got      string
				err      error
			}
			results := make(chan result, 1)

			srv := &http.Server{
				Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					netConn, brw, _, err := ws.UpgradeHTTP(r, w)
					if err != nil {
						results <- result{err: fmt.Errorf("upgrade: %w", err)}
						return
					}
					buffered := brw.Reader.Buffered()

					// Exactly the production shape: wrap, then choose which conn
					// to hand on to smux.
					conn := network.NewWebSocketConn(netConn, ws.StateServerSide, brw.Reader)
					handoff := conn.NetConn()
					if tc.useRaw {
						handoff = netConn
					}

					_ = handoff.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
					buf := make([]byte, len(pipelined))
					n, _ := io.ReadFull(handoff, buf)
					results <- result{buffered: buffered, got: string(buf[:n])}
					netConn.Close()
				}),
			}
			go srv.Serve(ln)
			defer srv.Close()

			raw, err := net.Dial("tcp", ln.Addr().String())
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			defer raw.Close()

			// The upgrade request and the first tunnel bytes in ONE write, so
			// they land in net/http's read buffer together.
			req := "GET /tunnel HTTP/1.1\r\n" +
				"Host: example.test\r\n" +
				"Upgrade: websocket\r\n" +
				"Connection: Upgrade\r\n" +
				"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" +
				"Sec-WebSocket-Version: 13\r\n\r\n" + pipelined
			if _, err := raw.Write([]byte(req)); err != nil {
				t.Fatalf("write handshake: %v", err)
			}

			select {
			case res := <-results:
				if res.err != nil {
					t.Fatalf("handler: %v", res.err)
				}
				if res.buffered == 0 {
					t.Skip("the upgrade did not buffer the pipelined bytes; " +
						"nothing to assert in this environment")
				}
				seen := res.got == pipelined
				if seen != tc.wantSeen {
					t.Errorf("pipelined bytes seen = %v, want %v (upgrade had %d bytes buffered, handoff read %q)",
						seen, tc.wantSeen, res.buffered, res.got)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("handler did not report")
			}
		})
	}
}
