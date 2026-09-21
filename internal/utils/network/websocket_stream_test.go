package network

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/xtaci/smux"
)

const streamTestTimeout = 10 * time.Second

var streamRoles = []struct {
	name         string
	state        ws.State // the stream under test
	peerIsClient bool     // the peer of a server-side stream is a client, whose frames must be masked
}{
	{"server", ws.StateServerSide, true},
	{"client", ws.StateClientSide, false},
}

// streamPair returns a stream in the given role over one end of a loopback TCP
// connection, and the raw other end for the test to play the peer on.
func streamPair(t *testing.T, state ws.State) (*WebSocketStream, net.Conn) {
	t.Helper()
	a, b := tcpPair(t)
	t.Cleanup(func() { a.Close(); b.Close() })
	s := NewWebSocketConn(a, state, nil).Stream()
	_ = s.SetDeadline(time.Now().Add(streamTestTimeout))
	_ = b.SetDeadline(time.Now().Add(streamTestTimeout))
	return s, b
}

// frameBytes encodes one frame as the peer would put it on the wire.
func frameBytes(peerIsClient bool, op ws.OpCode, fin bool, payload []byte) []byte {
	f := ws.NewFrame(op, fin, append([]byte(nil), payload...))
	if peerIsClient {
		f = ws.MaskFrameInPlace(f)
	}
	var buf bytes.Buffer
	if err := ws.WriteFrame(&buf, f); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func peerWrite(t *testing.T, w io.Writer, peerIsClient bool, op ws.OpCode, fin bool, payload []byte) {
	t.Helper()
	if _, err := w.Write(frameBytes(peerIsClient, op, fin, payload)); err != nil {
		t.Fatalf("peer write: %v", err)
	}
}

func testPayload(n, seed int) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = byte(i*31 + seed*7 + i/251)
	}
	return p
}

// readN reads exactly n bytes from s using reads of at most bufSize.
func readN(t *testing.T, s io.Reader, n, bufSize int) []byte {
	t.Helper()
	out := make([]byte, 0, n)
	buf := make([]byte, bufSize)
	for len(out) < n {
		m, err := s.Read(buf[:min(bufSize, n-len(out))])
		out = append(out, buf[:m]...)
		if err != nil {
			t.Fatalf("read after %d of %d bytes: %v", len(out), n, err)
		}
	}
	return out
}

// checkStreamFrame asserts the masking direction of a frame the stream emitted.
func checkStreamFrame(t *testing.T, f ws.Frame, state ws.State) {
	t.Helper()
	if f.Header.Masked != state.ClientSide() {
		t.Fatalf("frame masked=%v from a %s-side stream", f.Header.Masked, map[bool]string{true: "client", false: "server"}[state.ClientSide()])
	}
}

func TestWebSocketStreamFragmentation(t *testing.T) {
	msg := testPayload(10000, 1)
	for _, role := range streamRoles {
		for _, bufSize := range []int{1, 7, 4096, 65536} {
			t.Run(fmt.Sprintf("%s/buf=%d", role.name, bufSize), func(t *testing.T) {
				s, peer := streamPair(t, role.state)
				// One message in three fragments with a Ping between the fragments,
				// then a second message.
				peerWrite(t, peer, role.peerIsClient, ws.OpBinary, false, msg[:3000])
				peerWrite(t, peer, role.peerIsClient, ws.OpPing, true, []byte("mid"))
				peerWrite(t, peer, role.peerIsClient, ws.OpContinuation, false, msg[3000:6000])
				peerWrite(t, peer, role.peerIsClient, ws.OpContinuation, true, msg[6000:])
				peerWrite(t, peer, role.peerIsClient, ws.OpBinary, true, []byte("tail"))

				got := readN(t, s, len(msg)+4, bufSize)
				if !bytes.Equal(got[:len(msg)], msg) || string(got[len(msg):]) != "tail" {
					t.Fatalf("reassembled stream differs from the sent one")
				}

				pong, err := readPeerFrame(peer)
				if err != nil {
					t.Fatalf("read pong: %v", err)
				}
				if pong.Header.OpCode != ws.OpPong || string(pong.Payload) != "mid" {
					t.Fatalf("got %v %q, want a Pong echoing the intermediate Ping", pong.Header.OpCode, pong.Payload)
				}
				checkStreamFrame(t, pong, role.state)
			})
		}
	}
}

func TestWebSocketStreamEmptyFrame(t *testing.T) {
	for _, role := range streamRoles {
		t.Run(role.name, func(t *testing.T) {
			s, peer := streamPair(t, role.state)
			peerWrite(t, peer, role.peerIsClient, ws.OpBinary, true, nil)
			peerWrite(t, peer, role.peerIsClient, ws.OpBinary, true, nil)
			peerWrite(t, peer, role.peerIsClient, ws.OpBinary, false, nil)
			peerWrite(t, peer, role.peerIsClient, ws.OpContinuation, true, nil)
			peerWrite(t, peer, role.peerIsClient, ws.OpBinary, true, []byte("abc"))

			// Empty messages are not the end of the stream: the first Read that
			// returns anything returns the data behind them.
			buf := make([]byte, 16)
			n, err := s.Read(buf)
			if err != nil || string(buf[:n]) != "abc" {
				t.Fatalf("Read = %q, %v; want \"abc\", nil", buf[:n], err)
			}
		})
	}
}

func TestWebSocketStreamMaskingClient(t *testing.T) {
	s, peer := streamPair(t, ws.StateClientSide)
	payload := testPayload(1000, 2)
	if n, err := s.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("Write = %d, %v", n, err)
	}
	// Read the raw frame: the mask bit is set and the payload on the wire is
	// masked, not the plaintext.
	raw, err := ws.ReadFrame(peer)
	if err != nil {
		t.Fatal(err)
	}
	if !raw.Header.Masked || raw.Header.OpCode != ws.OpBinary || !raw.Header.Fin {
		t.Fatalf("header %+v: want a final, masked binary frame", raw.Header)
	}
	if bytes.Equal(raw.Payload, payload) {
		t.Fatalf("payload on the wire is not masked")
	}
	ws.Cipher(raw.Payload, raw.Header.Mask, 0)
	if !bytes.Equal(raw.Payload, payload) {
		t.Fatalf("payload does not unmask to what was written")
	}
}

func TestWebSocketStreamMaskingServer(t *testing.T) {
	s, peer := streamPair(t, ws.StateServerSide)
	payload := testPayload(1000, 3)
	if _, err := s.Write(payload); err != nil {
		t.Fatal(err)
	}
	raw, err := ws.ReadFrame(peer)
	if err != nil {
		t.Fatal(err)
	}
	if raw.Header.Masked || raw.Header.OpCode != ws.OpBinary || !raw.Header.Fin {
		t.Fatalf("header %+v: want a final, unmasked binary frame", raw.Header)
	}
	if !bytes.Equal(raw.Payload, payload) {
		t.Fatalf("payload differs")
	}
}

func TestWebSocketStreamWrongMasking(t *testing.T) {
	// A server-side stream must refuse an unmasked frame, a client-side one a
	// masked frame (RFC 6455 5.1): the peer is not speaking WebSocket.
	for _, tc := range []struct {
		name  string
		state ws.State
		mask  bool
		want  error
	}{
		{"server gets unmasked", ws.StateServerSide, false, ws.ErrProtocolMaskRequired},
		{"client gets masked", ws.StateClientSide, true, ws.ErrProtocolMaskUnexpected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, peer := streamPair(t, tc.state)
			peerWrite(t, peer, tc.mask, ws.OpBinary, true, []byte("x"))
			if _, err := s.Read(make([]byte, 8)); !errors.Is(err, tc.want) {
				t.Fatalf("Read error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestWebSocketStreamPingEcho(t *testing.T) {
	for _, role := range streamRoles {
		t.Run(role.name, func(t *testing.T) {
			s, peer := streamPair(t, role.state)
			peerWrite(t, peer, role.peerIsClient, ws.OpPing, true, []byte("hello"))
			peerWrite(t, peer, role.peerIsClient, ws.OpPing, true, nil)
			peerWrite(t, peer, role.peerIsClient, ws.OpPong, true, []byte("unsolicited"))
			peerWrite(t, peer, role.peerIsClient, ws.OpBinary, true, []byte("data"))

			if got := readN(t, s, 4, 16); string(got) != "data" {
				t.Fatalf("data after the pings = %q", got)
			}
			for _, want := range []string{"hello", ""} {
				f, err := readPeerFrame(peer)
				if err != nil {
					t.Fatalf("read pong: %v", err)
				}
				if f.Header.OpCode != ws.OpPong || string(f.Payload) != want {
					t.Fatalf("got %v %q, want Pong %q", f.Header.OpCode, f.Payload, want)
				}
				checkStreamFrame(t, f, role.state)
			}
		})
	}
}

func TestWebSocketStreamClose(t *testing.T) {
	for _, role := range streamRoles {
		t.Run(role.name+"/received", func(t *testing.T) {
			s, peer := streamPair(t, role.state)
			peerWrite(t, peer, role.peerIsClient, ws.OpBinary, true, []byte("last"))
			peerWrite(t, peer, role.peerIsClient, ws.OpClose, true, ws.NewCloseFrameBody(ws.StatusNormalClosure, ""))

			if got := readN(t, s, 4, 16); string(got) != "last" {
				t.Fatalf("data before the close = %q", got)
			}
			if _, err := s.Read(make([]byte, 8)); err != io.EOF {
				t.Fatalf("Read after Close frame = %v, want io.EOF", err)
			}
			f, err := readPeerFrame(peer)
			if err != nil {
				t.Fatalf("read close echo: %v", err)
			}
			if f.Header.OpCode != ws.OpClose {
				t.Fatalf("reply is %v, want a Close echo", f.Header.OpCode)
			}
			if code, _ := ws.ParseCloseFrameData(f.Payload); code != ws.StatusNormalClosure {
				t.Fatalf("close code %d", code)
			}
			checkStreamFrame(t, f, role.state)
			// The end is sticky.
			if _, err := s.Read(make([]byte, 8)); err != io.EOF {
				t.Fatalf("second Read after Close frame = %v, want io.EOF", err)
			}
		})

		t.Run(role.name+"/sent by Close", func(t *testing.T) {
			s, peer := streamPair(t, role.state)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			f, err := readPeerFrame(peer)
			if err != nil {
				t.Fatalf("read close frame: %v", err)
			}
			if f.Header.OpCode != ws.OpClose {
				t.Fatalf("got %v, want a Close frame", f.Header.OpCode)
			}
			checkStreamFrame(t, f, role.state)
			if _, err := peer.Read(make([]byte, 1)); err != io.EOF {
				t.Fatalf("connection after Close: %v, want EOF", err)
			}
		})
	}
}

func TestWebSocketStreamTextFrameRejected(t *testing.T) {
	s, peer := streamPair(t, ws.StateServerSide)
	peerWrite(t, peer, true, ws.OpText, true, []byte("text"))
	for i := 0; i < 2; i++ { // the failure is sticky
		if _, err := s.Read(make([]byte, 8)); !errors.Is(err, ErrUnexpectedDataFrame) {
			t.Fatalf("Read %d = %v, want ErrUnexpectedDataFrame", i, err)
		}
	}
}

// TestWebSocketStreamBufferedUpgrade: bytes the HTTP layer had already buffered
// when the upgrade completed come out of the stream first - including a frame
// that is split between that buffer and the wire.
func TestWebSocketStreamBufferedUpgrade(t *testing.T) {
	first := frameBytes(true, ws.OpBinary, true, []byte("buffered-"))
	second := frameBytes(true, ws.OpBinary, true, []byte("split-frame"))
	for _, tc := range []struct {
		name          string
		buffered, tcp []byte
	}{
		{"whole frame buffered", first, second},
		{"frame split across buffer and wire", append(append([]byte(nil), first...), second[:5]...), second[5:]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, b := tcpPair(t)
			t.Cleanup(func() { a.Close(); b.Close() })
			br := bufio.NewReader(bytes.NewReader(tc.buffered))
			if _, err := br.Peek(1); err != nil { // pull the bytes into the bufio buffer
				t.Fatal(err)
			}
			s := NewWebSocketConn(a, ws.StateServerSide, br).Stream()
			_ = s.SetDeadline(time.Now().Add(streamTestTimeout))
			if _, err := b.Write(tc.tcp); err != nil {
				t.Fatal(err)
			}
			want := "buffered-split-frame"
			if got := readN(t, s, len(want), 3); string(got) != want {
				t.Fatalf("got %q, want %q", got, want)
			}
		})
	}
}

func TestWebSocketStreamLargeWrite(t *testing.T) {
	for _, role := range streamRoles {
		t.Run(role.name, func(t *testing.T) {
			s, peer := streamPair(t, role.state)
			payload := testPayload(300_000, 4)
			werr := make(chan error, 1)
			go func() {
				n, err := s.Write(payload)
				if err == nil && n != len(payload) {
					err = fmt.Errorf("short write: %d", n)
				}
				werr <- err
			}()

			// A write bigger than the frame buffer goes out as one message in
			// fragments; every one of them is a valid frame in the right direction.
			var got []byte
			for i := 0; ; i++ {
				f, err := readPeerFrame(peer)
				if err != nil {
					t.Fatalf("read frame %d: %v", i, err)
				}
				checkStreamFrame(t, f, role.state)
				wantOp := ws.OpContinuation
				if i == 0 {
					wantOp = ws.OpBinary
				}
				if f.Header.OpCode != wantOp {
					t.Fatalf("frame %d opcode %v, want %v", i, f.Header.OpCode, wantOp)
				}
				got = append(got, f.Payload...)
				if f.Header.Fin {
					break
				}
			}
			if !bytes.Equal(got, payload) {
				t.Fatalf("payload differs (%d bytes, want %d)", len(got), len(payload))
			}
			if err := <-werr; err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestWebSocketStreamRoundTrip runs a client-side and a server-side stream
// against each other, both directions at once, with writes of every awkward size.
func TestWebSocketStreamRoundTrip(t *testing.T) {
	a, b := tcpPair(t)
	t.Cleanup(func() { a.Close(); b.Close() })
	cli := NewWebSocketConn(a, ws.StateClientSide, nil).Stream()
	srv := NewWebSocketConn(b, ws.StateServerSide, nil).Stream()
	_ = cli.SetDeadline(time.Now().Add(streamTestTimeout))
	_ = srv.SetDeadline(time.Now().Add(streamTestTimeout))

	const total = 1 << 20
	up, down := testPayload(total, 5), testPayload(total, 6)
	send := func(w io.Writer, data []byte) error {
		for sent, i := 0, 1; sent < len(data); i++ {
			n := min((i*7919)%70001+1, len(data)-sent)
			if _, err := w.Write(data[sent : sent+n]); err != nil {
				return err
			}
			sent += n
		}
		return nil
	}

	var wg sync.WaitGroup
	errs := make(chan error, 4)
	got := map[string][]byte{}
	var mu sync.Mutex
	run := func(name string, f func() ([]byte, error)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := f()
			mu.Lock()
			got[name] = data
			mu.Unlock()
			if err != nil {
				errs <- fmt.Errorf("%s: %w", name, err)
			}
		}()
	}
	run("cli->srv send", func() ([]byte, error) { return nil, send(cli, up) })
	run("srv->cli send", func() ([]byte, error) { return nil, send(srv, down) })
	run("srv recv", func() ([]byte, error) {
		d := make([]byte, total)
		_, err := io.ReadFull(srv, d)
		return d, err
	})
	run("cli recv", func() ([]byte, error) {
		d := make([]byte, total)
		_, err := io.ReadFull(cli, d)
		return d, err
	})
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if !bytes.Equal(got["srv recv"], up) || !bytes.Equal(got["cli recv"], down) {
		t.Fatal("data differs after the round trip")
	}
}

func TestWebSocketStreamEOFOnNetClose(t *testing.T) {
	t.Run("between frames", func(t *testing.T) {
		s, peer := streamPair(t, ws.StateServerSide)
		peerWrite(t, peer, true, ws.OpBinary, true, []byte("x"))
		peer.Close()
		if got := readN(t, s, 1, 4); string(got) != "x" {
			t.Fatalf("got %q", got)
		}
		if _, err := s.Read(make([]byte, 4)); err != io.EOF {
			t.Fatalf("Read = %v, want io.EOF", err)
		}
	})
	t.Run("inside a frame", func(t *testing.T) {
		s, peer := streamPair(t, ws.StateServerSide)
		raw := frameBytes(true, ws.OpBinary, true, testPayload(100, 7))
		if _, err := peer.Write(raw[:50]); err != nil {
			t.Fatal(err)
		}
		peer.Close()
		buf := make([]byte, 200)
		var err error
		for err == nil {
			_, err = s.Read(buf)
		}
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			t.Fatalf("Read timed out instead of failing on the closed connection: %v", err)
		}
		if err == io.EOF {
			t.Fatalf("a connection cut mid-frame reported a clean EOF")
		}
	})
}

func TestWebSocketStreamConcurrentWriteControl(t *testing.T) {
	const (
		numPings  = 50
		numWrites = 300
	)
	s, peer := streamPair(t, ws.StateServerSide)

	// The stream's reader (smux's recvLoop stand-in) is what answers the Pings.
	go io.Copy(io.Discard, s)

	writeErr := make(chan error, 1)
	go func() {
		for i := 0; i < numWrites; i++ {
			if _, err := s.Write(testPayload(1+(i*37)%4000, i)); err != nil {
				writeErr <- err
				return
			}
		}
		writeErr <- nil
	}()

	go func() {
		for i := 0; i < numPings; i++ {
			if _, err := peer.Write(frameBytes(true, ws.OpPing, true, []byte(fmt.Sprintf("ping-%d", i)))); err != nil {
				return
			}
		}
	}()

	// Every frame the stream emits parses, in order, and none is torn by a
	// concurrent control reply.
	pongs, writes := 0, 0
	for pongs < numPings || writes < numWrites {
		f, err := readPeerFrame(peer)
		if err != nil {
			t.Fatalf("read frame (pongs=%d writes=%d): %v", pongs, writes, err)
		}
		checkStreamFrame(t, f, ws.StateServerSide)
		switch f.Header.OpCode {
		case ws.OpPong:
			if want := fmt.Sprintf("ping-%d", pongs); string(f.Payload) != want {
				t.Fatalf("pong %d = %q, want %q", pongs, f.Payload, want)
			}
			pongs++
		case ws.OpBinary:
			if !bytes.Equal(f.Payload, testPayload(1+(writes*37)%4000, writes)) {
				t.Fatalf("binary frame %d corrupted", writes)
			}
			writes++
		default:
			t.Fatalf("unexpected opcode %v", f.Header.OpCode)
		}
	}
	if err := <-writeErr; err != nil {
		t.Fatal(err)
	}
}

func TestWebSocketStreamDeadlines(t *testing.T) {
	isTimeout := func(err error) bool {
		var ne net.Error
		return errors.As(err, &ne) && ne.Timeout()
	}

	t.Run("read deadline", func(t *testing.T) {
		s, _ := streamPair(t, ws.StateServerSide)
		_ = s.SetReadDeadline(time.Now().Add(-time.Second))
		if _, err := s.Read(make([]byte, 4)); !isTimeout(err) {
			t.Fatalf("Read past its deadline = %v, want a timeout", err)
		}
	})
	t.Run("write deadline", func(t *testing.T) {
		s, _ := streamPair(t, ws.StateServerSide)
		_ = s.SetWriteDeadline(time.Now().Add(-time.Second))
		if _, err := s.Write([]byte("x")); !isTimeout(err) {
			t.Fatalf("Write past its deadline = %v, want a timeout", err)
		}
	})
	t.Run("close unblocks a read", func(t *testing.T) {
		s, _ := streamPair(t, ws.StateServerSide)
		_ = s.SetDeadline(time.Time{})
		done := make(chan error, 1)
		go func() {
			_, err := s.Read(make([]byte, 4))
			done <- err
		}()
		s.Close()
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("Read returned no error after Close")
			}
		case <-time.After(streamTestTimeout):
			t.Fatal("Read still blocked after Close")
		}
	})
	t.Run("close does not wait for a stuck writer", func(t *testing.T) {
		// The peer never reads, so the writer ends up blocked in the kernel with
		// the write lock held. Close must not queue behind it (smux closes the
		// stream from the very session whose sender is stuck), and must wake it.
		s, _ := streamPair(t, ws.StateServerSide)
		_ = s.SetDeadline(time.Time{})
		started := make(chan struct{})
		wdone := make(chan error, 1)
		go func() {
			close(started)
			_, err := s.Write(make([]byte, 64<<20))
			wdone <- err
		}()
		<-started
		closed := make(chan struct{})
		go func() { s.Close(); close(closed) }()
		select {
		case <-closed:
		case <-time.After(streamTestTimeout):
			t.Fatal("Close blocked behind a stuck writer")
		}
		select {
		case err := <-wdone:
			if err == nil {
				t.Fatal("the stuck Write returned no error after Close")
			}
		case <-time.After(streamTestTimeout):
			t.Fatal("stuck Write not woken by Close")
		}
	})
}

func TestOffersMuxSubprotocol(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values []string
		want   bool
	}{
		{"absent", nil, false},
		{"exact", []string{MuxSubprotocol}, true},
		{"in a list", []string{"chat, " + MuxSubprotocol + " ,other"}, true},
		{"second header line", []string{"chat", MuxSubprotocol}, true},
		{"other token", []string{"chat"}, false},
		{"other version", []string{"backhaul-mux-v2"}, false},
		{"prefix of the token", []string{"backhaul-mux"}, false},
		{"token with a suffix", []string{MuxSubprotocol + "x"}, false},
		{"different case", []string{"BACKHAUL-MUX-V1"}, false},
		{"garbled", []string{";;,,  ,\x00"}, false},
		{"empty", []string{""}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := http.Header{}
			for _, v := range tc.values {
				h.Add("Sec-WebSocket-Protocol", v)
			}
			if got := OffersMuxSubprotocol(h); got != tc.want {
				t.Fatalf("OffersMuxSubprotocol(%q) = %v, want %v", tc.values, got, tc.want)
			}
		})
	}
}

// benchLegPair is tcpPair for benchmarks (tcpPair takes a *testing.T).
func benchLegPair(b *testing.B) (net.Conn, net.Conn) {
	b.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer ln.Close()
	ch := make(chan net.Conn, 1)
	go func() {
		c, _ := ln.Accept()
		ch <- c
	}()
	a, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	return a, <-ch
}

// benchSmuxLeg moves bulk data over one smux session on a loopback tunnel leg,
// either as raw smux bytes (legacy) or as WebSocket binary messages (framed), so
// the difference is what the framing costs. Loopback has no RTT, so this is the
// worst case for the overhead: it is CPU-bound, not window-bound. download
// selects the writing side: false is server -> client (unmasked frames, the
// user's upload), true is client -> server (masked frames, the user's download).
func benchSmuxLeg(b *testing.B, framed, download bool) {
	const (
		streams   = 4
		perStream = 32 << 20
	)
	cfg := smux.DefaultConfig()
	cfg.Version = 2
	cfg.MaxFrameSize = 32768
	cfg.MaxReceiveBuffer = 4 << 20
	cfg.MaxStreamBuffer = 512 << 10
	if err := smux.VerifyConfig(cfg); err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(streams) * perStream)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		a, c := benchLegPair(b)
		var srvLeg, cliLeg io.ReadWriteCloser = a, c
		if framed {
			srvLeg = NewWebSocketConn(a, ws.StateServerSide, nil).Stream()
			cliLeg = NewWebSocketConn(c, ws.StateClientSide, nil).Stream()
		}
		srv, err := smux.Client(srvLeg, cfg) // the backhaul server opens the streams
		if err != nil {
			b.Fatal(err)
		}
		cli, err := smux.Server(cliLeg, cfg)
		if err != nil {
			b.Fatal(err)
		}
		opened := make([]*smux.Stream, streams)
		accepted := make([]*smux.Stream, streams)
		for j := range opened {
			if opened[j], err = srv.OpenStream(); err != nil {
				b.Fatal(err)
			}
			if accepted[j], err = cli.AcceptStream(); err != nil {
				b.Fatal(err)
			}
		}
		writers, readers := opened, accepted
		if download {
			writers, readers = accepted, opened
		}

		payload := make([]byte, 256<<10)
		var wg sync.WaitGroup
		b.StartTimer()
		for j := 0; j < streams; j++ {
			wg.Add(2)
			go func(st *smux.Stream) {
				defer wg.Done()
				for sent := 0; sent < perStream; {
					n, err := st.Write(payload[:min(len(payload), perStream-sent)])
					if err != nil {
						return
					}
					sent += n
				}
			}(writers[j])
			go func(st *smux.Stream) {
				defer wg.Done()
				_, _ = io.CopyN(io.Discard, st, perStream)
			}(readers[j])
		}
		wg.Wait()
		b.StopTimer()
		srv.Close()
		cli.Close()
		b.StartTimer()
	}
}

// BenchmarkSmuxLeg compares raw (legacy) and framed tunnel legs; see benchSmuxLeg.
func BenchmarkSmuxLeg(b *testing.B) {
	for _, dir := range []struct {
		name     string
		download bool
	}{{"upload_srv_to_cli", false}, {"download_cli_to_srv", true}} {
		for _, mode := range []struct {
			name   string
			framed bool
		}{{"legacy", false}, {"framed", true}} {
			b.Run(dir.name+"/"+mode.name, func(b *testing.B) { benchSmuxLeg(b, mode.framed, dir.download) })
		}
	}
}
