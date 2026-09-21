package network

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/gobwas/ws"
)

// countingConn records the size of every Write that reaches the socket, so a
// test can assert how many syscalls one WriteMessage turns into.
type countingConn struct {
	net.Conn
	writes []int
}

func (c *countingConn) Write(p []byte) (int, error) {
	c.writes = append(c.writes, len(p))
	return len(p), nil
}

func (c *countingConn) Read(p []byte) (int, error)       { return 0, net.ErrClosed }
func (c *countingConn) Close() error                     { return nil }
func (c *countingConn) LocalAddr() net.Addr              { return nil }
func (c *countingConn) RemoteAddr() net.Addr             { return nil }
func (c *countingConn) SetDeadline(time.Time) error      { return nil }
func (c *countingConn) SetReadDeadline(time.Time) error  { return nil }
func (c *countingConn) SetWriteDeadline(time.Time) error { return nil }

// TestWebSocketPingPayload verifies that Pong responses echo the Ping payload
// for both client-side and server-side roles, covering empty, small, and
// max-valid (125-byte) payloads per RFC 6455 §5.5.
func TestWebSocketPingPayload(t *testing.T) {
	cases := []struct {
		name    string
		state   ws.State // SUT side
		peerSt  ws.State // peer side (opposite)
		payload []byte
	}{
		{"server-empty", ws.StateServerSide, ws.StateClientSide, nil},
		{"server-nonempty", ws.StateServerSide, ws.StateClientSide, []byte("hello")},
		{"server-max", ws.StateServerSide, ws.StateClientSide, bytes.Repeat([]byte{0xab}, 125)},
		{"client-empty", ws.StateClientSide, ws.StateServerSide, nil},
		{"client-nonempty", ws.StateClientSide, ws.StateServerSide, []byte("hello")},
		{"client-max", ws.StateClientSide, ws.StateServerSide, bytes.Repeat([]byte{0xcd}, 125)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Real loopback TCP, not net.Pipe: a zero-length Write (an empty
			// Pong payload) blocks forever on a pipe because no Read ever
			// consumes it.
			sutConn, peerConn := tcpPair(t)
			defer sutConn.Close()
			defer peerConn.Close()

			deadline := time.Now().Add(5 * time.Second)
			_ = sutConn.SetDeadline(deadline)
			_ = peerConn.SetDeadline(deadline)

			sut := NewWebSocketConn(sutConn, tc.state, nil)

			// Peer: send Ping then a binary message so NextReader has something to return.
			peerErrCh := make(chan error, 1)
			var gotPong ws.Frame
			go func() {
				// Send Ping.
				pingFrame := ws.NewPingFrame(tc.payload)
				if tc.peerSt.ClientSide() {
					pingFrame = ws.MaskFrame(pingFrame)
				}
				if err := ws.WriteFrame(peerConn, pingFrame); err != nil {
					peerErrCh <- err
					return
				}
				// Read the Pong reply.
				f, err := readPeerFrame(peerConn)
				if err != nil {
					peerErrCh <- err
					return
				}
				gotPong = f
				// Now send a binary message so NextReader returns and the goroutine can exit.
				binaryPayload := []byte("data")
				binFrame := ws.NewBinaryFrame(binaryPayload)
				if tc.peerSt.ClientSide() {
					binFrame = ws.MaskFrame(binFrame)
				}
				if err := ws.WriteFrame(peerConn, binFrame); err != nil {
					peerErrCh <- err
					return
				}
				peerErrCh <- nil
			}()

			// SUT: drive NextReader until we get the binary message.
			msgType, r, err := sut.NextReader()
			if err != nil {
				t.Fatalf("NextReader: %v", err)
			}
			if msgType != BinaryMessage {
				t.Fatalf("expected BinaryMessage, got %d", msgType)
			}
			// Drain so the connection is clean.
			if _, err := io.Copy(io.Discard, r); err != nil {
				t.Fatalf("draining data frame: %v", err)
			}

			if peerErr := <-peerErrCh; peerErr != nil {
				t.Fatalf("peer: %v", peerErr)
			}

			// Verify Pong opcode.
			if gotPong.Header.OpCode != ws.OpPong {
				t.Fatalf("got opcode %v, want Pong", gotPong.Header.OpCode)
			}
			// A client must mask its frames, a server must not (RFC 6455 §5.1).
			if gotPong.Header.Masked != tc.state.ClientSide() {
				t.Fatalf("pong masked = %v, want %v", gotPong.Header.Masked, tc.state.ClientSide())
			}
			// Verify Pong payload echoes Ping payload exactly.
			want := tc.payload
			if len(want) == 0 {
				want = nil
			}
			if !bytes.Equal(gotPong.Payload, want) {
				t.Fatalf("pong payload = %q, want %q", gotPong.Payload, want)
			}
		})
	}
}

// tcpPair returns the two ends of a loopback TCP connection.
func tcpPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	type accepted struct {
		c   net.Conn
		err error
	}
	ch := make(chan accepted, 1)
	go func() {
		c, err := ln.Accept()
		ch <- accepted{c, err}
	}()
	a, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	r := <-ch
	if r.err != nil {
		a.Close()
		t.Fatalf("accept: %v", r.err)
	}
	return a, r.c
}

// readPeerFrame reads one frame as the test peer and unmasks its payload, so
// callers compare application data. Header.Masked is left intact so the caller
// can assert the role's masking direction.
func readPeerFrame(r io.Reader) (ws.Frame, error) {
	f, err := ws.ReadFrame(r)
	if err != nil {
		return f, err
	}
	if f.Header.Masked {
		ws.Cipher(f.Payload, f.Header.Mask, 0)
	}
	return f, nil
}

// TestWebSocketPingConcurrentWrite drives Pings into NextReader while another
// goroutine writes binary messages through WriteMessage on the same conn. A
// single peer reader drains every frame the SUT emits (net.Pipe is synchronous,
// so an undrained SUT write would block), and checks that:
//   - every Pong echoes the exact Ping payload, in order;
//   - every binary frame is intact and in order (no interleaved/corrupt frames);
//   - masking matches the SUT's role.
func TestWebSocketPingConcurrentWrite(t *testing.T) {
	const (
		numPings  = 20
		numWrites = 200
		timeout   = 10 * time.Second
	)
	for _, tc := range []struct {
		name  string
		state ws.State
	}{
		{"server", ws.StateServerSide},
		{"client", ws.StateClientSide},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sutConn, peerConn := net.Pipe()
			defer sutConn.Close()
			defer peerConn.Close()
			deadline := time.Now().Add(timeout)
			_ = sutConn.SetDeadline(deadline)
			_ = peerConn.SetDeadline(deadline)

			sutMasks := tc.state.ClientSide() // frames the SUT sends are masked iff it is the client
			peerMasks := !sutMasks
			sut := NewWebSocketConn(sutConn, tc.state, nil)

			pingPayload := func(i int) []byte { return []byte(fmt.Sprintf("ping-%03d", i)) }
			msgPayload := func(i int) []byte { return []byte(fmt.Sprintf("msg-%04d", i)) }

			// SUT: NextReader loop, ends when it returns the peer's sentinel.
			type readResult struct {
				typ  int
				data []byte
				err  error
			}
			readCh := make(chan readResult, 1)
			go func() {
				typ, r, err := sut.NextReader()
				if err != nil {
					readCh <- readResult{err: err}
					return
				}
				data, err := io.ReadAll(r)
				readCh <- readResult{typ: typ, data: data, err: err}
			}()

			// SUT: concurrent application writes.
			writeCh := make(chan error, 1)
			go func() {
				for i := 0; i < numWrites; i++ {
					if err := sut.WriteMessage(BinaryMessage, msgPayload(i)); err != nil {
						writeCh <- fmt.Errorf("WriteMessage %d: %w", i, err)
						return
					}
				}
				writeCh <- nil
			}()

			// Peer sender: all Pings, without waiting for Pongs.
			sendCh := make(chan error, 1)
			go func() {
				for i := 0; i < numPings; i++ {
					f := ws.NewPingFrame(pingPayload(i))
					if peerMasks {
						f = ws.MaskFrame(f)
					}
					if err := ws.WriteFrame(peerConn, f); err != nil {
						sendCh <- fmt.Errorf("send ping %d: %w", i, err)
						return
					}
				}
				sendCh <- nil
			}()

			// Peer reader: the only reader of peerConn; drains and classifies
			// everything until it has seen all Pongs and all binary frames.
			recvCh := make(chan error, 1)
			go func() {
				pongs, msgs := 0, 0
				for pongs < numPings || msgs < numWrites {
					f, err := readPeerFrame(peerConn)
					if err != nil {
						recvCh <- fmt.Errorf("after %d pongs, %d msgs: %w", pongs, msgs, err)
						return
					}
					if f.Header.Masked != sutMasks {
						recvCh <- fmt.Errorf("frame %v masked=%v, want %v", f.Header.OpCode, f.Header.Masked, sutMasks)
						return
					}
					if !f.Header.Fin {
						recvCh <- fmt.Errorf("frame %v not final (fragmented)", f.Header.OpCode)
						return
					}
					switch f.Header.OpCode {
					case ws.OpPong:
						if want := pingPayload(pongs); !bytes.Equal(f.Payload, want) {
							recvCh <- fmt.Errorf("pong %d = %q, want %q", pongs, f.Payload, want)
							return
						}
						pongs++
					case ws.OpBinary:
						if want := msgPayload(msgs); !bytes.Equal(f.Payload, want) {
							recvCh <- fmt.Errorf("binary frame %d = %q, want %q", msgs, f.Payload, want)
							return
						}
						msgs++
					default:
						recvCh <- fmt.Errorf("unexpected opcode %v", f.Header.OpCode)
						return
					}
				}
				recvCh <- nil
			}()

			wait := func(what string, ch <-chan error) {
				t.Helper()
				select {
				case err := <-ch:
					if err != nil {
						t.Fatalf("%s: %v", what, err)
					}
				case <-time.After(timeout):
					t.Fatalf("%s: timed out", what)
				}
			}
			wait("peer sender", sendCh)
			wait("peer reader", recvCh)
			wait("sut writer", writeCh)

			// All Pings answered and all writes seen; now release NextReader.
			sentinel := ws.NewBinaryFrame([]byte("end"))
			if peerMasks {
				sentinel = ws.MaskFrame(sentinel)
			}
			if err := ws.WriteFrame(peerConn, sentinel); err != nil {
				t.Fatalf("send sentinel: %v", err)
			}
			select {
			case res := <-readCh:
				if res.err != nil {
					t.Fatalf("NextReader: %v", res.err)
				}
				if res.typ != BinaryMessage || string(res.data) != "end" {
					t.Fatalf("NextReader got type=%d data=%q, want binary \"end\"", res.typ, res.data)
				}
			case <-time.After(timeout):
				t.Fatal("NextReader: timed out waiting for sentinel")
			}
		})
	}
}

// A full-sized payload must leave as one frame in one socket write.
//
// The writer used to run with wsutil's 4KB default buffer while the data path
// handed it up to handlers.copyBufferSize (64KB) at a time. Anything larger
// than the buffer took gobwas's WriteThrough path, which cost three socket
// writes and two frames per message: a tiny header write, the payload as a
// non-final fragment, then an empty continuation frame carrying only fin.
// Under TCP_NODELAY those became three TCP segments, two of them a few bytes,
// and on wss three separate TLS records.
//
// This is also what pins the writer's buffer to copyBufferSize: grow the copy
// buffer past it and the fragmented path returns silently, so this test fails
// rather than letting the regression through.
func TestWriteMessageSingleWrite(t *testing.T) {
	const copyBufferSize = 64 * 1024 // handlers.copyBufferSize

	for _, tc := range []struct {
		name  string
		state ws.State
	}{
		{"server", ws.StateServerSide},
		{"client", ws.StateClientSide},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, size := range []int{1, 1024, 4096, copyBufferSize} {
				cc := &countingConn{}
				conn := NewWebSocketConn(cc, tc.state, nil)

				if err := conn.WriteMessage(BinaryMessage, make([]byte, size)); err != nil {
					t.Fatalf("payload %d: WriteMessage: %v", size, err)
				}

				if len(cc.writes) != 1 {
					t.Errorf("payload %d: got %d socket writes %v, want 1 - "+
						"the writer fell back to the fragmented WriteThrough path",
						size, len(cc.writes), cc.writes)
				}
			}
		})
	}
}

// replayConn serves a fixed byte stream and counts Read calls, so a test can
// assert how many socket reads the framing layer needs.
type replayConn struct {
	net.Conn
	data  []byte
	off   int
	reads int
}

func (c *replayConn) Read(p []byte) (int, error) {
	c.reads++
	if c.off >= len(c.data) {
		return 0, io.EOF
	}
	n := copy(p, c.data[c.off:])
	c.off += n
	return n, nil
}

func (c *replayConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *replayConn) Close() error                     { return nil }
func (c *replayConn) LocalAddr() net.Addr              { return nil }
func (c *replayConn) RemoteAddr() net.Addr             { return nil }
func (c *replayConn) SetDeadline(time.Time) error      { return nil }
func (c *replayConn) SetReadDeadline(time.Time) error  { return nil }
func (c *replayConn) SetWriteDeadline(time.Time) error { return nil }

// encodeFrame builds one unmasked final binary frame carrying payload.
func encodeFrame(t *testing.T, payload []byte) []byte {
	t.Helper()
	var buf sliceWriter
	h := ws.Header{Fin: true, OpCode: ws.OpBinary, Length: int64(len(payload))}
	if err := ws.WriteHeader(&buf, h); err != nil {
		t.Fatalf("WriteHeader: %v", err)
	}
	return append([]byte(buf), payload...)
}

// The read side must be buffered.
//
// Every frame opens with a 2-14 byte header that gobwas reads with two separate
// io.ReadFull calls, so an unbuffered socket spends two syscalls per message
// before a single payload byte moves - three socket reads per message in total.
// On a tunnel carrying interactive traffic that is two thirds of all read
// syscalls doing nothing but collecting four bytes of header.
func TestReadIsBuffered(t *testing.T) {
	const messages = 32
	payload := make([]byte, 256)

	var stream []byte
	for i := 0; i < messages; i++ {
		stream = append(stream, encodeFrame(t, payload)...)
	}

	rc := &replayConn{data: stream}
	conn := NewWebSocketConn(rc, ws.StateClientSide, nil)
	sink := make([]byte, 64*1024)

	for i := 0; i < messages; i++ {
		_, r, err := conn.NextReader()
		if err != nil {
			t.Fatalf("message %d: NextReader: %v", i, err)
		}
		n, err := io.Copy(io.Discard, io.LimitReader(r, int64(len(sink))))
		if err != nil {
			t.Fatalf("message %d: drain: %v", i, err)
		}
		if n != int64(len(payload)) {
			t.Fatalf("message %d: drained %d bytes, want %d", i, n, len(payload))
		}
	}

	// Unbuffered this is 3 reads per message. Buffered, the whole 32-message
	// stream fits in a couple of fills.
	if rc.reads >= messages {
		t.Errorf("got %d socket reads for %d messages; the read side is not buffered "+
			"(unbuffered costs ~3 reads per message, two of them for the header alone)",
			rc.reads, messages)
	}
}

// ReadMessage and NextReader must draw from the SAME buffered source.
//
// This is the invariant that makes read buffering safe. client/transport/ws.go
// reads the remote address off a tunnel leg with ReadMessage and then hands the
// same conn to WSConnectionHandler, which reads it with NextReader. If those two
// paths had separate buffers, whatever the first one read ahead into its buffer
// would be invisible to the second, silently truncating the stream.
func TestReadMessageAndNextReaderShareBuffer(t *testing.T) {
	first := []byte("remote-address-message")
	second := make([]byte, 4096)
	for i := range second {
		second[i] = byte(i)
	}

	stream := append(encodeFrame(t, first), encodeFrame(t, second)...)

	rc := &replayConn{data: stream}
	conn := NewWebSocketConn(rc, ws.StateClientSide, nil)

	_, got, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if string(got) != string(first) {
		t.Fatalf("ReadMessage returned %q, want %q", got, first)
	}

	_, r, err := conn.NextReader()
	if err != nil {
		t.Fatalf("NextReader after ReadMessage: %v", err)
	}
	rest, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("drain second message: %v", err)
	}
	if len(rest) != len(second) {
		t.Fatalf("second message is %d bytes, want %d - bytes were stranded in a "+
			"buffer the other read path could not see", len(rest), len(second))
	}
	for i := range rest {
		if rest[i] != second[i] {
			t.Fatalf("second message corrupted at byte %d", i)
		}
	}
}
