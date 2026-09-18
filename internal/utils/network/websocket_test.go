package network

import (
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

func (c *countingConn) Read(p []byte) (int, error)  { return 0, net.ErrClosed }
func (c *countingConn) Close() error                { return nil }
func (c *countingConn) LocalAddr() net.Addr         { return nil }
func (c *countingConn) RemoteAddr() net.Addr        { return nil }
func (c *countingConn) SetDeadline(time.Time) error { return nil }

func (c *countingConn) SetReadDeadline(time.Time) error  { return nil }
func (c *countingConn) SetWriteDeadline(time.Time) error { return nil }

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
