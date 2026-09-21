package network

import (
	"bufio"
	"bytes"
	"sync"

	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

var ErrCloseSent = errors.New("close sent")

const (
	TextMessage   = int(ws.OpText)
	BinaryMessage = int(ws.OpBinary)
)

type bufferedConn struct {
	net.Conn
	r io.Reader
}

func (b *bufferedConn) Read(p []byte) (int, error) {
	return b.r.Read(p)
}

type WebSocketConn struct {
	net.Conn
	state   ws.State
	reader  *wsutil.Reader
	writer  *wsutil.Writer
	writeMu sync.Mutex
}

// wsReadBufferSize is the size of the read buffer placed in front of the socket.
//
// Every WebSocket frame opens with a 2-14 byte header, and gobwas reads it with
// two separate io.ReadFull calls (first two bytes, then the extended length and
// mask). On an unbuffered socket that is two syscalls per message before a
// single payload byte moves - measured at three socket reads per message, two
// of them for the header alone. Buffering collapses those into one read, and
// small messages arrive whole in that same read.
//
// Deliberately kept well below handlers.copyBufferSize (64KB): bufio.Reader
// reads straight into the caller's buffer whenever its own buffer is empty and
// the request is at least as large, so a full-size payload read still lands
// directly in the copy buffer. Sizing this at or above the copy buffer would
// instead put an extra 64KB memcpy on every large message.
const wsReadBufferSize = 4 * 1024

func NewWebSocketConn(conn net.Conn, state ws.State, br *bufio.Reader) *WebSocketConn {
	var src io.Reader = conn
	if br != nil && br.Buffered() > 0 {
		peek, _ := br.Peek(br.Buffered())
		// create a multi reader to consume the buffered bytes then raw conn
		// we use peek to avoid taking ownership of the bufio reader's internal lock,
		// but since we only need the buffered bytes:
		src = io.MultiReader(bytes.NewReader(peek), conn)
	}

	// Every read path on this conn - NextReader, ReadMessage, and the raw conn
	// handed out by NetConn - goes through this one buffered source. Sharing it
	// is what makes the buffer safe: bytes pulled off the socket by one path
	// are visible to all of them, so none can be stranded in a buffer another
	// path cannot see.
	wrap := &bufferedConn{
		Conn: conn,
		r:    bufio.NewReaderSize(src, wsReadBufferSize),
	}

	r := wsutil.NewReader(wrap, state)

	wsConn := &WebSocketConn{
		Conn:  wrap,
		state: state,
		// Sized to match handlers.copyBufferSize, the largest payload
		// WriteMessage is ever handed. wsutil.NewWriter's 4KB default was
		// smaller than that, so every full-sized write took gobwas's
		// WriteThrough path: a separate socket write for the header, one for
		// the payload as a non-final fragment, and a third from Flush for an
		// empty continuation frame carrying only fin. That is three writes and
		// two frames per message - three TCP segments under TCP_NODELAY, two of
		// them a handful of bytes, and three separate TLS records on wss.
		// Giving the writer room for a whole buffer keeps it on the copy path,
		// where flushFragment emits header and payload in a single write.
		//
		// Keep this at or above copyBufferSize: if the copy buffer grows past
		// it, the fragmented path silently comes back.
		writer: wsutil.NewWriterSize(wrap, state, ws.OpBinary, 64*1024),
	}
	r.OnIntermediate = func(hdr ws.Header, src io.Reader) error {
		// Drain intermediate frames (e.g. fragments of control frames)
		payload, err := io.ReadAll(src)
		if err != nil {
			return err
		}
		if hdr.OpCode == ws.OpPing {
			wsConn.writeMu.Lock()
			defer wsConn.writeMu.Unlock()
			return wsutil.WriteMessage(wrap, state, ws.OpPong, payload)
		}
		return nil
	}
	wsConn.reader = r
	return wsConn

}

func (c *WebSocketConn) ReadMessage() (int, []byte, error) {
	b, op, err := wsutil.ReadData(c.Conn, c.state)
	return int(op), b, err
}

func (c *WebSocketConn) WriteMessage(messageType int, data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if messageType == BinaryMessage || messageType == TextMessage {
		c.writer.ResetOp(ws.OpCode(messageType))
		_, err := c.writer.Write(data)
		if err != nil {
			return err
		}
		return c.writer.Flush()
	}
	return wsutil.WriteMessage(c.Conn, c.state, ws.OpCode(messageType), data)
}

func (c *WebSocketConn) NextReader() (int, io.Reader, error) {
	for {
		hdr, err := c.reader.NextFrame()
		if err != nil {
			return 0, nil, err
		}
		if hdr.OpCode == ws.OpClose {
			return int(hdr.OpCode), nil, io.EOF
		}
		if hdr.OpCode == ws.OpPing {
			// RFC 6455 §5.5.3: Pong MUST echo the application data from the Ping.
			// Read payload before acquiring the write lock (control frames ≤ 125 B).
			payload, err := io.ReadAll(c.reader)
			if err != nil {
				return 0, nil, err
			}
			c.writeMu.Lock()
			err = wsutil.WriteMessage(c.Conn, c.state, ws.OpPong, payload)
			c.writeMu.Unlock()
			if err != nil {
				return 0, nil, err
			}
			continue
		}
		if hdr.OpCode == ws.OpPong {
			// Drain the pong payload and continue
			_, err = io.Copy(io.Discard, c.reader)
			if err != nil {
				return 0, nil, err
			}
			continue
		}
		// It's OpBinary or OpText, return it to the caller
		return int(hdr.OpCode), c.reader, nil
	}
}

func (c *WebSocketConn) NetConn() net.Conn {
	return c.Conn
}

// MuxSubprotocol is the Sec-WebSocket-Protocol token that negotiates standards
// framed mux legs (see WebSocketStream). It is an HTTP-handshake header only:
// control-channel frames and smux's MuxVersion are unaffected.
const MuxSubprotocol = "backhaul-mux-v1"

// OffersMuxSubprotocol reports whether an upgrade request offers MuxSubprotocol.
// Anything else in the header - another token, a garbled value - counts as
// absent; there is no sniffing of post-upgrade bytes.
func OffersMuxSubprotocol(h http.Header) bool {
	for _, v := range h.Values("Sec-WebSocket-Protocol") {
		for _, tok := range strings.Split(v, ",") {
			if strings.TrimSpace(tok) == MuxSubprotocol {
				return true
			}
		}
	}
	return false
}

// ErrUnexpectedDataFrame is returned by WebSocketStream.Read for a text data
// frame: a mux leg carries binary messages only.
var ErrUnexpectedDataFrame = errors.New("websocket mux stream: unexpected non-binary data frame")

// streamCloseTimeout bounds the best-effort Close frame WebSocketStream sends.
const streamCloseTimeout = time.Second

// WebSocketStream presents a WebSocketConn as a plain byte stream (net.Conn) whose
// wire form is RFC 6455 binary messages, so smux can run on it while every byte
// on the wire is a valid WebSocket frame (which is what a CDN that reassembles
// messages needs). Each Write is one binary message; Read concatenates message
// payloads across whatever buffer sizes and frame boundaries the caller uses,
// never buffering a whole message. Bytes buffered at upgrade time are preserved
// by NewWebSocketConn, which the stream is built on.
//
// Read is not goroutine safe (smux has a single reader); Write, Close and the
// deadline setters are.
type WebSocketStream struct {
	c         *WebSocketConn
	inMsg     bool  // a data message is being read out of c.reader
	rerr      error // sticky read error
	closeOnce sync.Once
}

// streamReadBuffer is the read-ahead of a stream. Unbuffered, every frame costs
// separate socket reads for its 2-14 header bytes on top of the payload reads
// smux already makes for its own 8-byte headers, which measurably slows bulk
// transfer; buffering brings the syscall count back to the raw-leg level.
const streamReadBuffer = 32 * 1024

// Stream returns the byte-stream view of c. c must not be used for
// ReadMessage/NextReader afterwards: the stream reads ahead of the frames it
// hands out.
func (c *WebSocketConn) Stream() *WebSocketStream {
	// c.reader's source already yields any bytes buffered at upgrade time first.
	c.reader.Source = bufio.NewReaderSize(c.reader.Source, streamReadBuffer)
	return &WebSocketStream{c: c}
}

func (s *WebSocketStream) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if s.rerr != nil {
		return 0, s.rerr
	}
	for {
		if !s.inMsg {
			// NextReader answers Ping (echoing its payload), drops Pong and
			// reports Close as io.EOF; only data frames come back.
			op, _, err := s.c.NextReader()
			if err != nil {
				if op == int(ws.OpClose) {
					s.sendClose()
				}
				s.rerr = err
				return 0, err
			}
			if op != BinaryMessage {
				s.rerr = ErrUnexpectedDataFrame
				return 0, s.rerr
			}
			s.inMsg = true
		}
		n, err := s.c.reader.Read(p)
		if err == io.EOF {
			// End of this message. An empty message is not the end of the
			// stream: go on to the next one.
			s.inMsg, err = false, nil
		}
		if err != nil {
			s.rerr = err
			return n, err
		}
		if n > 0 {
			return n, nil
		}
	}
}

func (s *WebSocketStream) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if err := s.c.WriteMessage(BinaryMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// sendClose sends a normal-closure Close frame once, best effort: it is skipped
// when a writer holds the write side (a stuck write must not make Close hang)
// and bounded by a short write deadline otherwise.
func (s *WebSocketStream) sendClose() {
	s.closeOnce.Do(func() {
		if !s.c.writeMu.TryLock() {
			return
		}
		defer s.c.writeMu.Unlock()
		_ = s.c.SetWriteDeadline(time.Now().Add(streamCloseTimeout))
		_ = wsutil.WriteMessage(s.c.Conn, s.c.state, ws.OpClose, ws.NewCloseFrameBody(ws.StatusNormalClosure, ""))
	})
}

func (s *WebSocketStream) Close() error {
	s.sendClose()
	return s.c.Close()
}

func (s *WebSocketStream) LocalAddr() net.Addr                { return s.c.LocalAddr() }
func (s *WebSocketStream) RemoteAddr() net.Addr               { return s.c.RemoteAddr() }
func (s *WebSocketStream) SetDeadline(t time.Time) error      { return s.c.SetDeadline(t) }
func (s *WebSocketStream) SetReadDeadline(t time.Time) error  { return s.c.SetReadDeadline(t) }
func (s *WebSocketStream) SetWriteDeadline(t time.Time) error { return s.c.SetWriteDeadline(t) }
