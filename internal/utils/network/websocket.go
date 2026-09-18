package network

import (
	"bufio"
	"bytes"
	"sync"

	"errors"
	"io"
	"net"

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
			c.writeMu.Lock()
			err = wsutil.WriteMessage(c.Conn, c.state, ws.OpPong, nil)
			c.writeMu.Unlock()
			if err != nil {
				return 0, nil, err
			}
			// Drain the ping payload
			_, err = io.Copy(io.Discard, c.reader)
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
