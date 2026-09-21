package handlers

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// Half-close envelope: per-stream framing layered above one smux stream, so a
// plain smux flow can signal a directional EOF (END) without closing the
// reverse direction. smux.Stream has no CloseWrite, so without this the
// closeWrite hook in tcp_handler.go falls back to a full Close, which truncates
// any reply still in flight (see plan 024).
//
//	record := type (1 byte) | length (2 bytes, big-endian) | payload (length bytes)
//	  0x01 DATA   length 1..32768  application bytes
//	  0x02 END    length 0         sender sends no more DATA (directional EOF)
//	  0x03 ABORT  length 0         sender abandons the stream; receiver fails loudly
//
// Every receiver-side violation is a protocol error: the stream is closed and
// Read reports an error wrapping io.ErrUnexpectedEOF, never a clean io.EOF.
const (
	hcData  byte = 0x01
	hcEnd   byte = 0x02
	hcAbort byte = 0x03

	hcHeaderLen = 3
	hcMaxData   = 32768

	// hcAbortTimeout bounds the best-effort ABORT written by Close, so a peer
	// that stopped reading cannot wedge teardown.
	hcAbortTimeout = 500 * time.Millisecond
)

// errHalfCloseAborted is what Read returns once the peer sent ABORT.
var errHalfCloseAborted = errors.New("halfclose: peer aborted the stream")

// hcBufPool holds header+payload scratch buffers: each record must reach the
// smux stream in ONE Write call, which costs one copy of the payload.
var hcBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, hcHeaderLen+hcMaxData)
		return &b
	},
}

// halfCloseConn implements the envelope over a net.Conn (a smux stream).
//
// One reader and one writer may run concurrently; CloseWrite, Close and
// AbortWrite are safe from any goroutine. It deliberately does not implement
// io.WriterTo/io.ReaderFrom, so pickCopyMode keeps using the pooled copy path
// and the envelope cannot be bypassed.
type halfCloseConn struct {
	net.Conn

	// Read side, guarded by rmu. hdr/hdrN and remaining survive a read
	// timeout, so a retry resumes exactly where it stopped.
	rmu       sync.Mutex
	hdr       [hcHeaderLen]byte
	hdrN      int
	remaining int   // unread payload bytes of the current DATA record
	rerr      error // sticky terminal read result (io.EOF after END)

	// wmu serializes Write, CloseWrite and the ABORT sent by Close.
	wmu sync.Mutex

	// mu guards the state below. sendEnded is only set after END has been
	// written, so the "both ended" close can never overtake an unsent END.
	mu          sync.Mutex
	sendEnded   bool
	sendAborted bool
	recvEnded   bool
	closed      bool  // Close called or stream failed; further I/O fails
	wpoison     error // a record was cut short mid-write: framing is lost

	closeOnce sync.Once
	closeErr  error
}

// NewHalfCloseConn wraps stream (one smux stream) in the half-close envelope.
// Both peers must wrap their end of the stream.
func NewHalfCloseConn(stream net.Conn) net.Conn {
	return newHalfCloseConn(stream)
}

func newHalfCloseConn(stream net.Conn) *halfCloseConn {
	return &halfCloseConn{Conn: stream}
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func (c *halfCloseConn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// closeStream closes the underlying smux stream exactly once.
func (c *halfCloseConn) closeStream() error {
	c.closeOnce.Do(func() { c.closeErr = c.Conn.Close() })
	return c.closeErr
}

// fail records a terminal read error and closes the stream (reader only).
func (c *halfCloseConn) fail(err error) {
	c.rerr = err
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	c.closeStream()
}

// readFailed turns an underlying read error into the terminal Read error.
func (c *halfCloseConn) readFailed(err error, what string) {
	switch {
	case c.isClosed():
		err = net.ErrClosed
	case errors.Is(err, io.EOF):
		err = fmt.Errorf("halfclose: %s before END: %w", what, io.ErrUnexpectedEOF)
	}
	c.fail(err)
}

// readHeader accumulates one record header and acts on it. On success either
// remaining>0 (DATA) or rerr is set (END/ABORT/violation); a timeout leaves the
// partial header in place.
func (c *halfCloseConn) readHeader() error {
	for c.hdrN < hcHeaderLen {
		n, err := c.Conn.Read(c.hdr[c.hdrN:])
		c.hdrN += n
		if err != nil && c.hdrN < hcHeaderLen {
			if isTimeout(err) {
				return err
			}
			what := "stream ended"
			if c.hdrN > 0 {
				what = "stream ended inside a record header"
			}
			c.readFailed(err, what)
			return nil
		}
	}
	typ, length := c.hdr[0], int(binary.BigEndian.Uint16(c.hdr[1:]))
	c.hdrN = 0
	switch {
	case typ == hcData && length >= 1 && length <= hcMaxData:
		c.remaining = length
	case typ == hcEnd && length == 0:
		c.rerr = io.EOF
		c.mu.Lock()
		c.recvEnded = true
		both := c.sendEnded
		c.mu.Unlock()
		if both {
			c.closeStream()
		}
	case typ == hcAbort && length == 0:
		c.fail(errHalfCloseAborted)
	default:
		c.fail(fmt.Errorf("halfclose: protocol error (type 0x%02x, length %d): %w", typ, length, io.ErrUnexpectedEOF))
	}
	return nil
}

// Read returns application bytes from DATA records, io.EOF after END, and an
// error (never io.EOF) for ABORT, a violation, or a stream that ended without END.
func (c *halfCloseConn) Read(p []byte) (int, error) {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	for {
		if c.rerr != nil {
			return 0, c.rerr
		}
		if c.isClosed() {
			return 0, net.ErrClosed
		}
		if len(p) == 0 {
			return 0, nil
		}
		if c.remaining == 0 {
			if err := c.readHeader(); err != nil {
				return 0, err
			}
			continue
		}
		n, err := c.Conn.Read(p[:min(len(p), c.remaining)])
		c.remaining -= n
		if err != nil && !isTimeout(err) {
			c.readFailed(err, "stream ended inside a record")
			if n > 0 {
				return n, nil // the error is delivered by the next Read
			}
			return 0, c.rerr
		}
		if n > 0 {
			return n, nil
		}
		if err != nil {
			return 0, err
		}
	}
}

// writeErr maps an underlying write error. smux reports a peer-closed stream
// as io.EOF, which a Write must not return.
func (c *halfCloseConn) writeErr(err error) error {
	switch {
	case c.isClosed():
		return net.ErrClosed
	case errors.Is(err, io.EOF):
		return fmt.Errorf("halfclose: peer closed the stream: %w", io.ErrClosedPipe)
	}
	return err
}

// sendErr reports why nothing more may be written, or nil.
func (c *halfCloseConn) sendErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.closed || c.sendEnded || c.sendAborted:
		return net.ErrClosed
	case c.wpoison != nil:
		return c.wpoison
	}
	return nil
}

// Write sends p as ceil(len(p)/32768) DATA records, each one stream Write.
func (c *halfCloseConn) Write(p []byte) (int, error) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if err := c.sendErr(); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	bp := hcBufPool.Get().(*[]byte)
	defer hcBufPool.Put(bp)
	buf := *bp
	done := 0
	for done < len(p) {
		n := min(len(p)-done, hcMaxData)
		buf[0] = hcData
		binary.BigEndian.PutUint16(buf[1:], uint16(n))
		copy(buf[hcHeaderLen:], p[done:done+n])
		w, err := c.Conn.Write(buf[:hcHeaderLen+n])
		if err != nil {
			err = c.writeErr(err)
			if w > 0 {
				// A partial record is on the wire: any further record would be
				// parsed as its payload and silently corrupt the stream.
				c.mu.Lock()
				c.wpoison = err
				c.mu.Unlock()
			}
			return done, err
		}
		done += n
	}
	return done, nil
}

// CloseWrite sends END once (idempotent) and leaves the reverse direction
// open. The stream is only closed when the receive side has already ended.
func (c *halfCloseConn) CloseWrite() error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	c.mu.Lock()
	switch {
	case c.sendEnded:
		c.mu.Unlock()
		return nil
	case c.closed || c.sendAborted:
		c.mu.Unlock()
		return net.ErrClosed
	case c.wpoison != nil:
		err := c.wpoison
		c.mu.Unlock()
		return err
	}
	c.mu.Unlock()

	if _, err := c.Conn.Write([]byte{hcEnd, 0, 0}); err != nil {
		return c.writeErr(err)
	}
	c.mu.Lock()
	c.sendEnded = true
	both := c.recvEnded
	c.mu.Unlock()
	if both {
		c.closeStream()
	}
	return nil
}

// sendAbort writes a best-effort ABORT unless the send side already ended,
// aborted or lost framing. The caller holds wmu.
func (c *halfCloseConn) sendAbort() {
	c.mu.Lock()
	skip := c.sendEnded || c.sendAborted || c.wpoison != nil
	c.sendAborted = true
	c.mu.Unlock()
	if skip {
		return
	}
	_ = c.Conn.SetWriteDeadline(time.Now().Add(hcAbortTimeout))
	_, _ = c.Conn.Write([]byte{hcAbort, 0, 0})
}

// Close is a full close and is idempotent. While the send side is still open it
// first sends ABORT, so the peer's Read fails instead of reporting a clean end;
// the ABORT is skipped when a Write is in flight (it would block), and the
// stream close then unblocks that Write and any blocked Read.
func (c *halfCloseConn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	if c.wmu.TryLock() {
		c.sendAbort()
		c.wmu.Unlock()
	}
	return c.closeStream()
}

// AbortWrite is the loud teardown transferData uses after a mid-copy error.
func (c *halfCloseConn) AbortWrite() {
	_ = c.Close()
}
