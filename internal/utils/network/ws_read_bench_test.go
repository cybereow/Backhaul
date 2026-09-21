package network

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobwas/ws"
)

// frameStreamConn serves an endless stream of pre-encoded WebSocket frames from
// one reused buffer, and counts how many Read calls (i.e. socket syscalls in
// production) the WebSocket layer makes to consume them.
//
// Deliberately not net.Pipe: a synchronous pipe couples the reader to a writer
// goroutine and measures the pipe's handoff, not the framing layer's own cost.
type frameStreamConn struct {
	stream []byte
	off    int
	reads  int64
}

func (c *frameStreamConn) Read(p []byte) (int, error) {
	atomic.AddInt64(&c.reads, 1)
	n := copy(p, c.stream[c.off:])
	c.off += n
	if c.off == len(c.stream) {
		c.off = 0 // wrap: the stream is a whole number of frames
	}
	return n, nil
}

func (c *frameStreamConn) Write(p []byte) (int, error)        { return len(p), nil }
func (c *frameStreamConn) Close() error                       { return nil }
func (c *frameStreamConn) LocalAddr() net.Addr                { return nil }
func (c *frameStreamConn) RemoteAddr() net.Addr               { return nil }
func (c *frameStreamConn) SetDeadline(time.Time) error        { return nil }
func (c *frameStreamConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *frameStreamConn) SetWriteDeadline(t time.Time) error { return nil }

// buildFrameStream encodes `count` unmasked binary frames of `payload` bytes
// into a single contiguous buffer.
func buildFrameStream(tb testing.TB, payload, count int) []byte {
	var out []byte
	body := make([]byte, payload)
	for i := 0; i < count; i++ {
		var hdrBuf sliceWriter
		h := ws.Header{Fin: true, OpCode: ws.OpBinary, Length: int64(payload)}
		if err := ws.WriteHeader(&hdrBuf, h); err != nil {
			tb.Fatalf("WriteHeader: %v", err)
		}
		out = append(out, hdrBuf...)
		out = append(out, body...)
	}
	return out
}

type sliceWriter []byte

func (w *sliceWriter) Write(p []byte) (int, error) {
	*w = append(*w, p...)
	return len(p), nil
}

// benchWSRead measures the per-message cost of the ws/wss read path: one
// NextReader (frame header) plus draining the payload, exactly as
// handlers.transferWebSocketToTCP does.
func benchWSRead(b *testing.B, payload int) {
	const framesPerCycle = 64
	stream := buildFrameStream(b, payload, framesPerCycle)
	src := &frameStreamConn{stream: stream}
	conn := NewWebSocketConn(src, ws.StateClientSide, nil)
	sink := make([]byte, 64*1024)

	b.SetBytes(int64(payload))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, r, err := conn.NextReader()
		if err != nil {
			b.Fatalf("NextReader: %v", err)
		}
		if _, err := io.CopyBuffer(onlyWriterBench{io.Discard}, r, sink); err != nil {
			b.Fatalf("drain: %v", err)
		}
	}

	b.StopTimer()
	b.ReportMetric(float64(atomic.LoadInt64(&src.reads))/float64(b.N), "reads/msg")
}

// onlyWriterBench hides ReaderFrom so CopyBuffer uses the supplied buffer,
// mirroring handlers.onlyWriter.
type onlyWriterBench struct{ io.Writer }

func BenchmarkWSRead1KB(b *testing.B)  { benchWSRead(b, 1024) }
func BenchmarkWSRead8KB(b *testing.B)  { benchWSRead(b, 8*1024) }
func BenchmarkWSRead64KB(b *testing.B) { benchWSRead(b, 64*1024) }

// countingTCPConn wraps a real socket so the benchmark can count actual read
// syscalls while still paying their true cost.
type countingTCPConn struct {
	net.Conn
	reads int64
}

func (c *countingTCPConn) Read(p []byte) (int, error) {
	atomic.AddInt64(&c.reads, 1)
	return c.Conn.Read(p)
}

// benchWSReadLoopback measures the ws/wss read path over a real TCP socket, so
// every avoided frame-header read is an avoided syscall rather than an avoided
// memcpy. This is the measurement that matters: the in-memory variants above
// make reads nearly free and therefore understate the saving.
func benchWSReadLoopback(b *testing.B, payload int) {
	const framesPerCycle = 32
	stream := buildFrameStream(b, payload, framesPerCycle)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	stop := make(chan struct{})
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := c.Write(stream); err != nil {
				return
			}
		}
	}()

	raw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	defer raw.Close()

	src := &countingTCPConn{Conn: raw}
	conn := NewWebSocketConn(src, ws.StateClientSide, nil)
	sink := make([]byte, 64*1024)

	b.SetBytes(int64(payload))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, r, err := conn.NextReader()
		if err != nil {
			b.Fatalf("NextReader: %v", err)
		}
		if _, err := io.CopyBuffer(onlyWriterBench{io.Discard}, r, sink); err != nil {
			b.Fatalf("drain: %v", err)
		}
	}

	b.StopTimer()
	close(stop)
	b.ReportMetric(float64(atomic.LoadInt64(&src.reads))/float64(b.N), "reads/msg")
}

func BenchmarkWSReadLoopback1KB(b *testing.B)  { benchWSReadLoopback(b, 1024) }
func BenchmarkWSReadLoopback8KB(b *testing.B)  { benchWSReadLoopback(b, 8*1024) }
func BenchmarkWSReadLoopback64KB(b *testing.B) { benchWSReadLoopback(b, 64*1024) }
