package striping

import (
	"encoding/binary"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// seqFrameConn is a net.Conn whose Read serves an endless stream of sequential,
// full-size striping frames (an 8-byte header carrying seq+length, followed by a
// chunkSize payload) straight from memory. It never blocks and never allocates
// per frame - a single reusable frame buffer is rewound with a fresh sequence
// number each time - so a read benchmark measures the Conn's own reassembly and
// buffer handling rather than the cost of an in-memory pipe. Write is discarded;
// only the read half is exercised.
type seqFrameConn struct {
	frame  []byte
	off    int
	seq    uint32
	closed int32
}

func newSeqFrameConn(chunkSize int) *seqFrameConn {
	frame := make([]byte, headerSize+chunkSize)
	binary.BigEndian.PutUint32(frame[4:8], uint32(chunkSize))
	return &seqFrameConn{frame: frame, off: len(frame)}
}

func (f *seqFrameConn) Read(p []byte) (int, error) {
	if atomic.LoadInt32(&f.closed) == 1 {
		return 0, io.EOF
	}
	if f.off >= len(f.frame) {
		// Start the next frame: only the 4-byte sequence number changes, so the
		// payload bytes are reused untouched and nothing is allocated.
		binary.BigEndian.PutUint32(f.frame[0:4], f.seq)
		f.seq++
		f.off = 0
	}
	n := copy(p, f.frame[f.off:])
	f.off += n
	return n, nil
}

func (f *seqFrameConn) Write(b []byte) (int, error)     { return len(b), nil }
func (f *seqFrameConn) Close() error                    { atomic.StoreInt32(&f.closed, 1); return nil }
func (f *seqFrameConn) LocalAddr() net.Addr             { return dummyAddr{} }
func (f *seqFrameConn) RemoteAddr() net.Addr            { return dummyAddr{} }
func (f *seqFrameConn) SetDeadline(time.Time) error     { return nil }
func (f *seqFrameConn) SetReadDeadline(time.Time) error { return nil }
func (f *seqFrameConn) SetWriteDeadline(t time.Time) error {
	return nil
}

type dummyAddr struct{}

func (dummyAddr) Network() string { return "mem" }
func (dummyAddr) String() string  { return "mem" }

// BenchmarkConnRead measures per-chunk allocation on the striped read path. A
// single leg is fed a continuous in-memory stream of full-size framed chunks and
// Read is drained as fast as it can reassemble them; what remains is the cost of
// reassembly and buffer management. With the read buffer pool in place the
// chunkSize payload buffers no longer show up as per-op allocations - mirroring
// BenchmarkConnWrite on the write side.
func BenchmarkConnRead(b *testing.B) {
	const chunkSize = 16 * 1024
	c := New([]net.Conn{newSeqFrameConn(chunkSize)}, chunkSize)
	defer c.Close()

	dst := make([]byte, chunkSize)
	b.SetBytes(chunkSize)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// io.ReadFull collapses partial reads into exactly one chunk consumed per
		// iteration, keeping the allocation accounting at one chunk per op.
		if _, err := io.ReadFull(c, dst); err != nil {
			b.Fatalf("read: %v", err)
		}
	}
}
