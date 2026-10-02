package handlers

import (
	"io"
	"net"
	"sync/atomic"
	"testing"

	"github.com/xtaci/smux"
)

// countingConn counts the writes smux makes to its underlying connection (one
// per frame).
type countingConn struct {
	net.Conn
	writes atomic.Int64
}

func (c *countingConn) Write(b []byte) (int, error) {
	c.writes.Add(1)
	return c.Conn.Write(b)
}

// framesFor sends total bytes through an enveloped stream, written 32 KiB at a
// time as the pumps do, over a session whose max frame size is frame, and
// returns how many writes smux made to its connection.
func framesFor(t *testing.T, frame, total int) int64 {
	t.Helper()
	c1, c2 := net.Pipe()
	cc := &countingConn{Conn: c1}
	cfg := smux.DefaultConfig()
	cfg.Version = 2
	cfg.MaxFrameSize = frame
	client, err := smux.Client(cc, cfg)
	if err != nil {
		t.Fatal(err)
	}
	server, err := smux.Server(c2, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close(); server.Close(); c1.Close(); c2.Close() })
	a, b := openStreamPair(t, client, server)
	wa, wb := NewHalfCloseConn(a), NewHalfCloseConn(b)

	done := make(chan struct{})
	go func() {
		defer close(done)
		io.Copy(io.Discard, wb)
	}()
	buf := make([]byte, 32768)
	for sent := 0; sent < total; sent += len(buf) {
		if _, err := wa.Write(buf); err != nil {
			t.Fatal(err)
		}
	}
	if cw, ok := wa.(interface{ CloseWrite() error }); ok {
		cw.CloseWrite()
	}
	<-done
	return cc.writes.Load()
}

// With the frame size at HalfCloseRecordSize every record travels in one frame;
// at the stock 32768 each full record leaves a 3-byte frame behind, roughly
// doubling the writes.
func TestHalfCloseRecordFitsOneFrame(t *testing.T) {
	const total = 8 << 20 // 256 records
	fit := framesFor(t, HalfCloseRecordSize, total)
	spill := framesFor(t, 32768, total)
	// The counts include a few control frames, so compare the two runs rather than
	// an exact number: one frame per record against two per record.
	if fit*10 > spill*7 {
		t.Fatalf("frame size %d: %d writes, against %d at 32768; a record should travel in one frame", HalfCloseRecordSize, fit, spill)
	}
	if spill < 2*256-16 {
		t.Fatalf("frame size 32768: %d writes, expected about two per record; has smux's framing changed?", spill)
	}
}
