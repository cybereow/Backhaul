package handlers

import (
	"io"
	"net"
	"sync/atomic"
	"testing"

	"github.com/xtaci/smux"
)

// tailConn counts the writes smux makes to its underlying connection that carry
// exactly one 3-byte record header: a smux frame header (8 bytes) plus 3 payload
// bytes. That is what a full record leaves behind when it does not fit one frame.
type tailConn struct {
	net.Conn
	tails atomic.Int64
}

func (c *tailConn) Write(b []byte) (int, error) {
	if len(b) == 8+HalfCloseRecordSize-32768 {
		c.tails.Add(1)
	}
	return c.Conn.Write(b)
}

// tailsFor sends total bytes through an enveloped stream, written 32 KiB at a
// time as the pumps do, over a session whose max frame size is frame, and
// returns how many tail-only frames smux wrote.
func tailsFor(t *testing.T, frame, total int) int64 {
	t.Helper()
	c1, c2 := net.Pipe()
	cc := &tailConn{Conn: c1}
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
	return cc.tails.Load()
}

// With the frame size at HalfCloseRecordSize every record travels in one frame;
// at the stock 32768 each full record leaves a tail frame carrying just its last
// 3 bytes (the lone END record is the only other write of that size).
func TestHalfCloseRecordFitsOneFrame(t *testing.T) {
	const records = 256
	fit := tailsFor(t, HalfCloseRecordSize, records*32768)
	spill := tailsFor(t, 32768, records*32768)
	t.Logf("tail frames: %d at frame size %d, %d at 32768", fit, HalfCloseRecordSize, spill)
	if spill < records/4 { // smux sometimes coalesces frames into one write
		t.Fatalf("frame size 32768: only %d tail frames for %d records, expected a good share of one each; has smux's framing changed?", spill, records)
	}
	if fit > 8 {
		t.Fatalf("frame size %d: %d tail frames, want none (a record must travel in one frame)", HalfCloseRecordSize, fit)
	}
}
