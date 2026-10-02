package handlers

import (
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/xtaci/smux"
)

// tailConn walks the smux frame stream written to its underlying connection and
// counts PSH frames carrying exactly one 3-byte record header: what a full
// record leaves behind when it does not fit one frame. Frames are parsed from
// the byte stream, so the count does not depend on how smux coalesces writes.
type tailConn struct {
	net.Conn
	mu    sync.Mutex
	hdr   [8]byte
	hn    int // header bytes collected
	skip  int // payload bytes left in the current frame
	tails int64
}

func (c *tailConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	for p := b; len(p) > 0; {
		if c.skip > 0 {
			n := min(c.skip, len(p))
			c.skip -= n
			p = p[n:]
			continue
		}
		n := copy(c.hdr[c.hn:], p)
		c.hn += n
		p = p[n:]
		if c.hn < len(c.hdr) {
			continue
		}
		c.hn = 0
		c.skip = int(binary.LittleEndian.Uint16(c.hdr[2:4]))
		if c.hdr[1] == 2 && c.skip == 3 { // cmdPSH
			c.tails++
		}
	}
	c.mu.Unlock()
	return c.Conn.Write(b)
}

func (c *tailConn) count() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tails
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
	return cc.count()
}

// With the frame size at HalfCloseRecordSize every record travels in one frame;
// at the stock 32768 each full record leaves a tail frame carrying just its last
// 3 bytes (the lone END record is the only other write of that size).
func TestHalfCloseRecordFitsOneFrame(t *testing.T) {
	const records = 256
	fit := tailsFor(t, HalfCloseRecordSize, records*32768)
	spill := tailsFor(t, 32768, records*32768)
	t.Logf("tail frames: %d at frame size %d, %d at 32768", fit, HalfCloseRecordSize, spill)
	if spill < records/4 {
		t.Fatalf("frame size 32768: only %d tail frames for %d records, expected a good share of one each; has smux's framing changed?", spill, records)
	}
	if fit > 8 {
		t.Fatalf("frame size %d: %d tail frames, want none (a record must travel in one frame)", HalfCloseRecordSize, fit)
	}
}
