package handlers

import (
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/xtaci/smux"
)

// frameConn records the payload length of every smux PSH frame written to its
// underlying connection, parsed from the byte stream so it does not depend on how
// smux coalesces frames into writes.
type frameConn struct {
	net.Conn
	mu     sync.Mutex
	hdr    [8]byte
	hn     int // header bytes collected
	skip   int // payload bytes left in the current frame
	frames []int
}

func (c *frameConn) Write(b []byte) (int, error) {
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
		if c.hdr[1] == 2 { // cmdPSH
			c.frames = append(c.frames, c.skip)
		}
	}
	c.mu.Unlock()
	return c.Conn.Write(b)
}

func (c *frameConn) payloads() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int(nil), c.frames...)
}

// recordFrames writes one full-size record through an enveloped stream, over a
// fresh session with the given max frame size, and returns the PSH frame
// payloads smux wrote for it. A lone record fits any flow-control window, so the
// split is deterministic.
func recordFrames(t *testing.T, frame int) []int {
	t.Helper()
	c1, c2 := net.Pipe()
	cc := &frameConn{Conn: c1}
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

	if _, err := wa.Write(make([]byte, 32768)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(wb, make([]byte, 32768)); err != nil {
		t.Fatal(err)
	}
	return cc.payloads()
}

// With the frame size at HalfCloseRecordSize a full record travels in one frame;
// at the stock 32768 it leaves a tail frame carrying just its last 3 bytes.
func TestHalfCloseRecordFitsOneFrame(t *testing.T) {
	if got := recordFrames(t, HalfCloseRecordSize); len(got) != 1 || got[0] != HalfCloseRecordSize {
		t.Fatalf("frame size %d: a full record went out as frames %v, want one frame of %d", HalfCloseRecordSize, got, HalfCloseRecordSize)
	}
	// Guards the premise: if smux stops splitting at 32768, the first check proves nothing.
	if got := recordFrames(t, 32768); len(got) != 2 || got[0] != 32768 || got[1] != 3 {
		t.Fatalf("frame size 32768: a full record went out as frames %v, want [32768 3]; has smux's framing changed?", got)
	}
}
