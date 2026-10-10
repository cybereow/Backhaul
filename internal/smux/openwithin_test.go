package smux

import (
	"io"
	"net"
	"testing"
	"time"
)

// A connection that takes nothing (its peer has stopped reading) keeps a stream's
// SYN from being written: OpenStreamWithin gives up after its own bound instead
// of the default half minute.
func TestOpenStreamWithinGivesUpOnAStalledConnection(t *testing.T) {
	a, b := net.Pipe() // unbuffered, and nobody reads b
	defer a.Close()
	defer b.Close()
	cfg := DefaultConfig()
	cfg.KeepAliveDisabled = true
	s, err := Client(a, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	start := time.Now()
	if _, err := s.OpenStreamWithin(200 * time.Millisecond); err == nil {
		t.Fatal("a stream was opened on a connection that takes nothing")
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("gave up after %v, want about 200ms", d)
	}

	// The connection gets through again: the SYN that was left queued arrives
	// after all, and the stream it opens on the peer is closed right behind it
	// instead of staying there with nobody on this side.
	srv, err := Server(b, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ghost, err := srv.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	_ = ghost.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := ghost.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("the stream left behind by a timed-out open was not closed: %v", err)
	}
}
