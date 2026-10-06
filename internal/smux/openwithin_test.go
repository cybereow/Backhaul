package smux

import (
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
}
