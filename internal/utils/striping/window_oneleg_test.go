package striping

import (
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type windowedLeg struct {
	net.Conn
	sets atomic.Int32
}

func (l *windowedLeg) SetReceiveWindow(int) error { l.sets.Add(1); return nil }

// A group of one leg has nothing to run ahead of, and windowLoop does not manage
// it: its window is left as the transport set it, not lowered for good. Legs of
// a real group are still put on their initial window at once.
func TestOneLegGroupKeepsItsWindow(t *testing.T) {
	for _, n := range []int{1, 2} {
		legs := make([]net.Conn, n)
		rec := make([]*windowedLeg, n)
		for i := range legs {
			a, b := net.Pipe()
			defer b.Close()
			rec[i] = &windowedLeg{Conn: a}
			legs[i] = rec[i]
		}
		c := New(legs, DefaultChunkSize)
		time.Sleep(50 * time.Millisecond) // readLeg has started on every leg
		for i, l := range rec {
			if got := l.sets.Load(); (got > 0) != (n > 1) {
				t.Errorf("%d-leg group: leg %d had its window set %d time(s)", n, i, got)
			}
		}
		c.Close()
	}
}
