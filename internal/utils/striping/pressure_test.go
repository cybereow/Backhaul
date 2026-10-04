package striping

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"
)

// TestStripedBudgetPressureDeliversEverything runs many small transfers over
// eight legs with room for only five chunks, so leg readers are waiting for room
// all the time. Waiting is never a reason to fail a flow whose chunks all
// arrive: in particular, a leg that hands over the awaited chunk and then waits
// on a later one must not be taken for a flow that can no longer make progress.
// That mistake showed about once in two hundred transfers, so this run is a
// check under pressure rather than a sure catch; raise runs to hunt for it.
func TestStripedBudgetPressureDeliversEverything(t *testing.T) {
	const (
		nLegs = 8
		chunk = 16
		size  = 4096
		runs  = 40
	)
	payload := bytes.Repeat([]byte("0123456789abcdef"), size/16)
	for run := 0; run < runs; run++ {
		a, b := pipePair(nLegs)
		sender, receiver := New(a, chunk), New(b, chunk)
		receiver.budget.limit = 5 * (retainedEntryOverhead + chunk) // before any I/O

		sent := make(chan error, 1)
		go func() {
			_, err := sender.Write(payload)
			sender.Close()
			sent <- err
		}()
		_ = receiver.SetReadDeadline(time.Now().Add(20 * time.Second))
		got, err := io.ReadAll(receiver)
		receiver.Close()
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("transfer %d: %d of %d bytes, read error %v, write error %v", run, len(got), size, err, <-sent)
		}
		for _, c := range append(a, b...) {
			c.(net.Conn).Close()
		}
	}
}
