package handlers

import (
	"testing"
	"time"
)

// The end that closes pool sessions keeps a tunnel its flow was moved off until
// the peer has closed it: closing first would let the session under it be closed
// with this end's last bytes still on their way. The other end, and a flow that
// was not told to hold, close at once (or each would wait for the other).
func TestReleaseHoldsTheOldTunnelUntilThePeerClosesIt(t *testing.T) {
	released := func(hold bool) (done <-chan struct{}, peerClose func()) {
		a, b := streamPair(t)
		p := &PumpSwapper{holdReleased: hold, abortCh: make(chan struct{})}
		ch := make(chan struct{})
		go func() {
			p.release(NewHalfCloseConn(a))
			close(ch)
		}()
		return ch, func() { b.Close() }
	}

	done, _ := released(false)
	waitClosed(t, done, "a release without the hold")

	done, peerClose := released(true)
	select {
	case <-done:
		t.Fatal("the tunnel was released while the peer still had it open")
	case <-time.After(150 * time.Millisecond):
	}
	// The peer's END only ends its data; it is the close of its stream that says
	// it has finished with the tunnel.
	peerClose()
	waitClosed(t, done, "the release after the peer closed its side")

	// A flow that ends does not leave its old tunnels waiting.
	a, _ := streamPair(t)
	p := &PumpSwapper{holdReleased: true, abortCh: make(chan struct{})}
	ch := make(chan struct{})
	go func() {
		p.release(NewHalfCloseConn(a))
		close(ch)
	}()
	close(p.abortCh)
	waitClosed(t, ch, "the release of an aborted flow's tunnel")
}
