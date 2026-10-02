package handlers

import (
	"context"
	"errors"
	"sync"
)

// replayRing keeps the payload bytes a resumable flow has sent but the peer has
// not yet acknowledged, so that after a tunnel dies they can be sent again on the
// next one. Offsets count from the start of the flow, like PumpSwapper's.
//
// The ring is bounded by limit: when it is full the sender waits for an ACK, which
// is the same backpressure TCP's window gives. Memory is only held while there are
// unacknowledged bytes: it starts empty, grows by doubling, and is released when
// everything has been acknowledged.
type replayRing struct {
	mu    sync.Mutex
	buf   []byte
	head  int    // index in buf of the first retained byte
	size  int    // retained bytes
	base  uint64 // flow offset of the first retained byte
	limit int

	space chan struct{} // signalled (capacity 1) when an ACK freed room
}

const (
	replayMinCap    = 32 << 10
	replayReleaseAt = 256 << 10 // an emptied ring bigger than this gives its memory back
)

var (
	errReplayTooLarge = errors.New("replay: write larger than the replay limit")
	errReplayWake     = errors.New("replay: wait interrupted")
)

func newReplayRing(limit int) *replayRing {
	return &replayRing{limit: limit, space: make(chan struct{}, 1)}
}

// end is the flow offset one past the last retained byte.
func (r *replayRing) end() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.base + uint64(r.size)
}

// len is the number of retained (unacknowledged) bytes.
func (r *replayRing) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.size
}

func (r *replayRing) free() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.limit - r.size
}

// waitFree blocks until n more bytes fit, ctx or abort ends, or n can never fit.
// A signal on wake (may be nil) interrupts the wait with errReplayWake: the caller
// has something more urgent than room (a swap that needs the sender to reach its
// next boundary) and decides whether to carry on.
func (r *replayRing) waitFree(ctx context.Context, abort <-chan struct{}, wake <-chan struct{}, n int) error {
	if n > r.limit {
		return errReplayTooLarge
	}
	for {
		if r.free() >= n {
			return nil
		}
		select {
		case <-r.space:
		case <-wake:
			return errReplayWake
		case <-abort:
			return context.Canceled
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// append stores p. The caller has made room with waitFree; the ring does not
// exceed its limit even if it has not.
func (r *replayRing) append(p []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if need := r.size + len(p); need > len(r.buf) {
		newCap := len(r.buf) * 2
		if newCap < replayMinCap {
			newCap = replayMinCap
		}
		for newCap < need {
			newCap *= 2
		}
		if newCap > r.limit {
			newCap = max(r.limit, need) // never more than needed past the limit
		}
		nb := make([]byte, newCap)
		r.copyOut(nb[:r.size])
		r.buf, r.head = nb, 0
	}
	tail := (r.head + r.size) % len(r.buf)
	n := copy(r.buf[tail:], p)
	if n < len(p) {
		copy(r.buf, p[n:])
	}
	r.size += len(p)
}

// copyOut copies the retained bytes, in order, into dst (len(dst) <= size).
// The caller holds mu.
func (r *replayRing) copyOut(dst []byte) {
	if len(dst) == 0 {
		return
	}
	n := copy(dst, r.buf[r.head:min(r.head+r.size, len(r.buf))])
	if n < len(dst) {
		copy(dst[n:], r.buf)
	}
}

// ackTo drops every byte before flow offset off. An offset outside what is
// retained (stale, or beyond what was sent) is ignored, so a duplicated or
// reordered ACK is harmless.
func (r *replayRing) ackTo(off uint64) {
	r.mu.Lock()
	if off <= r.base || off > r.base+uint64(r.size) {
		r.mu.Unlock()
		return
	}
	drop := int(off - r.base)
	r.head = (r.head + drop) % max(len(r.buf), 1)
	r.size -= drop
	r.base = off
	if r.size == 0 {
		r.head = 0
		if len(r.buf) > replayReleaseAt {
			r.buf = nil
		}
	}
	r.mu.Unlock()
	select {
	case r.space <- struct{}{}:
	default:
	}
}

// readAt copies retained bytes starting at flow offset off into p and returns how
// many it copied; zero if off is not retained (already acknowledged or not yet
// sent). Used to resend what the peer missed.
func (r *replayRing) readAt(off uint64, p []byte) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if off < r.base || off >= r.base+uint64(r.size) {
		return 0
	}
	skip := int(off - r.base)
	n := min(len(p), r.size-skip)
	start := (r.head + skip) % len(r.buf)
	c := copy(p[:n], r.buf[start:min(start+n, len(r.buf))])
	if c < n {
		copy(p[c:n], r.buf)
	}
	return n
}
