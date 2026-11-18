package handlers

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// replayRing keeps the payload bytes a resumable flow has sent but the peer has
// not yet acknowledged, so that after a tunnel dies they can be sent again on the
// next one. Offsets count from the start of the flow, like PumpSwapper's.
//
// The ring is bounded by limit: when it is full the sender waits for an ACK, which
// is the same backpressure TCP's window gives. Memory is only held while there are
// unacknowledged bytes: it starts empty, grows by doubling, and is released when
// everything has been acknowledged.
// Process-wide figures about the rings, for the pool health line: what they hold
// now, and how many senders wait for room in one, for how long in all.
var (
	replayHeld    atomic.Int64
	replayWaiting atomic.Int64
	replayWaitNs  atomic.Int64
)

// ReplayGauges are the process's replay figures: bytes held in rings now,
// senders waiting for room in a full ring now, and the time senders have waited
// for room since the process started.
func ReplayGauges() (held, waiting int64, waited time.Duration) {
	return replayHeld.Load(), replayWaiting.Load(), time.Duration(replayWaitNs.Load())
}

// A ringChunk is a stretch of retained bytes in a buffer from copyBufferPool,
// returned to it once all of the chunk has been acknowledged.
type ringChunk struct {
	bp   *[]byte
	s, e int // the retained bytes are (*bp)[s:e]
}

type replayRing struct {
	mu     sync.Mutex
	chunks []ringChunk // oldest first
	size   int         // retained bytes
	base   uint64      // flow offset of the first retained byte
	limit  int

	space chan struct{} // signalled (capacity 1) when an ACK freed room

	onEmpty func()      // called, outside mu, when an ACK has emptied the ring
	idle    *time.Timer // see arm
}

// ringOwnMin is the least a read must be for the ring to take over its buffer
// (appendOwned) instead of copying it: a smaller one would leave most of a
// chunk's memory idle for as long as its bytes are retained.
const ringOwnMin = copyBufferSize * 3 / 4

var (
	errReplayTooLarge = errors.New("replay: write larger than the replay limit")
	errReplayWake     = errors.New("replay: wait interrupted")
	errReplayTimer    = errors.New("replay: waited long enough to grow")
)

// getLimit and setLimit: the limit can grow while the flow runs (see ReplayGrant).
func (r *replayRing) getLimit() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.limit
}

func (r *replayRing) setLimit(n int) {
	r.mu.Lock()
	r.limit = n
	r.mu.Unlock()
	select { // a sender waiting for room may fit now
	case r.space <- struct{}{}:
	default:
	}
}

// discard drops everything retained and the storage with it: the flow no longer
// replays. (ackTo keeps a small emptied buffer for the next bytes; a ring that
// will never be used again must not, its share of the budget is being released.)
func (r *replayRing) discard() {
	r.mu.Lock()
	replayHeld.Add(-int64(r.size))
	r.base += uint64(r.size)
	for _, c := range r.chunks {
		copyBufferPool.Put(c.bp)
	}
	r.chunks, r.size = nil, 0
	r.mu.Unlock()
	select {
	case r.space <- struct{}{}:
	default:
	}
}

func newReplayRing(limit int) *replayRing {
	return &replayRing{limit: limit, space: make(chan struct{}, 1)}
}

// end is the flow offset one past the last retained byte.
func (r *replayRing) end() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.base + uint64(r.size)
}

// firstOffset is the flow offset of the oldest retained byte.
func (r *replayRing) firstOffset() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.base
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
func (r *replayRing) waitFree(ctx context.Context, abort <-chan struct{}, wake <-chan struct{}, timer <-chan time.Time, n int) error {
	if n > r.getLimit() {
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
		case <-timer:
			return errReplayTimer
		case <-abort:
			return context.Canceled
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// append stores a copy of p. The caller has made room with waitFree; the ring
// does not exceed its limit even if it has not.
func (r *replayRing) append(p []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for len(p) > 0 {
		if n := len(r.chunks); n == 0 || r.chunks[n-1].e == copyBufferSize {
			r.chunks = append(r.chunks, ringChunk{bp: copyBufferPool.Get().(*[]byte)})
		}
		t := &r.chunks[len(r.chunks)-1]
		c := copy((*t.bp)[t.e:], p)
		t.e += c
		p = p[c:]
		r.size += c
		replayHeld.Add(int64(c))
	}
}

// appendOwned retains the first n bytes of *bp without copying them: the ring
// takes the buffer, and the caller must neither write to those bytes nor put the
// buffer back (it may go on reading them, until the bytes have been sent: the
// peer cannot acknowledge what has not been). n must be at least ringOwnMin.
func (r *replayRing) appendOwned(bp *[]byte, n int) {
	r.mu.Lock()
	r.chunks = append(r.chunks, ringChunk{bp: bp, e: n})
	r.size += n
	r.mu.Unlock()
	replayHeld.Add(int64(n))
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
	replayHeld.Add(-int64(drop))
	r.size -= drop
	r.base = off
	for left := drop; left > 0; {
		c := &r.chunks[0]
		if avail := c.e - c.s; left >= avail {
			left -= avail
			copyBufferPool.Put(c.bp)
			r.chunks[0] = ringChunk{}
			r.chunks = r.chunks[1:]
		} else {
			c.s += left
			left = 0
		}
	}
	empty := r.size == 0
	if empty {
		r.chunks = nil
	}
	hook := r.onEmpty
	r.mu.Unlock()
	select {
	case r.space <- struct{}{}:
	default:
	}
	if empty && hook != nil {
		hook()
	}
}

// shrinkIfIdle puts the limit back to to if nothing is retained, and reports
// whether it did.
func (r *replayRing) shrinkIfIdle(to int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.size != 0 || r.limit <= to {
		return false
	}
	r.limit = to
	return true
}

// arm runs f after d unless the ring is armed again first: it is how a ring that
// has been emptied is given a moment to be filled again before it is shrunk.
func (r *replayRing) arm(d time.Duration, f func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.idle == nil {
		r.idle = time.AfterFunc(d, f)
		return
	}
	r.idle.Reset(d)
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
	got := 0
	for _, c := range r.chunks {
		if avail := c.e - c.s; skip >= avail {
			skip -= avail
			continue
		}
		got += copy(p[got:n], (*c.bp)[c.s+skip:c.e])
		skip = 0
		if got == n {
			break
		}
	}
	return got
}
