// Package striping lets a single logical byte stream be split across several
// underlying net.Conn legs instead of pinned to one. A single TCP-based leg
// (one pooled tunnel connection) caps a flow's throughput at roughly
// window/RTT; spreading one flow's bytes across N legs gives it N
// independent congestion windows instead of one - but only if the legs are
// actually kept busy in proportion to how fast each one drains, a clean
// end-of-stream is coordinated across all of them, and a leg that stops
// responding gets detected and torn down instead of hanging forever.
//
// How it works:
//   - Writes are handed to whichever leg is free next (a work-stealing
//     queue), not round-robined blindly - a congested leg naturally gets
//     fewer chunks instead of stalling the reorder buffer on the receiving
//     end waiting for its share. Pacing is left to each leg's own transport
//     (smux stream flow control over TCP): a congested leg simply blocks in
//     Write until its window opens.
//   - Shutdown is explicit, not inferred from timing. On Close the queued
//     chunks are flushed to the legs, then an end-of-stream marker carrying
//     the total chunk count is sent on each leg before the legs are torn
//     down. The receiver reports a clean io.EOF exactly when it has delivered
//     that many chunks in order - so the tail of a stream can't slip through
//     as a short read when a leg's EOF races the last chunks still buffered,
//     and a genuinely missing chunk surfaces as io.ErrUnexpectedEOF instead
//     of silent truncation.
//   - Every leg write carries a deadline, and Read gives up waiting for a
//     missing sequence number after stallTimeout. Either one tears the whole
//     Conn down, which unblocks the other direction's I/O too, instead of
//     leaking goroutines and streams on a silently dead leg.
//
// This still isn't real reliability: if a leg dies mid-stream its in-flight
// byte range can't be recovered, so Read reports io.ErrUnexpectedEOF like a
// dropped TCP connection would - there's no retransmission. What's guaranteed
// is that a *clean* shutdown delivers every byte, and a dirty one ends
// promptly with an error instead of hanging.
package striping

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultChunkSize is the payload size each write is sliced into before
// being handed to a leg. Small enough that legs interleave frequently (so
// one slow leg doesn't hold up a large share of the data), large enough
// that the 8-byte per-chunk header is negligible overhead.
const DefaultChunkSize = 16 * 1024

// defaultStallTimeout bounds how long a leg write or a Read waiting on a
// missing sequence number can block before the whole Conn is torn down.
const defaultStallTimeout = 20 * time.Second

const headerSize = 8 // 4 bytes sequence number + 4 bytes payload length

// endMarkerLen is a sentinel in a chunk header's length field marking the end
// of the stream. A real chunk's length is at most chunkSize, nowhere near
// this, so it's unambiguous. When set, the header's seq field carries the
// total number of data chunks the sender produced - so the receiver knows
// exactly when it has the whole stream, completion that doesn't depend on the
// timing of each leg's EOF (which is what made the tail of a stream
// occasionally slip through as a short read).
const endMarkerLen = 0xFFFFFFFF

// Reroute tunables. A leg that stops moving altogether must not strand the
// chunk it is holding: the reader would wait on that sequence number until the
// flow times out.
const (
	// resendTimeout is how long a chunk may sit in flight on one leg before the
	// same sequence number is re-sent on another leg (the receiver dedups by
	// seq). It trips for a leg that has stalled, or one whose receiver is making
	// it wait for the others; either way the copy costs one chunk.
	resendTimeout = 1500 * time.Millisecond
	// healthTick is how often the reroute watchdog scans for stuck chunks.
	healthTick = 200 * time.Millisecond
)

// legSched tracks the chunk a leg is currently writing, so the watchdog can
// re-send it elsewhere if the leg stalls. Guarded by Conn.schedMu.
//
// There is deliberately no ranking of legs here. Each worker takes the next
// chunk when its leg's transport accepts one, so a leg carries what it can
// deliver, and how far a leg may run behind the others is bounded by the
// receiving side (see window.go). An earlier version also kept slow legs out of
// the sequential path by timing each Write; but a Write into a transport window
// returns at once until the window is full and then blocks, whatever the path
// carries, so those timings ranked equal legs apart at random and left all but
// one of them idle.
type legSched struct {
	inSeq    uint32
	inData   []byte
	inFly    bool
	inStart  time.Time
	inResent bool
}

// reassemblyBudgetBytes is the per-connection ceiling on memory retained for
// reassembly: raw/decoded payload waiting in the reorder buffer, payload held in
// the hand-off channel, and (FEC) incomplete shards. A striped leg that would
// exceed it waits (see Conn.admit); an FEC flow that exceeds it is closed with a
// descriptive error. With the 16KiB DefaultChunkSize even a 256-shard FEC row
// (4MiB) fits.
// ponytail: fixed initial ceiling; make it configurable if real traffic needs a
// deeper reorder window.
const reassemblyBudgetBytes = 32 << 20

// retainedEntryOverhead is charged per retained chunk/row on top of its payload,
// so zero-length or tiny entries cannot fill a map for free.
const retainedEntryOverhead = 64

// reassemblyBudget is a lock-free byte counter for retained reassembly memory.
// limit is set before any I/O (New, or a test before it touches the legs).
type reassemblyBudget struct {
	limit int64
	used  int64 // atomic
	peak  int64 // atomic
}

// reserve charges n bytes, reporting false (and charging nothing) if that would
// exceed limit. Callers reserve before retaining and release exactly what they
// reserved.
func (b *reassemblyBudget) reserve(n int64) bool {
	if n > b.limit {
		return false
	}
	v := atomic.AddInt64(&b.used, n)
	if v > b.limit {
		atomic.AddInt64(&b.used, -n)
		return false
	}
	for {
		p := atomic.LoadInt64(&b.peak)
		if v <= p || atomic.CompareAndSwapInt64(&b.peak, p, v) {
			return true
		}
	}
}

func (b *reassemblyBudget) release(n int64) { atomic.AddInt64(&b.used, -n) }

// force charges n bytes whatever is retained already. It is for the one chunk
// that cannot wait: the sequence Read is blocked on, whose arrival is what frees
// the rest.
func (b *reassemblyBudget) force(n int64) { atomic.AddInt64(&b.used, n) }

func (b *reassemblyBudget) err(need int64) error {
	return fmt.Errorf("striping: reassembly budget exceeded (%d bytes retained + %d needed > %d limit): an earlier sequence has not arrived or the reader is too far behind",
		atomic.LoadInt64(&b.used), need, b.limit)
}

// depth sizes a hand-off channel so a full channel holds at most a quarter of
// the budget (at least one slot), never more than max.
func (b *reassemblyBudget) depth(max int, per int64) int {
	if d := b.limit / 4 / per; d < int64(max) {
		max = int(d)
	}
	if max < 1 {
		max = 1
	}
	return max
}

// gapState remembers when the gap at one expected sequence was first observed,
// so later arrivals of other sequences cannot renew its deadline. Guarded by
// the owning conn's rmu.
type gapState struct {
	seq   uint32
	start time.Time // zero: no gap observed
}

// Callers clear the state while merely idle (nothing provably missing), and
// begin a gap - stamped with the first-evidence time, so a following gap that
// was already observable does not get a fresh full timeout - whenever the
// expected sequence has no recorded gap yet.
func (g *gapState) clear()                         { g.start = time.Time{} }
func (g *gapState) fresh(next uint32) bool         { return g.start.IsZero() || g.seq != next }
func (g *gapState) begin(next uint32, t time.Time) { g.seq, g.start = next, t }

// armTimer (re)arms *t to fire after d, stopping and draining it first so no
// stale tick survives, and returns its channel. d < 0 leaves it stopped and
// returns nil, which blocks forever in a select (idle: no timer).
func armTimer(t **time.Timer, d time.Duration) <-chan time.Time {
	if *t == nil {
		if d < 0 {
			return nil
		}
		*t = time.NewTimer(d)
		return (*t).C
	}
	if !(*t).Stop() {
		select {
		case <-(*t).C:
		default:
		}
	}
	if d < 0 {
		return nil
	}
	(*t).Reset(d)
	return (*t).C
}

type chunk struct {
	seq  uint32
	at   time.Time // when Read filed it in pending (out-of-order arrival)
	data []byte
	// base is the full-size backing buffer that data slices into, borrowed from
	// the Conn's readPool by readLeg. Read returns it to the pool once the
	// chunk's bytes have been fully copied out to the caller. nil for a
	// zero-length chunk, which borrows no buffer.
	base *[]byte
}

type writeJob struct {
	seq  uint32
	data []byte
	// buf is the full-size backing buffer that data slices into, borrowed from
	// the Conn's chunkPool. The write worker returns it to the pool once the
	// chunk has been framed and sent, so a sustained transfer reuses a small
	// set of buffers instead of allocating one per chunk.
	buf *[]byte
}

// Conn stripes Read/Write over multiple legs. It implements net.Conn so it
// is a drop-in replacement anywhere a single stream/connection was used.
type Conn struct {
	legs         []net.Conn
	chunkSize    int
	stallTimeout time.Duration

	wmu        sync.Mutex // guards writeSeq only; queueing itself is lock-free via the channel
	writeSeq   uint32
	writeQueue chan writeJob
	// resendQueue is a priority reroute path: chunks a stalled leg is stuck on
	// are re-queued here (by the watchdog) and picked up by another leg ahead
	// of new work, so a stalled sequence number reaches the reader without
	// waiting out the stuck leg.
	resendQueue chan writeJob
	// schedMu guards sched, the chunk each leg has in flight.
	schedMu sync.Mutex
	sched   []legSched
	// chunkPool recycles chunkSize payload buffers across the write path so a
	// high-throughput flow doesn't allocate a fresh buffer for every chunk.
	// Buffers are handed out in Write and returned by the write workers.
	chunkPool sync.Pool
	writersWG sync.WaitGroup // write workers, waited on by a graceful Close
	flush     chan struct{}  // closed by a graceful Close: drain writeQueue, then stop
	flushOnce sync.Once

	// rmu guards only the reassembly state below (nextSeq/pending/readBuf)
	// and is held for as long as Read blocks waiting on the network. permErr
	// deliberately has its own lock: Read and Write must be able to proceed
	// concurrently the way any net.Conn allows, and sharing rmu with Write
	// (even just to peek at permErr) deadlocks a bidirectional flow - one
	// side's Read blocks on rmu waiting for data, which is exactly the lock
	// the other side's Write needs to even start sending it.
	rmu     sync.Mutex
	nextSeq uint32
	// nextSeqSeen mirrors nextSeq for the leg readers, which cannot take rmu
	// (Read holds it while it waits for them). Atomic; see advance.
	nextSeqSeen uint32
	// parkAt is, for each leg reader waiting for reassembly room (see admit), the
	// sequence it is holding plus one; zero for a reader that is not waiting.
	// Atomic. parkCh wakes Read when one starts to wait.
	parkAt []uint64
	// legWaited is set for a leg whose reader had to wait for room since
	// windowLoop last looked. Atomic.
	legWaited []uint32
	parkCh    chan struct{}
	pending   map[uint32]chunk
	readBuf   []byte
	// readBufBase is the pooled backing buffer for readBuf. It's returned to
	// readPool once readBuf drains to empty, so the next inbound chunk can reuse
	// it. Guarded by rmu, like the rest of the reassembly state.
	readBufBase *[]byte
	readCost    int64 // budget charge of the chunk behind readBuf
	// budget bounds everything retained for reassembly (see reassemblyBudgetBytes).
	budget reassemblyBudget
	// legRead counts the payload bytes each leg delivered since windowLoop last
	// looked. Atomic.
	legRead []int64
	// gap is the absolute deadline state of the gap Read is waiting on.
	gap gapState
	// readTimer is the reusable gap timer for Read's blocking loop, created
	// lazily and re-armed via armTimer. It only runs while a gap is proven; an
	// idle connection has no timer. Guarded by rmu (Read holds it for its whole
	// duration), so one timer serves every Read call and every loop turn.
	readTimer *time.Timer
	// readPool recycles chunkSize payload buffers on the read/reassembly path,
	// mirroring chunkPool on the write side: readLeg borrows one per inbound
	// chunk instead of allocating a fresh buffer for every 16KB chunk, and Read
	// hands it back once the chunk has been fully consumed. A sustained striped
	// download then reuses a small set of buffers instead of churning one per
	// chunk through the garbage collector.
	readPool sync.Pool

	errMu   sync.Mutex
	permErr error // sticky once set: every Read/Write after this returns it

	// writeBroken is set (via AbortWrite) when the byte stream feeding Write was
	// truncated by an upstream error, so the chunks queued so far are NOT a
	// complete stream. Close consults it to decide whether the end-of-stream
	// marker may be sent: emitting it on a truncated stream would tell the peer
	// the data ended cleanly at the truncation point - silent loss.
	writeBroken int32

	chunkCh chan chunk
	errCh   chan error

	legsActive int32         // read workers still delivering; decremented on each leg's clean EOF
	legsDone   chan struct{} // closed once every leg has ended cleanly

	total      uint32        // total data-chunk count, announced by the END marker
	haveTotal  int32         // atomic bool: set once the END marker has been seen
	totalKnown chan struct{} // closed when the END marker arrives, to wake a blocked Read
	totalOnce  sync.Once

	closeOnce sync.Once
	closed    chan struct{}
}

// New wraps legs (already-connected, already correlated to the same logical
// flow on both ends) into a single striped net.Conn.
func New(legs []net.Conn, chunkSize int) *Conn {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	c := &Conn{
		legs:         legs,
		chunkSize:    chunkSize,
		stallTimeout: defaultStallTimeout,
		pending:      make(map[uint32]chunk),
		// Deeper channels than the old len(legs)*{2,4}: extra headroom lets the
		// fast legs run further ahead of a slow one (their chunks buffer here and
		// in the reorder map instead of back-pressuring), which is what turns a
		// heterogeneous set of legs into their summed throughput rather than the
		// slowest leg's rate. The reorder buffer (pending) is bounded by the
		// reassembly budget; the channel depth only bounds how far ahead a leg
		// races before its readLeg parks.
		budget:      reassemblyBudget{limit: reassemblyBudgetBytes},
		errCh:       make(chan error, len(legs)),
		writeQueue:  make(chan writeJob, len(legs)*8),
		resendQueue: make(chan writeJob, max(len(legs)*2, 1)),
		flush:       make(chan struct{}),
		legsActive:  int32(len(legs)),
		legsDone:    make(chan struct{}),
		totalKnown:  make(chan struct{}),
		closed:      make(chan struct{}),
		parkAt:      make([]uint64, len(legs)),
		legWaited:   make([]uint32, len(legs)),
		parkCh:      make(chan struct{}, 1),
		sched:       make([]legSched, len(legs)),
		legRead:     make([]int64, len(legs)),
	}
	c.chunkCh = make(chan chunk, c.budget.depth(len(legs)*8, c.chunkCost(true)))
	c.chunkPool.New = func() any {
		b := make([]byte, chunkSize)
		return &b
	}
	c.readPool.New = func() any {
		b := make([]byte, chunkSize)
		return &b
	}
	for i, leg := range legs {
		c.writersWG.Add(1)
		go c.readLeg(i, leg)
		go c.writeLeg(i, leg)
	}
	go c.rerouteWatchdog()
	go c.windowLoop()
	return c
}

func (c *Conn) readLeg(i int, leg net.Conn) {
	HoldLeg(leg)
	header := make([]byte, headerSize)
	for {
		if _, err := io.ReadFull(leg, header); err != nil {
			// io.EOF exactly at a chunk boundary is this leg finishing
			// cleanly; anything else (ErrUnexpectedEOF mid-header, a
			// transport error) is real truncation and fails the whole Conn.
			if err == io.EOF {
				c.legDone()
			} else {
				c.fail(err)
			}
			return
		}
		seq := binary.BigEndian.Uint32(header[0:4])
		length := binary.BigEndian.Uint32(header[4:8])

		if length == endMarkerLen {
			// End of stream: seq carries the total data-chunk count. No payload
			// follows; the sender closes this leg next, so the following read
			// EOFs into legDone.
			c.setTotal(seq)
			continue
		}

		// A real chunk is never larger than chunkSize (the sender slices Write
		// to exactly that). A length beyond it can only be a corrupt or hostile
		// header, so refuse to allocate for it instead of trusting the wire and
		// make()ing a multi-gigabyte buffer (OOM / DoS).
		if int(length) > c.chunkSize {
			c.fail(fmt.Errorf("striping: chunk length %d exceeds max %d", length, c.chunkSize))
			return
		}

		// Reserve before retaining: from here until Read consumes or drops the
		// chunk, its payload counts against the reassembly budget.
		cost := c.chunkCost(length > 0)
		if !c.admit(i, seq, cost) {
			return
		}

		// Borrow a payload buffer from readPool instead of allocating one per
		// chunk. A zero-length chunk carries no payload, so it borrows nothing.
		ch := chunk{seq: seq}
		if length > 0 {
			bufp := c.readPool.Get().(*[]byte)
			ch.data = (*bufp)[:length]
			ch.base = bufp
			if _, err := io.ReadFull(leg, ch.data); err != nil {
				// Never delivered: recycle immediately so a mid-chunk read error
				// doesn't drop the buffer out of the pool.
				c.readPool.Put(bufp)
				c.budget.release(cost)
				// The header promised length bytes, so EOF with none of them
				// is truncation (ReadFull returns bare io.EOF only then).
				if err == io.EOF {
					err = io.ErrUnexpectedEOF
				}
				c.fail(err)
				return
			}
		}

		atomic.AddInt64(&c.legRead[i], int64(length))
		select {
		case c.chunkCh <- ch:
		case <-c.closed:
			// The chunk never reached Read, so nothing else will return its
			// buffer - hand it back here instead of leaking it from the pool.
			c.recycle(ch)
			return
		}
	}
}

// parkPoll is how often a parked leg reader looks for reassembly room again.
// ponytail: a poll, at most this much added latency per pause; wake parked
// readers from release() if it ever shows up in a profile.
const parkPoll = 2 * time.Millisecond

// admit charges the reassembly budget for chunk seq, which leg i is about to
// read. When the budget is full the leg waits here, its payload still in the
// transport, so its sender is held back by the transport's own flow control:
// the legs that ran ahead pause until the one carrying the sequence Read is
// waiting for catches up, instead of the flow dying of a leg that fell behind
// for a moment.
//
// Waiting cannot keep that sequence out. A leg sends its chunks in order, so the
// leg carrying it is never one holding a later chunk, and the chunk itself is
// admitted whatever is retained. If every leg is holding a later chunk it can no
// longer arrive at all, and Read fails the flow (see the parkCh case there).
// admit returns false if the Conn ended first.
func (c *Conn) admit(i int, seq uint32, cost int64) bool {
	if c.budget.reserve(cost) {
		return true
	}
	if cost > c.budget.limit {
		c.fail(c.budget.err(cost))
		return false
	}
	atomic.StoreUint32(&c.legWaited[i], 1)
	atomic.StoreUint64(&c.parkAt[i], uint64(seq)+1)
	defer atomic.StoreUint64(&c.parkAt[i], 0)
	select {
	case c.parkCh <- struct{}{}:
	default:
	}
	t := time.NewTicker(parkPoll)
	defer t.Stop()
	for {
		if seq <= atomic.LoadUint32(&c.nextSeqSeen) {
			c.budget.force(cost)
			return true
		}
		if c.budget.reserve(cost) {
			return true
		}
		select {
		case <-c.closed:
			return false
		case <-t.C:
		}
	}
}

// allParkedPast reports whether every leg still delivering is waiting for room
// with a chunk later than next. Caller holds rmu.
func (c *Conn) allParkedPast(next uint32) bool {
	n := int32(0)
	for i := range c.parkAt {
		if at := atomic.LoadUint64(&c.parkAt[i]); at > uint64(next)+1 {
			n++
		}
	}
	return n > 0 && n >= atomic.LoadInt32(&c.legsActive)
}

// advance moves Read on to the next sequence. Caller holds rmu.
func (c *Conn) advance() {
	c.nextSeq++
	atomic.StoreUint32(&c.nextSeqSeen, c.nextSeq)
}

// setTotal records the announced total chunk count and wakes any blocked Read
// so it can re-check completion. Idempotent: every leg carries the marker.
func (c *Conn) setTotal(total uint32) {
	atomic.StoreUint32(&c.total, total)
	atomic.StoreInt32(&c.haveTotal, 1)
	c.totalOnce.Do(func() { close(c.totalKnown) })
}

// complete reports whether the whole stream has been reassembled: the total is
// known and every chunk up to it has been delivered in order. Caller holds rmu.
func (c *Conn) complete() bool {
	return atomic.LoadInt32(&c.haveTotal) == 1 && c.nextSeq >= atomic.LoadUint32(&c.total)
}

// legDone records one leg finishing cleanly. When the last one does, every
// leg's read half has hit EOF - the peer closed the striped Conn - so we both
// signal Read (via legsDone, so it can report a clean io.EOF once it has
// drained what's buffered) and tear the Conn down. The teardown matters for
// the direction that is only ever writing: without a Read call to observe
// legsDone, its write workers would otherwise sit blocked on an empty queue
// forever instead of noticing the peer is gone. Teardown only closes the leg
// sockets and signals closed; the chunks already handed to chunkCh stay in
// memory for Read to finish reassembling.
func (c *Conn) legDone() {
	if atomic.AddInt32(&c.legsActive, -1) == 0 {
		close(c.legsDone)
		// Close, not teardown: go through the graceful path so any write
		// still in flight on the other direction finishes before the legs are
		// closed. A bare teardown here would close a leg out from under a
		// writeLeg mid-send, dropping the tail of the stream it was writing.
		// Run it in its own goroutine so this readLeg doesn't block on the
		// write workers draining.
		go c.Close()
	}
}

// writeLeg is a worker that pulls the next queued chunk - whichever one is
// available first - and sends it on this leg. A leg that's draining fast
// comes back for more sooner, so faster/less congested legs naturally end
// up carrying more of the flow instead of every leg getting a fixed,
// round-robined share regardless of how quickly it can move it.
//
// Pacing is left to each leg's own transport (smux stream flow control over
// TCP congestion control): a congested leg simply blocks in Write until its
// window opens, so it takes fewer chunks off the shared queue. An earlier
// version added a global in-flight cap across all legs on top of this, to
// keep the ensemble from hammering a shared bottleneck - but with in-order
// reassembly on the far end it could deadlock: the cap could starve the exact
// leg carrying the sequence number the reader was blocked on, and that
// reader's stall was in turn what kept the in-flight slot from freeing.
// Per-leg backpressure alone can't form that cycle, because every leg can
// always make progress independently.
func (c *Conn) writeLeg(i int, leg net.Conn) {
	defer c.writersWG.Done()
	// One reusable frame buffer per worker: header + a full-size payload. Chunks
	// are framed into it and sent in a single Write (see writeChunk), so the leg
	// transport sees one frame per chunk instead of two.
	frame := make([]byte, headerSize+c.chunkSize)
	for {
		// Reroute work first (a stalled sequence a healthy leg must carry now),
		// then, preferring to drain a queued chunk before considering exit -
		// which is what keeps a graceful Close lossless even when a teardown
		// races it: a worker only stops once the queues are genuinely empty.
		select {
		case job := <-c.resendQueue:
			if !c.writeChunkTracked(i, leg, frame, job) {
				return
			}
			continue
		case job := <-c.writeQueue:
			if !c.writeChunkTracked(i, leg, frame, job) {
				return
			}
			continue
		default:
		}

		select {
		case job := <-c.resendQueue:
			if !c.writeChunkTracked(i, leg, frame, job) {
				return
			}
		case job := <-c.writeQueue:
			if !c.writeChunkTracked(i, leg, frame, job) {
				return
			}
		case <-c.flush:
			c.flushRemaining(i, leg, frame)
			return
		case <-c.closed:
			return
		}
	}
}

// flushRemaining drains both queues on a graceful close. Draining writeQueue to
// empty is what makes Close lossless: every chunk Write() enqueued is sent by
// some worker before it exits. Resends are duplicates (deduped by the receiver),
// so draining them is best-effort - losing one never loses data.
func (c *Conn) flushRemaining(i int, leg net.Conn, frame []byte) {
	for {
		select {
		case job := <-c.resendQueue:
			if !c.writeChunkTracked(i, leg, frame, job) {
				return
			}
		default:
			select {
			case job := <-c.writeQueue:
				if !c.writeChunkTracked(i, leg, frame, job) {
					return
				}
			default:
				return
			}
		}
	}
}

// writeChunkTracked sends one chunk on leg i, recording it as in-flight (so the
// watchdog can reroute it if the leg stalls) and returning the borrowed pool
// buffer. It returns false (after failing the whole Conn) if the leg errors.
func (c *Conn) writeChunkTracked(i int, leg net.Conn, frame []byte, job writeJob) bool {
	c.beginWrite(i, job)
	ok := c.writeChunk(leg, frame, job)
	c.endWrite(i)
	// A resend job carries a fresh copy (buf == nil), not a pooled buffer.
	if job.buf != nil {
		c.chunkPool.Put(job.buf)
	}
	return ok
}

// beginWrite records leg i's chunk as in-flight, so the watchdog can reroute its
// sequence number onto a healthy leg if this one freezes. It stores a reference
// to the payload (not a copy); the buffer stays valid until endWrite runs, and
// the watchdog only reads it under schedMu while inFly is set.
func (c *Conn) beginWrite(i int, job writeJob) {
	c.schedMu.Lock()
	s := &c.sched[i]
	s.inFly = true
	s.inSeq = job.seq
	s.inData = job.data
	s.inStart = time.Now()
	s.inResent = false
	c.schedMu.Unlock()
}

func (c *Conn) endWrite(i int) {
	c.schedMu.Lock()
	s := &c.sched[i]
	s.inFly = false
	s.inData = nil
	c.schedMu.Unlock()
}

// rerouteWatchdog periodically re-sends a chunk that a leg has stalled on: its
// sequence number is copied onto the priority resend queue for another leg to
// carry, so the reader isn't stalled waiting out the stuck one. It stops on a
// graceful close (originals are flushed anyway) or teardown.
func (c *Conn) rerouteWatchdog() {
	t := time.NewTicker(healthTick)
	defer t.Stop()
	for {
		select {
		case <-c.closed:
			return
		case <-c.flush:
			return
		case <-t.C:
			c.scanStuck()
		}
	}
}

// scanStuck finds chunks in flight longer than resendTimeout and re-queues their
// sequence numbers (with a fresh copy of the payload) for another leg. The
// receiver dedups by sequence number, so a re-send that races the original is
// harmless.
func (c *Conn) scanStuck() {
	now := time.Now()
	var resends []writeJob
	c.schedMu.Lock()
	for i := range c.sched {
		s := &c.sched[i]
		if s.inFly && !s.inResent && now.Sub(s.inStart) > resendTimeout {
			data := make([]byte, len(s.inData))
			copy(data, s.inData)
			resends = append(resends, writeJob{seq: s.inSeq, data: data})
			s.inResent = true
		}
	}
	c.schedMu.Unlock()

	for _, job := range resends {
		// Non-lossy: the leg this chunk is stuck on has stalled, so the original
		// write may never deliver - dropping the reroute would leave the reader
		// stalled on this sequence number until stallTimeout tears the whole flow
		// down (which is exactly the mid-session disconnect this path exists to
		// prevent). Block until another leg can take it; only a teardown
		// (c.closed) lets us give up, and by then the flow is already ending.
		select {
		case c.resendQueue <- job:
		case <-c.closed:
			return
		}
	}
}

// writeChunk frames and sends one chunk on a leg, returning false (after
// failing the whole Conn) if the leg errors. Header and payload are packed into
// a single buffer and sent with one Write so the leg's transport (an smux
// stream) frames the chunk once instead of twice - fewer syscalls, and the
// header can't interleave with another chunk's payload under load.
func (c *Conn) writeChunk(leg net.Conn, frame []byte, job writeJob) bool {
	if err := leg.SetWriteDeadline(time.Now().Add(c.stallTimeout)); err != nil {
		c.fail(err)
		return false
	}
	binary.BigEndian.PutUint32(frame[0:4], job.seq)
	binary.BigEndian.PutUint32(frame[4:8], uint32(len(job.data)))
	n := copy(frame[headerSize:], job.data)
	if _, err := leg.Write(frame[:headerSize+n]); err != nil {
		c.fail(err)
		return false
	}
	return true
}

// fail records the first error seen (from either direction) and tears the
// whole striped connection down - abruptly, without the graceful write flush
// Close does, since a leg has already broken. Without this, one leg going
// quietly dead (no error, just never progressing) would leave Read blocked
// forever and the streams/goroutines behind the other legs never released.
//
// permErr is set here directly rather than only inside Read's error handling:
// a write-side failure (a leg's write deadline expiring) has to be visible to
// the next Write call even if nothing ever calls Read on this Conn.
func (c *Conn) fail(err error) {
	// No caller signals a normal end through fail (a clean leg finish goes via
	// legDone), so a bare io.EOF here would surface as a silent clean EOF.
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	c.setPermErr(err)
	select {
	case c.errCh <- err:
	default:
	}
	c.teardown()
}

// setPermErr records the first permanent error seen, from either Read or
// Write's side of the connection.
func (c *Conn) setPermErr(err error) {
	c.errMu.Lock()
	if c.permErr == nil {
		c.permErr = err
	}
	c.errMu.Unlock()
}

func (c *Conn) getPermErr() error {
	c.errMu.Lock()
	defer c.errMu.Unlock()
	return c.permErr
}

// Write slices p into chunkSize pieces, each tagged with a global sequence
// number, and queues them for whichever leg picks them up first.
func (c *Conn) Write(p []byte) (int, error) {
	if err := c.getPermErr(); err != nil {
		return 0, err
	}
	// A torn-down Conn (e.g. the peer closed every leg, tripping legDone)
	// isn't necessarily carrying a permErr, but writes to it must still fail
	// rather than pile up in a queue no worker will ever drain.
	select {
	case <-c.closed:
		if err := c.getPermErr(); err != nil {
			return 0, err
		}
		return 0, net.ErrClosed
	default:
	}

	c.wmu.Lock()
	defer c.wmu.Unlock()

	total := 0
	for len(p) > 0 {
		n := len(p)
		if n > c.chunkSize {
			n = c.chunkSize
		}
		bufp := c.chunkPool.Get().(*[]byte)
		data := (*bufp)[:n]
		copy(data, p[:n])
		p = p[n:]

		seq := c.writeSeq
		c.writeSeq++

		select {
		case c.writeQueue <- writeJob{seq: seq, data: data, buf: bufp}:
			total += n
		case <-c.closed:
			// This chunk never entered the queue, so no worker will return its
			// buffer - hand it back here instead of leaking it from the pool.
			c.chunkPool.Put(bufp)
			return total, c.currentErr()
		case <-c.flush:
			// CloseWrite closes flush only on write-side shutdown, after which
			// the write workers exit and nothing drains writeQueue; without
			// this a Write blocked on a full queue holds wmu forever and
			// deadlocks CloseWrite's sendEndMarkers.
			c.chunkPool.Put(bufp)
			if err := c.getPermErr(); err != nil {
				return total, err
			}
			return total, net.ErrClosed
		}
	}
	return total, nil
}

// stash files a chunk: if it's the next byte in line it becomes the read
// buffer, otherwise it waits in pending until its turn comes. A duplicate (a
// sequence number already delivered or already waiting - which the reroute path
// can produce when a re-send races the original) is dropped and its pooled
// buffer recycled, so it can neither corrupt the stream nor be delivered twice.
func (c *Conn) stash(ch chunk) {
	if ch.seq < c.nextSeq {
		c.recycle(ch)
		return
	}
	if ch.seq == c.nextSeq {
		c.setReadBuf(ch)
		c.advance()
	} else if _, dup := c.pending[ch.seq]; dup {
		c.recycle(ch)
	} else {
		ch.at = time.Now()
		c.pending[ch.seq] = ch
	}
}

// chunkCost is the budget charge for one retained chunk: its pooled payload
// buffer (full chunkSize, whatever the wire length) plus fixed entry overhead.
func (c *Conn) chunkCost(hasPayload bool) int64 {
	n := int64(retainedEntryOverhead)
	if hasPayload {
		n += int64(c.chunkSize)
	}
	return n
}

// recycle returns a dropped chunk's pooled backing buffer to readPool and
// releases its budget charge. A zero-length chunk borrows no buffer.
func (c *Conn) recycle(ch chunk) {
	if ch.base != nil {
		c.readPool.Put(ch.base)
	}
	c.budget.release(c.chunkCost(ch.base != nil))
}

// setReadBuf installs ch as the current read buffer and remembers its pooled
// backing buffer so releaseReadBuf can return it once the bytes are consumed.
// readBuf is only ever replaced when empty (all call sites are guarded by a
// len(readBuf)==0 check), so any previous buffer has already been released.
// Caller holds rmu.
func (c *Conn) setReadBuf(ch chunk) {
	c.readBuf = ch.data
	c.readBufBase = ch.base
	c.readCost = c.chunkCost(ch.base != nil)
	if len(ch.data) == 0 {
		c.releaseReadBuf() // nothing to consume: Read never drains an empty buffer
	}
}

// releaseReadBuf returns the fully-consumed read buffer's pooled backing buffer
// to readPool. Called under rmu once readBuf has drained to empty; niling the
// reference makes it safe against a double return.
func (c *Conn) releaseReadBuf() {
	if c.readBufBase != nil {
		c.readPool.Put(c.readBufBase)
		c.readBufBase = nil
	}
	c.budget.release(c.readCost)
	c.readCost = 0
}

// earliestPending is the first-evidence time for a new gap: the oldest
// out-of-order chunk Read is holding, or now if the evidence is only an END
// marker. Caller holds rmu.
func (c *Conn) earliestPending() time.Time {
	t := time.Now()
	for _, ch := range c.pending {
		if ch.at.Before(t) {
			t = ch.at
		}
	}
	return t
}

// dropReassembly gives back everything Read still retains once the stream has
// ended for good (EOF or a terminal error), so charges and pooled buffers are
// not stranded. Caller holds rmu.
func (c *Conn) dropReassembly() {
	c.drainAvailable()
	for seq, ch := range c.pending {
		c.recycle(ch)
		delete(c.pending, seq)
	}
	c.readBuf = nil
	c.releaseReadBuf()
}

// drainAvailable moves everything currently sitting in chunkCh into the
// pending map without blocking. Used at end-of-stream and on error to make
// sure nothing already received is dropped before deciding what to do.
//
// It deliberately files every chunk into pending rather than calling stash:
// stash promotes a next-in-line chunk straight into readBuf, so draining
// several contiguous chunks in a row would overwrite readBuf again and again,
// advancing nextSeq (counting them delivered) while silently discarding all
// but the last one's bytes. Leaving them in pending lets Read's main loop hand
// them to the caller one at a time.
func (c *Conn) drainAvailable() {
	for {
		select {
		case ch := <-c.chunkCh:
			if ch.seq < c.nextSeq {
				c.recycle(ch) // already delivered (a raced reroute)
				continue
			}
			if _, dup := c.pending[ch.seq]; dup {
				c.recycle(ch)
				continue
			}
			ch.at = time.Now()
			c.pending[ch.seq] = ch
		default:
			return
		}
	}
}

// Read reassembles chunks arriving out of order across legs into the
// original in-order byte stream. It reports a clean io.EOF only once every
// announced chunk has been delivered. An idle connection (nothing missing, so
// nothing to wait out) blocks without any timer, like a quiet TCP flow. A
// proven gap - a later sequence arrived, or the END marker announced more than
// has been delivered - must be filled within stallTimeout of when it was first
// observed; later arrivals do not renew that deadline. Otherwise Read fails
// loudly (like a dropped connection, since there's no retransmission).
func (c *Conn) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	c.rmu.Lock()
	defer c.rmu.Unlock()
	// Once Read reports EOF or a terminal error nothing retained can ever be
	// delivered: release it.
	defer func() {
		if err != nil {
			c.dropReassembly()
		}
	}()

	if len(c.readBuf) == 0 {
		// A completed stream reports EOF even if a leg later errored - every
		// byte was delivered, so it's a clean end.
		if c.complete() {
			return 0, io.EOF
		}
		if err := c.getPermErr(); err != nil {
			// Deliver anything already reassembled and contiguously
			// available before surfacing the sticky error.
			if ch, ok := c.pending[c.nextSeq]; ok {
				delete(c.pending, c.nextSeq)
				c.setReadBuf(ch)
				c.advance()
			} else {
				return 0, err
			}
		}
	}

	for len(c.readBuf) == 0 {
		// Completion is defined by the sender's announced total, not by leg
		// EOF timing: once every chunk up to the total has been delivered in
		// order, the stream is done. This is what stops the tail of a stream
		// slipping through as a short read when a leg's EOF races the last
		// few chunks still sitting in chunkCh.
		if c.complete() {
			return 0, io.EOF
		}
		if ch, ok := c.pending[c.nextSeq]; ok {
			delete(c.pending, c.nextSeq)
			c.setReadBuf(ch)
			c.advance()
			continue
		}

		// Only arm the totalKnown case while the END marker hasn't been seen.
		// totalKnown is closed (never reopened) once the marker arrives, so
		// leaving it in the select would make it win every iteration - a busy
		// spin that pegs a core and starves the gap timer whenever the marker
		// arrives on a fast leg before a chunk still in flight on a slow one. A
		// nil channel never selects, so afterwards we fall through to the real
		// work (chunkCh / legsDone / errCh / timer).
		var totalKnown <-chan struct{}
		if atomic.LoadInt32(&c.haveTotal) == 0 {
			totalKnown = c.totalKnown
		}

		// Time the wait only if data is provably missing: a later sequence is
		// held, or END announced more than we've delivered. The deadline is
		// absolute from when the gap was first observed, so a stream of
		// out-of-order arrivals cannot postpone it.
		proven := len(c.pending) > 0 ||
			(atomic.LoadInt32(&c.haveTotal) == 1 && c.nextSeq < atomic.LoadUint32(&c.total))
		wait := time.Duration(-1)
		if !proven {
			c.gap.clear()
		} else {
			if c.gap.fresh(c.nextSeq) {
				c.gap.begin(c.nextSeq, c.earliestPending())
			}
			wait = max(c.stallTimeout-time.Since(c.gap.start), 0)
		}
		timerC := armTimer(&c.readTimer, wait)

		select {
		case ch := <-c.chunkCh:
			c.stash(ch)

		case <-totalKnown:
			// The total just became known; loop to re-check completion and
			// drain whatever is buffered.
			c.drainAvailable()

		case <-c.legsDone:
			// Every leg ended. Absorb anything still buffered; if that
			// completes the stream we'll report EOF on the next loop, otherwise
			// a byte range no leg carried is genuinely lost.
			c.drainAvailable()
			if len(c.readBuf) == 0 && !c.complete() {
				if _, ok := c.pending[c.nextSeq]; !ok {
					err := io.ErrUnexpectedEOF
					c.setPermErr(err)
					c.teardown()
					return 0, err
				}
			}

		case err := <-c.errCh:
			c.setPermErr(err)
			// A leg erroring doesn't necessarily mean the chunk we're waiting
			// on is lost - absorb whatever is already queued before giving up.
			c.drainAvailable()
			if len(c.readBuf) == 0 && !c.complete() {
				if _, ok := c.pending[c.nextSeq]; !ok {
					return 0, c.getPermErr()
				}
			}

		case <-c.closed:
			// Closed locally (or torn down by a failure whose error was already
			// consumed): a Read blocked on an idle connection must not hang.
			c.drainAvailable()
			if len(c.readBuf) == 0 && !c.complete() {
				if _, ok := c.pending[c.nextSeq]; !ok {
					if err := c.getPermErr(); err != nil {
						return 0, err
					}
					select {
					case <-c.legsDone:
						return 0, io.ErrUnexpectedEOF
					default:
						return 0, net.ErrClosed
					}
				}
			}

		case <-c.parkCh:
			// A leg is waiting for room. If they all are, each holds a chunk
			// later than the one awaited, and a leg sends in order: it can no
			// longer arrive, and nothing would ever make room.
			//
			// The legs are looked at first and the queue after. A leg hands over
			// a chunk before it reads, and waits on, the next one; so whatever a
			// leg seen waiting has delivered is in the queue by now - including
			// the awaited chunk, if it was the last thing that leg did before it
			// stopped. Looked at the other way round, that chunk could arrive
			// between the two looks and a flow with nothing missing be failed.
			stuck := c.allParkedPast(c.nextSeq)
			c.drainAvailable()
			if _, ok := c.pending[c.nextSeq]; ok {
				continue
			}
			if stuck {
				err := c.budget.err(c.chunkCost(true))
				c.setPermErr(err)
				c.teardown()
				return 0, err
			}

		case <-timerC:
			// The chunk we're waiting on may have landed in chunkCh in the same
			// instant; only a still-missing sequence is a stall.
			c.drainAvailable()
			if _, ok := c.pending[c.nextSeq]; ok {
				continue
			}
			stallErr := fmt.Errorf("striping: stalled waiting for sequence %d for %s", c.nextSeq, c.stallTimeout)
			c.setPermErr(stallErr)
			c.teardown()
			return 0, stallErr
		}
	}

	n = copy(p, c.readBuf)
	c.readBuf = c.readBuf[n:]
	// Once the chunk's bytes are fully copied out to the caller, its pooled
	// backing buffer is free to be reused by the next inbound chunk.
	if len(c.readBuf) == 0 {
		c.releaseReadBuf()
	}
	return n, nil
}

// currentErr returns the first recorded leg error, or a generic closed
// error if the Conn was closed locally (e.g. via Close()) without one.
func (c *Conn) currentErr() error {
	select {
	case err := <-c.errCh:
		return err
	default:
		return net.ErrClosed
	}
}

// AbortWrite records that the byte stream feeding Write was truncated by an
// upstream error - the data queued so far is not a complete stream. A
// subsequent Close therefore skips the end-of-stream marker, so the peer's Read
// reports io.ErrUnexpectedEOF (a loud, dropped-connection-style failure)
// instead of a clean io.EOF that would silently pass the truncated stream off
// as complete. The caller still calls Close to flush and tear down; AbortWrite
// only records intent and is safe to call concurrently with, or before, Close.
func (c *Conn) AbortWrite() {
	atomic.StoreInt32(&c.writeBroken, 1)
}

// CloseWrite flushes queued writes to the legs before sending EOF.
func (c *Conn) CloseWrite() error {
	c.flushOnce.Do(func() {
		close(c.flush)
		done := make(chan struct{})
		go func() {
			c.writersWG.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(c.stallTimeout):
		}
		// The end-of-stream marker certifies "the complete stream is exactly
		// writeSeq chunks". Emit it only when the outbound data really is
		// complete: if the write source was truncated (AbortWrite) or a leg has
		// already failed, writeSeq is a partial count, and announcing it would
		// tell the peer the stream ended cleanly at the truncation point -
		// silent data loss. Skipping it leaves the peer's Read to report
		// io.ErrUnexpectedEOF when the legs close short, honouring this package's
		// lossless-or-loud contract.
		if atomic.LoadInt32(&c.writeBroken) == 0 && c.getPermErr() == nil {
			c.sendEndMarkers()
		}
	})
	return nil
}

// Close flushes queued writes to the legs before tearing them down, so the
// tail of the stream isn't dropped when Close follows the last Write. A
// broken leg takes the abrupt path via fail instead.
func (c *Conn) Close() error {
	_ = c.CloseWrite()
	return c.teardown()
}

// sendEndMarkers writes the end-of-stream marker (carrying the total data-chunk
// count) on every leg. Called after the write workers have exited, so there is
// no concurrent writer on any leg. Best-effort: a leg that errors is left to
// teardown, and the peer only needs the marker on one surviving leg.
func (c *Conn) sendEndMarkers() {
	c.wmu.Lock()
	total := c.writeSeq
	c.wmu.Unlock()

	var header [headerSize]byte
	binary.BigEndian.PutUint32(header[0:4], total)
	binary.BigEndian.PutUint32(header[4:8], endMarkerLen)
	for _, leg := range c.legs {
		_ = leg.SetWriteDeadline(time.Now().Add(c.stallTimeout))
		_, _ = leg.Write(header[:])
	}
}

// teardown signals every goroutine to stop and closes all legs. Idempotent;
// shared by Close (after its flush) and fail (immediately).
func (c *Conn) teardown() error {
	var err error
	c.closeOnce.Do(func() {
		close(c.closed)
		for _, leg := range c.legs {
			if e := leg.Close(); e != nil {
				err = e
			}
		}
	})
	return err
}

func (c *Conn) LocalAddr() net.Addr  { return c.legs[0].LocalAddr() }
func (c *Conn) RemoteAddr() net.Addr { return c.legs[0].RemoteAddr() }

func (c *Conn) SetDeadline(t time.Time) error {
	return c.forEachLeg(func(l net.Conn) error { return l.SetDeadline(t) })
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	return c.forEachLeg(func(l net.Conn) error { return l.SetReadDeadline(t) })
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	return c.forEachLeg(func(l net.Conn) error { return l.SetWriteDeadline(t) })
}

func (c *Conn) forEachLeg(f func(net.Conn) error) error {
	var firstErr error
	for _, leg := range c.legs {
		if e := f(leg); e != nil && firstErr == nil {
			firstErr = fmt.Errorf("leg error: %w", e)
		}
	}
	return firstErr
}
