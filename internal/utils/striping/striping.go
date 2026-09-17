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

// Adaptive scheduling tunables. In-order reassembly makes a striped flow only
// as fast as its slowest leg can deliver the sequence number the reader is
// waiting on: a single throttled CDN leg (a common asymmetry on the egress
// direction) drags the whole flow down to its rate even though the other legs
// are idle. These make the striper route around such a leg the way plain
// per-connection load-balancing naturally does.
const (
	// legEWMAAlpha weights the newest per-write throughput sample into each
	// leg's running estimate. High enough to react to a leg that suddenly
	// throttles within a few chunks, low enough not to flap on one slow write.
	legEWMAAlpha = 0.3
	// quarantineFraction is the share of the fastest leg's throughput below
	// which a leg is quarantined - it stops being handed new sequential chunks
	// so it can't become a head-of-line stall point. A leg carrying a quarter of
	// the best leg's rate hurts more (as a reorder-buffer gate) than it helps.
	quarantineFraction = 0.25
	// quarantineProbeEvery is how often a quarantined leg is allowed to take one
	// chunk again, so a leg whose throttling lifted can re-measure and rejoin.
	quarantineProbeEvery = 500 * time.Millisecond
	// resendTimeout is how long a chunk may sit in flight on one leg before the
	// same sequence number is re-sent on a healthy leg (the receiver dedups by
	// seq). Only trips for a near-stalled leg - a merely slow leg moves a chunk
	// in microseconds - so reroute costs a little duplicate bandwidth only when a
	// leg has actually frozen, not on every slow write.
	resendTimeout = 1500 * time.Millisecond
	// healthTick is how often the reroute watchdog scans for stuck chunks.
	healthTick = 200 * time.Millisecond
)

// legSched is the per-leg scheduling state the adaptive write path keeps, all
// guarded by Conn.schedMu. rate is the EWMA throughput estimate. A leg is held
// out of new sequential work for either of two independent reasons, tracked
// separately so one can't mask the other:
//
//   - slowQuar: a *rate* decision - the leg is persistently slower than its
//     peers AND the write queue is genuinely backlogged (a bulk transfer). This
//     is the throughput-aggregation path; it is deliberately never engaged for a
//     low-rate, bursty flow (a game, an SSH session), whose per-write rate
//     samples are noise and whose queue is near-empty, so such a flow behaves
//     exactly like the plain work-stealing striper and can't be reordered into a
//     reassembly stall.
//   - frozen: a *liveness* decision - scanStuck saw this leg stuck on one chunk
//     past resendTimeout, so its sequence number was rerouted onto a healthy leg
//     and no new work goes here until it proves alive again (a successful write
//     clears it, see recordRate). This one always applies, bulk or interactive,
//     because a frozen leg is a real problem for any flow.
//
// The in* fields track the chunk currently being written so the watchdog can
// re-send it elsewhere if the leg freezes.
type legSched struct {
	rate     float64
	slowQuar bool
	frozen   bool
	inSeq    uint32
	inData   []byte
	inFly    bool
	inStart  time.Time
	inResent bool
}

type chunk struct {
	seq  uint32
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
	// resendQueue is a priority reroute path: chunks a frozen leg is stuck on
	// are re-queued here (by the watchdog) and picked up by a healthy leg ahead
	// of new work, so a stalled sequence number reaches the reader without
	// waiting out the frozen leg.
	resendQueue chan writeJob
	// schedMu guards sched, the per-leg adaptive scheduling state.
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
	pending map[uint32]chunk
	readBuf []byte
	// readBufBase is the pooled backing buffer for readBuf. It's returned to
	// readPool once readBuf drains to empty, so the next inbound chunk can reuse
	// it. Guarded by rmu, like the rest of the reassembly state.
	readBufBase *[]byte
	// readTimer is the reusable stall timer for Read's blocking loop, created
	// lazily on first use and re-armed with Reset on every turn instead of a
	// fresh time.NewTimer per turn. Guarded by rmu (Read holds it for its whole
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
		// slowest leg's rate. The reorder buffer (pending) is unbounded, so this
		// only bounds how far ahead a leg races before its readLeg parks.
		chunkCh:     make(chan chunk, len(legs)*8),
		errCh:       make(chan error, len(legs)),
		writeQueue:  make(chan writeJob, len(legs)*8),
		resendQueue: make(chan writeJob, max(len(legs)*2, 1)),
		flush:       make(chan struct{}),
		legsActive:  int32(len(legs)),
		legsDone:    make(chan struct{}),
		totalKnown:  make(chan struct{}),
		closed:      make(chan struct{}),
		sched:       make([]legSched, len(legs)),
	}
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
		go c.readLeg(leg)
		go c.writeLeg(i, leg)
	}
	go c.rerouteWatchdog()
	return c
}

func (c *Conn) readLeg(leg net.Conn) {
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
				c.fail(err)
				return
			}
		}

		select {
		case c.chunkCh <- ch:
		case <-c.closed:
			// The chunk never reached Read, so nothing else will return its
			// buffer - hand it back here instead of leaking it from the pool.
			if ch.base != nil {
				c.readPool.Put(ch.base)
			}
			return
		}
	}
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
		if c.quarantined(i) {
			// A quarantined leg stays out of the *sequential* path so it can't
			// gate the reorder buffer - but it still services the reroute queue.
			// A reroute is a stalled sequence number that needs any live carrier
			// now; when the leg holding the original is frozen, a quarantined but
			// alive leg is exactly the leg that must move it (otherwise, in a
			// two-leg flow where the fast leg froze, the reroute would have no
			// worker at all). It also honours a graceful close and periodically
			// takes one normal chunk to re-measure so a recovered leg can rejoin.
			select {
			case <-c.flush:
				c.flushRemaining(i, leg, frame)
				return
			case <-c.closed:
				return
			case job := <-c.resendQueue:
				if !c.writeChunkTracked(i, leg, frame, job) {
					return
				}
			case <-time.After(quarantineProbeEvery):
				select {
				case job := <-c.writeQueue:
					if !c.writeChunkTracked(i, leg, frame, job) {
						return
					}
				default:
				}
			}
			continue
		}

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
// watchdog can reroute it if the leg freezes), timing it to keep the leg's
// throughput estimate fresh, and returning the borrowed pool buffer. It returns
// false (after failing the whole Conn) if the leg errors.
func (c *Conn) writeChunkTracked(i int, leg net.Conn, frame []byte, job writeJob) bool {
	c.beginWrite(i, job)
	start := time.Now()
	ok := c.writeChunk(leg, frame, job)
	elapsed := time.Since(start)
	c.endWrite(i)
	// A resend job carries a fresh copy (buf == nil), not a pooled buffer.
	if job.buf != nil {
		c.chunkPool.Put(job.buf)
	}
	if ok {
		c.recordRate(i, len(job.data), elapsed)
	}
	return ok
}

// quarantined reports whether leg i is currently held out of the sequential
// write path, for either reason (rate-slow while backlogged, or frozen).
func (c *Conn) quarantined(i int) bool {
	c.schedMu.Lock()
	defer c.schedMu.Unlock()
	return c.sched[i].slowQuar || c.sched[i].frozen
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

// recordRate folds one write's throughput into leg i's EWMA estimate and
// re-evaluates which legs are quarantined. A completed write is also proof the
// leg is alive, so it clears any frozen mark scanStuck had set: this is the
// liveness-based recovery path, independent of the rate estimate, so a leg that
// froze and then came back rejoins even for a low-rate flow that never triggers
// the rate path at all.
func (c *Conn) recordRate(i, n int, elapsed time.Duration) {
	if elapsed <= 0 {
		elapsed = time.Microsecond
	}
	inst := float64(n) / elapsed.Seconds()
	c.schedMu.Lock()
	s := &c.sched[i]
	s.frozen = false // a successful write means this leg is not frozen
	if s.rate == 0 {
		s.rate = inst
	} else {
		s.rate = legEWMAAlpha*inst + (1-legEWMAAlpha)*s.rate
	}
	c.reevaluateLocked()
	c.schedMu.Unlock()
}

// reevaluateLocked manages the *rate* quarantine (slowQuar) only; the frozen
// mark is owned by scanStuck/recordRate. It engages at all only when the write
// queue is genuinely backlogged - i.e. Write is outpacing the legs, the signature
// of a bulk transfer where routing a slow leg out of the sequential path actually
// raises aggregate throughput. A low-rate, bursty flow (a game, an SSH session)
// keeps the queue near-empty and its per-write rate samples are meaningless, so
// it must never be reordered by this path: when the queue is not backlogged every
// slowQuar mark is cleared and the flow runs as plain work-stealing. Caller holds
// schedMu.
func (c *Conn) reevaluateLocked() {
	// Not backlogged: this is not a bulk transfer (or the burst already
	// drained). Drop all rate quarantines and leave the flow work-stealing.
	if len(c.writeQueue) < cap(c.writeQueue)/2 {
		for i := range c.sched {
			c.sched[i].slowQuar = false
		}
		c.ensureActiveLocked()
		return
	}

	best := 0.0
	for i := range c.sched {
		if c.sched[i].rate > best {
			best = c.sched[i].rate
		}
	}
	if best <= 0 {
		return
	}
	threshold := quarantineFraction * best
	for i := range c.sched {
		r := c.sched[i].rate
		c.sched[i].slowQuar = r > 0 && r < threshold
	}
	c.ensureActiveLocked()
}

// ensureActiveLocked guarantees at least one leg stays out of quarantine, so a
// transient where every leg looks slow can never leave the flow with no writer.
// It prefers to reactivate a leg that is NOT currently mid-write: a leg frozen
// in flight (the one scanStuck just quarantined) can't take new work until its
// stuck write returns, so reactivating it would leave the flow with a nominally-
// active but blocked writer while everything else stays quarantined. Only if
// every leg is in flight does it fall back to the highest-rate one. Caller holds
// schedMu.
func (c *Conn) ensureActiveLocked() {
	for i := range c.sched {
		if !c.sched[i].slowQuar && !c.sched[i].frozen {
			return // at least one active leg already
		}
	}
	best := -1
	for i := range c.sched {
		if c.sched[i].inFly {
			continue // frozen mid-write; can't service work now
		}
		if best < 0 || c.sched[i].rate > c.sched[best].rate {
			best = i
		}
	}
	if best < 0 {
		// Every leg is in flight; pick the highest-rate one anyway so the
		// invariant (at least one non-quarantined leg) still holds.
		for i := range c.sched {
			if best < 0 || c.sched[i].rate > c.sched[best].rate {
				best = i
			}
		}
	}
	if best >= 0 {
		c.sched[best].slowQuar = false
		c.sched[best].frozen = false
	}
}

// rerouteWatchdog periodically re-sends a chunk that a leg has frozen on: its
// sequence number is copied onto the priority resend queue for a healthy leg to
// carry, so the reader isn't stalled waiting out the frozen leg. It stops on a
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
// sequence numbers (with a fresh copy of the payload) for a healthy leg. The leg
// they were stuck on is quarantined so it takes no new sequential work. The
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
			s.frozen = true // clearly frozen; keep new work off it until it writes again
		}
	}
	c.ensureActiveLocked()
	c.schedMu.Unlock()

	for _, job := range resends {
		// Non-lossy: the leg this chunk was stuck on is frozen, so the original
		// write may never deliver - dropping the reroute would leave the reader
		// stalled on this sequence number until stallTimeout tears the whole flow
		// down (which is exactly the mid-session disconnect this path exists to
		// prevent). Block until a healthy leg can take it; only a teardown
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
		c.nextSeq++
	} else if _, dup := c.pending[ch.seq]; dup {
		c.recycle(ch)
	} else {
		c.pending[ch.seq] = ch
	}
}

// recycle returns a dropped chunk's pooled backing buffer to readPool. A
// zero-length chunk borrows none.
func (c *Conn) recycle(ch chunk) {
	if ch.base != nil {
		c.readPool.Put(ch.base)
	}
}

// setReadBuf installs ch as the current read buffer and remembers its pooled
// backing buffer so releaseReadBuf can return it once the bytes are consumed.
// readBuf is only ever replaced when empty (all call sites are guarded by a
// len(readBuf)==0 check), so any previous buffer has already been released.
// Caller holds rmu.
func (c *Conn) setReadBuf(ch chunk) {
	c.readBuf = ch.data
	c.readBufBase = ch.base
}

// releaseReadBuf returns the fully-consumed read buffer's pooled backing buffer
// to readPool. Called under rmu once readBuf has drained to empty; niling the
// reference makes it safe against a double return.
func (c *Conn) releaseReadBuf() {
	if c.readBufBase != nil {
		c.readPool.Put(c.readBufBase)
		c.readBufBase = nil
	}
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
			c.pending[ch.seq] = ch
		default:
			return
		}
	}
}

// Read reassembles chunks arriving out of order across legs into the
// original in-order byte stream. It reports a clean io.EOF only once every
// leg has ended and nothing is left stranded; a gap when the legs end, or a
// missing sequence number that never arrives within stallTimeout, is an
// error (io.ErrUnexpectedEOF / stall) - like a dropped connection, since
// there's no retransmission.
func (c *Conn) Read(p []byte) (int, error) {
	c.rmu.Lock()
	defer c.rmu.Unlock()

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
				c.nextSeq++
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
			c.nextSeq++
			continue
		}

		// Only arm the totalKnown case while the END marker hasn't been seen.
		// totalKnown is closed (never reopened) once the marker arrives, so
		// leaving it in the select would make it win every iteration - a busy
		// spin that pegs a core and starves the stallTimeout timer whenever the
		// marker arrives on a fast leg before a chunk still in flight on a slow
		// one. A nil channel never selects, so afterwards we fall through to the
		// real work (chunkCh / legsDone / errCh / timer).
		var totalKnown <-chan struct{}
		if atomic.LoadInt32(&c.haveTotal) == 0 {
			totalKnown = c.totalKnown
		}

		// Arm the reusable stall timer for this turn. Created once and re-armed
		// with Reset on every subsequent turn (and every subsequent Read call),
		// instead of allocating a fresh time.NewTimer per turn on the reassembly
		// hot path. Under striping, chunks routinely arrive out of order, so a
		// Read blocked on sequence N wakes once per earlier-arriving chunk and
		// loops again still waiting - each of those turns previously allocated
		// (and leaked to the GC) a new 20s timer. Stop-then-drain-then-Reset
		// re-arms without leaving a stale tick behind (the drain covers a timer
		// that fired between the previous select and this Stop), giving every
		// select the same full stallTimeout budget the per-turn timer did.
		if c.readTimer == nil {
			c.readTimer = time.NewTimer(c.stallTimeout)
		} else {
			if !c.readTimer.Stop() {
				select {
				case <-c.readTimer.C:
				default:
				}
			}
			c.readTimer.Reset(c.stallTimeout)
		}
		timer := c.readTimer

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

		case <-timer.C:
			stallErr := fmt.Errorf("striping: stalled waiting for sequence %d for %s", c.nextSeq, c.stallTimeout)
			c.setPermErr(stallErr)
			c.teardown()
			return 0, stallErr
		}
	}

	n := copy(p, c.readBuf)
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
