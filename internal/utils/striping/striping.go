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

type chunk struct {
	seq  uint32
	data []byte
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
		chunkCh:      make(chan chunk, len(legs)*4),
		errCh:        make(chan error, len(legs)),
		writeQueue:   make(chan writeJob, len(legs)*2),
		flush:        make(chan struct{}),
		legsActive:   int32(len(legs)),
		legsDone:     make(chan struct{}),
		totalKnown:   make(chan struct{}),
		closed:       make(chan struct{}),
	}
	c.chunkPool.New = func() any {
		b := make([]byte, chunkSize)
		return &b
	}
	for _, leg := range legs {
		c.writersWG.Add(1)
		go c.readLeg(leg)
		go c.writeLeg(leg)
	}
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

		data := make([]byte, length)
		if length > 0 {
			if _, err := io.ReadFull(leg, data); err != nil {
				c.fail(err)
				return
			}
		}

		select {
		case c.chunkCh <- chunk{seq: seq, data: data}:
		case <-c.closed:
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
func (c *Conn) writeLeg(leg net.Conn) {
	defer c.writersWG.Done()
	// One reusable frame buffer per worker: header + a full-size payload. Chunks
	// are framed into it and sent in a single Write (see writeChunk), so the leg
	// transport sees one frame per chunk instead of two.
	frame := make([]byte, headerSize+c.chunkSize)
	for {
		// Always prefer to drain a queued chunk before considering exit. This
		// is what makes a graceful Close lossless even when a teardown races
		// it: a worker can only stop once writeQueue is genuinely empty, so no
		// still-queued tail chunk is ever abandoned to the closed/flush signal.
		select {
		case job := <-c.writeQueue:
			// writeChunk copies job.data into frame before sending, so the
			// borrowed buffer is free to reuse the moment it returns - whether
			// the send succeeded or failed the whole Conn.
			ok := c.writeChunk(leg, frame, job)
			c.chunkPool.Put(job.buf)
			if !ok {
				return
			}
			continue
		default:
		}

		select {
		case job := <-c.writeQueue:
			ok := c.writeChunk(leg, frame, job)
			c.chunkPool.Put(job.buf)
			if !ok {
				return
			}
		case <-c.flush:
			return // graceful close and the queue is drained
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
// buffer, otherwise it waits in pending until its turn comes.
func (c *Conn) stash(ch chunk) {
	if ch.seq == c.nextSeq {
		c.readBuf = ch.data
		c.nextSeq++
	} else {
		c.pending[ch.seq] = ch
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
			if ch.seq >= c.nextSeq {
				c.pending[ch.seq] = ch
			}
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
				c.readBuf = ch.data
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
			c.readBuf = ch.data
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

		timer := time.NewTimer(c.stallTimeout)
		select {
		case ch := <-c.chunkCh:
			timer.Stop()
			c.stash(ch)

		case <-totalKnown:
			timer.Stop()
			// The total just became known; loop to re-check completion and
			// drain whatever is buffered.
			c.drainAvailable()

		case <-c.legsDone:
			timer.Stop()
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
			timer.Stop()
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
