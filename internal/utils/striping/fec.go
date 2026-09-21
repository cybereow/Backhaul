// FEC extends striping with Reed-Solomon erasure coding across legs. Plain
// striping (Conn, above) spreads one flow's bytes across legs for aggregate
// throughput, but as its own doc comment says, a leg dying mid-stream loses
// whatever byte range was in flight on it - there's no retransmission, so the
// whole Conn fails.
//
// FECConn groups writes into fixed-size rows of dataShards chunks and
// computes parityShards parity chunks per row (via
// github.com/klauspost/reedsolomon), sending one shard per leg on
// dataShards+parityShards dedicated legs. As long as at least dataShards of
// those legs stay alive, every row can still be reconstructed even if up to
// parityShards legs die outright - trading bandwidth (the parity shards) and
// some latency (a row can't be delivered until dataShards of its shards
// arrive) for tolerance to a leg failing, which on a lossy/high-latency path
// behind a CDN (idle-killed WebSocket connections, mobile handovers) is what
// actually shows up as "loss" above the TCP layer: TCP itself never drops a
// byte silently, but a whole connection dying certainly does.
package striping

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/reedsolomon"
)

// fecHeaderSize is rowSeq(4) + shardIndex(1) + rowLen(4). Payload follows at
// exactly chunkSize bytes, except for the end-of-stream marker (rowSeq ==
// fecEndMarkerSeq), which carries no payload.
const fecHeaderSize = 4 + 1 + 4

// fecLegQueueDepth is how many shards may sit queued for one leg's writer.
// Unlike plain striping's shared work-stealing queue, FEC assigns a specific
// shard to each leg, so a shallow per-leg queue lets a single slow/stalling leg
// fill its queue and gate flushRow. A deeper queue lets the fast legs run ahead
// while a slow one catches up (paired with flushRow's skip-the-congested-leg
// logic), which is what keeps FEC from collapsing toward the slowest leg.
// fecRowChanDepth sizes the reassembled-row handoff to Read.
const (
	fecLegQueueDepth = 64
	fecRowChanDepth  = 64
)

// fecEndMarkerSeq is a sentinel row sequence number marking end of stream.
// A real row's seq starts at 0 and counts up, nowhere near this, so it's
// unambiguous. When set, the header's rowLen field carries the total number
// of data rows the sender produced.
const fecEndMarkerSeq = 0xFFFFFFFF

type fecRow struct {
	seq  uint32
	at   time.Time // when Read filed it in pending (out-of-order arrival)
	data []byte
}

type fecShardJob struct {
	rowSeq uint32
	rowLen uint32
	data   []byte // exactly chunkSize bytes
}

// pendingRow accumulates the shards seen so far for one row until enough
// have arrived to reconstruct it. Its shards and metadata are charged to the
// reassembly budget until the row is decoded (or the connection ends).
type pendingRow struct {
	shards  [][]byte // len == dataShards+parityShards; nil until received
	got     int
	rowLen  uint32
	rowSeen bool // rowLen has been set by at least one shard
}

// FECConn is a net.Conn that stripes one logical flow across
// dataShards+parityShards legs with Reed-Solomon parity, tolerating up to
// parityShards leg failures.
type FECConn struct {
	legs         []net.Conn
	dataShards   int
	parityShards int
	chunkSize    int
	rowFullSize  int // dataShards * chunkSize
	stallTimeout time.Duration

	enc   reedsolomon.Encoder
	rsMu  sync.Mutex // serializes Reconstruct calls (Encode is only ever called under wmu)
	encMu sync.Mutex // serializes Encode calls (paranoia: keep symmetric with rsMu)

	legQueues []chan fecShardJob
	legAlive  []int32 // atomic bool per leg
	aliveLegs int32
	writersWG sync.WaitGroup
	flush     chan struct{}
	flushOnce sync.Once

	wmu         sync.Mutex // guards curRow/curRowLen/writeRowSeq
	curRow      []byte
	curRowLen   int
	writeRowSeq uint32

	rowsMu sync.Mutex
	rows   map[uint32]*pendingRow
	// doneBase/done record which rows have already been decoded (every seq below
	// doneBase, plus the out-of-order ones in done), so a late or duplicate shard
	// cannot recreate an entry and decode the row a second time. Guarded by
	// rowsMu - not rmu, which Read holds while waiting on producers. done only
	// ever holds rows still retained (charged) above the first undecoded row.
	doneBase uint32
	done     map[uint32]struct{}

	// budget bounds everything retained for reassembly: incomplete shards and
	// their row metadata, decoded rows in rowCh and pending, and readBuf.
	budget  reassemblyBudget
	rowMeta int64 // charge per incomplete row: entry + shard slice headers

	rmu       sync.Mutex
	nextSeq   uint32
	pending   map[uint32]fecRow
	readBuf   []byte
	readCost  int64
	gap       gapState
	readTimer *time.Timer

	errMu   sync.Mutex
	permErr error

	writeBroken int32

	rowCh      chan fecRow
	errCh      chan error
	total      uint32
	haveTotal  int32
	totalKnown chan struct{}
	totalOnce  sync.Once

	closeOnce sync.Once
	closed    chan struct{}
}

// NewFEC wraps exactly dataShards+parityShards legs (already-connected,
// already correlated to the same logical flow on both ends, in matching
// shard-index order on both peers) into a single net.Conn protected by
// Reed-Solomon erasure coding. parityShards must be > 0 and dataShards must
// be > 0; len(legs) must equal dataShards+parityShards.
func NewFEC(legs []net.Conn, chunkSize int, dataShards, parityShards int) (*FECConn, error) {
	if dataShards <= 0 || parityShards <= 0 {
		return nil, fmt.Errorf("striping: FEC requires dataShards>0 and parityShards>0")
	}
	if len(legs) != dataShards+parityShards {
		return nil, fmt.Errorf("striping: FEC needs %d legs (dataShards+parityShards), got %d", dataShards+parityShards, len(legs))
	}
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		return nil, fmt.Errorf("striping: failed to build Reed-Solomon encoder: %w", err)
	}

	n := len(legs)
	c := &FECConn{
		legs:         legs,
		dataShards:   dataShards,
		parityShards: parityShards,
		chunkSize:    chunkSize,
		rowFullSize:  dataShards * chunkSize,
		stallTimeout: defaultStallTimeout,
		enc:          enc,
		legQueues:    make([]chan fecShardJob, n),
		legAlive:     make([]int32, n),
		aliveLegs:    int32(n),
		flush:        make(chan struct{}),
		rows:         make(map[uint32]*pendingRow),
		done:         make(map[uint32]struct{}),
		pending:      make(map[uint32]fecRow),
		budget:       reassemblyBudget{limit: reassemblyBudgetBytes},
		rowMeta:      int64(2*retainedEntryOverhead + 24*n),
		errCh:        make(chan error, n),
		totalKnown:   make(chan struct{}),
		closed:       make(chan struct{}),
	}
	c.rowCh = make(chan fecRow, c.budget.depth(fecRowChanDepth, c.decodedCost(c.rowFullSize)))
	for i := range legs {
		c.legAlive[i] = 1
		c.legQueues[i] = make(chan fecShardJob, fecLegQueueDepth)
	}
	for i, leg := range legs {
		c.writersWG.Add(1)
		go c.readLeg(leg, i)
		go c.writeLeg(leg, i)
	}
	return c, nil
}

func (c *FECConn) readLeg(leg net.Conn, idx int) {
	header := make([]byte, fecHeaderSize)
	for {
		if _, err := io.ReadFull(leg, header); err != nil {
			// A clean io.EOF at a header boundary, once the stream's total
			// is already known, is this leg's expected close after Close()
			// broadcast the end marker and started tearing every leg down -
			// not a failure toward the redundancy budget. Without this check
			// every leg closing together at the end of a normal stream would
			// cascade through markLegDead's "too many legs lost" trip, even
			// though nothing was actually lost.
			if err == io.EOF && atomic.LoadInt32(&c.haveTotal) == 1 {
				return
			}
			c.markLegDead(idx, err)
			return
		}
		rowSeq := binary.BigEndian.Uint32(header[0:4])
		shardIndex := header[4]
		rowLen := binary.BigEndian.Uint32(header[5:9])

		if rowSeq == fecEndMarkerSeq {
			// End of stream: rowLen carries the total data-row count. No
			// payload follows.
			c.setTotal(rowLen)
			continue
		}

		// Reserve before retaining; receiveShard/decode own the charge from here.
		shardCost := int64(c.chunkSize)
		if !c.budget.reserve(shardCost) {
			c.fail(c.budget.err(shardCost))
			return
		}
		data := make([]byte, c.chunkSize)
		if _, err := io.ReadFull(leg, data); err != nil {
			c.budget.release(shardCost)
			c.markLegDead(idx, err)
			return
		}

		c.receiveShard(rowSeq, int(shardIndex), rowLen, data)
	}
}

// receiveShard files one shard under its row and reconstructs/delivers the
// row once dataShards of its dataShards+parityShards shards have arrived. The
// shard's chunkSize budget charge (reserved by readLeg) is owned here: kept
// while the shard is retained, released when it is dropped or its row decoded.
func (c *FECConn) receiveShard(rowSeq uint32, shardIndex int, rowLen uint32, data []byte) {
	shardCost := int64(c.chunkSize)
	c.rowsMu.Lock()
	if shardIndex < 0 || shardIndex >= c.dataShards+c.parityShards || c.rowDoneLocked(rowSeq) {
		// Corrupt/hostile shard index, or a straggler for a row already decoded.
		c.rowsMu.Unlock()
		c.budget.release(shardCost)
		return
	}
	row, ok := c.rows[rowSeq]
	if !ok {
		if !c.budget.reserve(c.rowMeta) {
			c.rowsMu.Unlock()
			c.budget.release(shardCost)
			c.fail(c.budget.err(c.rowMeta))
			return
		}
		row = &pendingRow{shards: make([][]byte, c.dataShards+c.parityShards)}
		c.rows[rowSeq] = row
	}
	if row.shards[shardIndex] != nil {
		c.rowsMu.Unlock()
		c.budget.release(shardCost)
		return // duplicate, ignore
	}
	row.shards[shardIndex] = data
	row.got++
	if !row.rowSeen {
		row.rowLen = rowLen
		row.rowSeen = true
	}

	if row.got < c.dataShards {
		c.rowsMu.Unlock()
		return
	}
	delete(c.rows, rowSeq)
	c.markRowDoneLocked(rowSeq)
	c.rowsMu.Unlock()

	// Exactly dataShards shards are charged: the row completes on the dataShards-th.
	held := int64(row.got)*shardCost + c.rowMeta
	full, err := c.decodeRow(row)
	if err != nil {
		c.budget.release(held)
		c.fail(fmt.Errorf("striping: failed to reconstruct row %d: %w", rowSeq, err))
		return
	}
	// The decoded row is never larger than the shards it came from, so swap the
	// charge in place instead of reserving again.
	dec := c.decodedCost(len(full))
	c.budget.release(held - dec)

	select {
	case c.rowCh <- fecRow{seq: rowSeq, data: full}:
	case <-c.closed:
		c.budget.release(dec)
	}
}

// decodedCost is the budget charge for a decoded row of n bytes.
func (c *FECConn) decodedCost(n int) int64 { return int64(n) + retainedEntryOverhead }

// rowDoneLocked reports whether the row was already decoded. Caller holds rowsMu.
func (c *FECConn) rowDoneLocked(seq uint32) bool {
	if seq < c.doneBase {
		return true
	}
	_, ok := c.done[seq]
	return ok
}

// markRowDoneLocked records a decoded row, advancing doneBase over the
// contiguous decoded prefix. Caller holds rowsMu.
func (c *FECConn) markRowDoneLocked(seq uint32) {
	if seq != c.doneBase {
		c.done[seq] = struct{}{}
		return
	}
	c.doneBase++
	for {
		if _, ok := c.done[c.doneBase]; !ok {
			return
		}
		delete(c.done, c.doneBase)
		c.doneBase++
	}
}

// decodeRow reconstructs any missing data shards (if needed) and returns the
// row's real bytes (rowFullSize bytes trimmed down to rowLen). Reconstruct is
// skipped entirely when every data shard already arrived - the common case
// when no leg has died - so parity is only ever spent computing on the
// degraded path.
func (c *FECConn) decodeRow(row *pendingRow) ([]byte, error) {
	missing := false
	for i := 0; i < c.dataShards; i++ {
		if row.shards[i] == nil {
			missing = true
			break
		}
	}
	if missing {
		c.rsMu.Lock()
		err := c.enc.Reconstruct(row.shards)
		c.rsMu.Unlock()
		if err != nil {
			return nil, err
		}
	}

	if row.rowLen > uint32(c.rowFullSize) {
		return nil, fmt.Errorf("row length %d exceeds max %d", row.rowLen, c.rowFullSize)
	}

	full := make([]byte, row.rowLen)
	remaining := int(row.rowLen)
	for i := 0; i < c.dataShards && remaining > 0; i++ {
		n := c.chunkSize
		if n > remaining {
			n = remaining
		}
		copy(full[i*c.chunkSize:i*c.chunkSize+n], row.shards[i][:n])
		remaining -= n
	}
	return full, nil
}

func (c *FECConn) setTotal(total uint32) {
	atomic.StoreUint32(&c.total, total)
	atomic.StoreInt32(&c.haveTotal, 1)
	c.totalOnce.Do(func() { close(c.totalKnown) })
}

func (c *FECConn) complete() bool {
	return atomic.LoadInt32(&c.haveTotal) == 1 && c.nextSeq >= atomic.LoadUint32(&c.total)
}

// markLegDead permanently retires one leg after a read or write failure.
// Up to parityShards legs can go this way without losing the ability to
// reconstruct every row; once alive legs drop below dataShards, the whole
// Conn fails - there's no longer enough redundancy to guarantee recovery.
func (c *FECConn) markLegDead(idx int, err error) {
	if !atomic.CompareAndSwapInt32(&c.legAlive[idx], 1, 0) {
		return
	}
	c.legs[idx].Close()
	if atomic.AddInt32(&c.aliveLegs, -1) < int32(c.dataShards) {
		c.fail(fmt.Errorf("striping: too many legs lost to sustain FEC (last: %w)", err))
	}
}

func (c *FECConn) legIsAlive(idx int) bool {
	return atomic.LoadInt32(&c.legAlive[idx]) == 1
}

func (c *FECConn) writeLeg(leg net.Conn, idx int) {
	defer c.writersWG.Done()
	frame := make([]byte, fecHeaderSize+c.chunkSize)
	for {
		select {
		case job := <-c.legQueues[idx]:
			c.sendShard(leg, idx, frame, job)
			continue
		default:
		}

		select {
		case job := <-c.legQueues[idx]:
			c.sendShard(leg, idx, frame, job)
		case <-c.flush:
			return
		case <-c.closed:
			return
		}
	}
}

// sendShard writes one shard frame. A dead leg's jobs are silently dropped
// (draining the channel keeps the row-encode step in Write from blocking on
// a leg nobody will ever read for again); a live leg that fails to write is
// retired via markLegDead instead of tearing down the whole Conn.
func (c *FECConn) sendShard(leg net.Conn, idx int, frame []byte, job fecShardJob) {
	if !c.legIsAlive(idx) {
		return
	}
	if err := leg.SetWriteDeadline(time.Now().Add(c.stallTimeout)); err != nil {
		c.markLegDead(idx, err)
		return
	}
	binary.BigEndian.PutUint32(frame[0:4], job.rowSeq)
	frame[4] = byte(idx)
	binary.BigEndian.PutUint32(frame[5:9], job.rowLen)
	copy(frame[fecHeaderSize:], job.data)
	if _, err := leg.Write(frame[:fecHeaderSize+c.chunkSize]); err != nil {
		c.markLegDead(idx, err)
	}
}

func (c *FECConn) fail(err error) {
	c.setPermErr(err)
	select {
	case c.errCh <- err:
	default:
	}
	c.teardown()
}

func (c *FECConn) setPermErr(err error) {
	c.errMu.Lock()
	if c.permErr == nil {
		c.permErr = err
	}
	c.errMu.Unlock()
}

func (c *FECConn) getPermErr() error {
	c.errMu.Lock()
	defer c.errMu.Unlock()
	return c.permErr
}

// Write buffers p into the current row, flushing (encoding + dispatching to
// legs) whenever a full row of dataShards*chunkSize bytes accumulates.
// After all input bytes are consumed, any remaining partial row is also
// flushed immediately so that short requests reach the peer without waiting
// for EOF. This prioritises request-progress over padding efficiency; tiny
// writes incur more padding overhead than full-row writes. A bounded-delay
// batching design is a separately justified follow-on, not an unbounded buffer.
// ponytail: per-Write flush; add batching only when padding overhead is measured.
func (c *FECConn) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if err := c.getPermErr(); err != nil {
		return 0, err
	}
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
		if c.curRow == nil {
			c.curRow = make([]byte, c.rowFullSize)
			c.curRowLen = 0
		}
		n := copy(c.curRow[c.curRowLen:], p)
		c.curRowLen += n
		p = p[n:]
		total += n

		if c.curRowLen == c.rowFullSize {
			if err := c.flushRow(); err != nil {
				return total, err
			}
		}
	}
	// Flush any remaining partial row immediately: a short request must reach
	// the peer before the caller either waits for a response or writes more
	// data. flushRow zero-pads the backing row (already zeroed at allocation
	// via make) and records rowLen as the real byte count so the peer strips
	// the padding on decode.
	if c.curRowLen > 0 {
		if err := c.flushRow(); err != nil {
			return total, err
		}
	}
	return total, nil
}

// flushRow Reed-Solomon encodes the current row and dispatches one shard to
// each leg. Caller holds wmu.
func (c *FECConn) flushRow() error {
	row := c.curRow
	rowLen := uint32(c.curRowLen)
	c.curRow = nil
	c.curRowLen = 0

	shards := make([][]byte, c.dataShards+c.parityShards)
	for i := 0; i < c.dataShards; i++ {
		shards[i] = row[i*c.chunkSize : (i+1)*c.chunkSize]
	}
	for i := c.dataShards; i < len(shards); i++ {
		shards[i] = make([]byte, c.chunkSize)
	}

	c.encMu.Lock()
	err := c.enc.Encode(shards)
	c.encMu.Unlock()
	if err != nil {
		c.fail(fmt.Errorf("striping: FEC encode failed: %w", err))
		return err
	}

	seq := c.writeRowSeq
	c.writeRowSeq++

	// Dispatch one shard per leg, but do NOT let a congested (slow/stalling) leg
	// gate the whole flow. The receiver reconstructs a row from any dataShards of
	// the dataShards+parityShards shards, so up to parityShards legs may miss a
	// given row and it still decodes - that redundancy is the entire point of
	// FEC. So: first try every leg without blocking; for the legs whose queue is
	// full, only block on as many as are still needed to put dataShards shards on
	// the wire, and skip the rest (a slow leg's shard, covered by parity). This
	// keeps throughput at the fast legs' rate during a stall instead of collapsing
	// to the slowest leg - the bug that made FEC ~3x slower than plain striping
	// with one stalling leg.
	pending := make([]int, 0, len(shards))
	sent := 0
	for i := range shards {
		if !c.legIsAlive(i) {
			continue
		}
		select {
		case c.legQueues[i] <- fecShardJob{rowSeq: seq, rowLen: rowLen, data: shards[i]}:
			sent++
		case <-c.closed:
			return c.currentErr()
		default:
			pending = append(pending, i)
		}
	}
	for _, i := range pending {
		// Enough shards already queued and we still have skip budget: drop this
		// congested leg's shard for this row - parity will cover it on decode.
		if sent >= c.dataShards {
			continue
		}
		select {
		case c.legQueues[i] <- fecShardJob{rowSeq: seq, rowLen: rowLen, data: shards[i]}:
			sent++
		case <-c.closed:
			return c.currentErr()
		}
	}
	return nil
}

func (c *FECConn) stash(row fecRow) {
	if row.seq < c.nextSeq {
		c.budget.release(c.decodedCost(len(row.data)))
	} else if row.seq == c.nextSeq {
		c.setReadBuf(row)
		c.nextSeq++
	} else if _, dup := c.pending[row.seq]; dup {
		c.budget.release(c.decodedCost(len(row.data)))
	} else {
		row.at = time.Now()
		c.pending[row.seq] = row
	}
}

// setReadBuf installs a decoded row as the current read buffer. Caller holds rmu.
func (c *FECConn) setReadBuf(row fecRow) {
	c.readBuf = row.data
	c.readCost = c.decodedCost(len(row.data))
	if len(row.data) == 0 {
		c.releaseReadBuf()
	}
}

// releaseReadBuf gives back the drained read buffer's budget charge. Caller holds rmu.
func (c *FECConn) releaseReadBuf() {
	c.budget.release(c.readCost)
	c.readCost = 0
}

func (c *FECConn) drainAvailable() {
	for {
		select {
		case row := <-c.rowCh:
			if _, dup := c.pending[row.seq]; row.seq < c.nextSeq || dup {
				c.budget.release(c.decodedCost(len(row.data)))
				continue
			}
			row.at = time.Now()
			c.pending[row.seq] = row
		default:
			return
		}
	}
}

// earliestPending is the first-evidence time for a new gap: the oldest
// out-of-order row Read is holding, or now if the evidence is only an END
// marker. Caller holds rmu.
func (c *FECConn) earliestPending() time.Time {
	t := time.Now()
	for _, row := range c.pending {
		if row.at.Before(t) {
			t = row.at
		}
	}
	return t
}

// dropReassembly gives back everything still retained once the stream has
// ended for good (EOF or a terminal error). Caller holds rmu.
func (c *FECConn) dropReassembly() {
	c.drainAvailable()
	for seq, row := range c.pending {
		c.budget.release(c.decodedCost(len(row.data)))
		delete(c.pending, seq)
	}
	c.readBuf = nil
	c.releaseReadBuf()
	c.rowsMu.Lock()
	for seq, row := range c.rows {
		c.budget.release(int64(row.got)*int64(c.chunkSize) + c.rowMeta)
		delete(c.rows, seq)
	}
	c.rowsMu.Unlock()
}

// Read reassembles rows arriving out of order into the original in-order
// byte stream, exactly like Conn.Read but at row granularity: each row is
// only ever handed to Read once it has already been reconstructed (or
// confirmed intact) from dataShards of its shards. Like Conn.Read, an idle
// connection is never timed out; only a proven gap (a later row arrived, or
// END announced more than was delivered) has an absolute stallTimeout deadline.
func (c *FECConn) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	c.rmu.Lock()
	defer c.rmu.Unlock()
	defer func() {
		if err != nil {
			c.dropReassembly()
		}
	}()

	if len(c.readBuf) == 0 {
		if c.complete() {
			return 0, io.EOF
		}
		if err := c.getPermErr(); err != nil {
			if row, ok := c.pending[c.nextSeq]; ok {
				delete(c.pending, c.nextSeq)
				c.setReadBuf(row)
				c.nextSeq++
			} else {
				return 0, err
			}
		}
	}

	for len(c.readBuf) == 0 {
		if c.complete() {
			return 0, io.EOF
		}
		if row, ok := c.pending[c.nextSeq]; ok {
			delete(c.pending, c.nextSeq)
			c.setReadBuf(row)
			c.nextSeq++
			continue
		}

		var totalKnown <-chan struct{}
		if atomic.LoadInt32(&c.haveTotal) == 0 {
			totalKnown = c.totalKnown
		}

		// Time the wait only if a row is provably missing (see Conn.Read).
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
		case row := <-c.rowCh:
			c.stash(row)

		case <-totalKnown:
			c.drainAvailable()

		case err := <-c.errCh:
			c.setPermErr(err)
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
					return 0, net.ErrClosed
				}
			}

		case <-timerC:
			c.drainAvailable()
			if _, ok := c.pending[c.nextSeq]; ok {
				continue
			}
			stallErr := fmt.Errorf("striping: FEC stalled waiting for row %d for %s", c.nextSeq, c.stallTimeout)
			c.setPermErr(stallErr)
			c.teardown()
			return 0, stallErr
		}
	}

	n = copy(p, c.readBuf)
	c.readBuf = c.readBuf[n:]
	if len(c.readBuf) == 0 {
		c.releaseReadBuf()
	}
	return n, nil
}

func (c *FECConn) currentErr() error {
	select {
	case err := <-c.errCh:
		return err
	default:
		return net.ErrClosed
	}
}

// AbortWrite mirrors Conn.AbortWrite: records that the byte stream feeding
// Write was truncated, so Close skips the end-of-stream marker and the
// peer's Read reports io.ErrUnexpectedEOF instead of a false-clean io.EOF.
func (c *FECConn) AbortWrite() {
	atomic.StoreInt32(&c.writeBroken, 1)
}

func (c *FECConn) CloseWrite() error {
	c.flushOnce.Do(func() {
		c.wmu.Lock()
		if c.curRowLen > 0 {
			for i := c.curRowLen; i < c.rowFullSize; i++ {
				c.curRow[i] = 0
			}
			_ = c.flushRow()
		}
		total := c.writeRowSeq
		c.wmu.Unlock()

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

		if atomic.LoadInt32(&c.writeBroken) == 0 && c.getPermErr() == nil {
			c.sendEndMarkers(total)
		}
	})
	return nil
}

// Close flushes any partial trailing row, then the end-of-stream marker, to
// every leg (best-effort, including already-dead ones) before tearing down.
func (c *FECConn) Close() error {
	_ = c.CloseWrite()
	return c.teardown()
}

func (c *FECConn) sendEndMarkers(total uint32) {
	var header [fecHeaderSize]byte
	binary.BigEndian.PutUint32(header[0:4], fecEndMarkerSeq)
	binary.BigEndian.PutUint32(header[5:9], total)
	for _, leg := range c.legs {
		_ = leg.SetWriteDeadline(time.Now().Add(c.stallTimeout))
		_, _ = leg.Write(header[:])
	}
}

func (c *FECConn) teardown() error {
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

func (c *FECConn) LocalAddr() net.Addr  { return c.legs[0].LocalAddr() }
func (c *FECConn) RemoteAddr() net.Addr { return c.legs[0].RemoteAddr() }

func (c *FECConn) SetDeadline(t time.Time) error {
	return c.forEachLeg(func(l net.Conn) error { return l.SetDeadline(t) })
}

func (c *FECConn) SetReadDeadline(t time.Time) error {
	return c.forEachLeg(func(l net.Conn) error { return l.SetReadDeadline(t) })
}

func (c *FECConn) SetWriteDeadline(t time.Time) error {
	return c.forEachLeg(func(l net.Conn) error { return l.SetWriteDeadline(t) })
}

func (c *FECConn) forEachLeg(f func(net.Conn) error) error {
	var firstErr error
	for _, leg := range c.legs {
		if e := f(leg); e != nil && firstErr == nil {
			firstErr = fmt.Errorf("leg error: %w", e)
		}
	}
	return firstErr
}
