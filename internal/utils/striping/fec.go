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
	data []byte
}

type fecShardJob struct {
	rowSeq uint32
	rowLen uint32
	data   []byte // exactly chunkSize bytes
}

// pendingRow accumulates the shards seen so far for one row until enough
// have arrived to reconstruct it. firstSeen lets a stale entry - e.g. one
// recreated by a shard that straggled in after its row was already decoded
// and removed - be swept instead of sitting in the map forever.
type pendingRow struct {
	shards    [][]byte // len == dataShards+parityShards; nil until received
	got       int
	rowLen    uint32
	rowSeen   bool // rowLen has been set by at least one shard
	firstSeen time.Time
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

	rmu     sync.Mutex
	nextSeq uint32
	pending map[uint32]fecRow
	readBuf []byte

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
		pending:      make(map[uint32]fecRow),
		rowCh:        make(chan fecRow, fecRowChanDepth),
		errCh:        make(chan error, n),
		totalKnown:   make(chan struct{}),
		closed:       make(chan struct{}),
	}
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

		data := make([]byte, c.chunkSize)
		if _, err := io.ReadFull(leg, data); err != nil {
			c.markLegDead(idx, err)
			return
		}

		c.receiveShard(rowSeq, int(shardIndex), rowLen, data)
	}
}

// receiveShard files one shard under its row and reconstructs/delivers the
// row once dataShards of its dataShards+parityShards shards have arrived.
func (c *FECConn) receiveShard(rowSeq uint32, shardIndex int, rowLen uint32, data []byte) {
	c.rowsMu.Lock()
	row, ok := c.rows[rowSeq]
	if !ok {
		row = &pendingRow{shards: make([][]byte, c.dataShards+c.parityShards), firstSeen: time.Now()}
		c.rows[rowSeq] = row
	}
	if shardIndex < 0 || shardIndex >= len(row.shards) {
		c.rowsMu.Unlock()
		return // corrupt/hostile shard index; drop it
	}
	if row.shards[shardIndex] != nil {
		c.rowsMu.Unlock()
		return // duplicate, ignore
	}
	row.shards[shardIndex] = data
	row.got++
	if !row.rowSeen {
		row.rowLen = rowLen
		row.rowSeen = true
	}

	if row.got < c.dataShards {
		c.sweepStaleRows()
		c.rowsMu.Unlock()
		return
	}
	delete(c.rows, rowSeq)
	c.rowsMu.Unlock()

	full, err := c.decodeRow(row)
	if err != nil {
		c.fail(fmt.Errorf("striping: failed to reconstruct row %d: %w", rowSeq, err))
		return
	}

	select {
	case c.rowCh <- fecRow{seq: rowSeq, data: full}:
	case <-c.closed:
	}
}

// sweepStaleRows drops pendingRow entries that have sat incomplete for more
// than 2*stallTimeout. The one case that matters in practice: a shard
// straggling in after its row was already decoded and deleted recreates a
// map entry that will never reach dataShards again (its siblings are gone),
// so left alone it would sit in the map for the life of the Conn. Caller
// holds rowsMu.
func (c *FECConn) sweepStaleRows() {
	if len(c.rows) == 0 {
		return
	}
	cutoff := time.Now().Add(-2 * c.stallTimeout)
	for seq, row := range c.rows {
		if row.firstSeen.Before(cutoff) {
			delete(c.rows, seq)
		}
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
func (c *FECConn) Write(p []byte) (int, error) {
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
	if row.seq == c.nextSeq {
		c.readBuf = row.data
		c.nextSeq++
	} else {
		c.pending[row.seq] = row
	}
}

func (c *FECConn) drainAvailable() {
	for {
		select {
		case row := <-c.rowCh:
			if row.seq >= c.nextSeq {
				c.pending[row.seq] = row
			}
		default:
			return
		}
	}
}

// Read reassembles rows arriving out of order into the original in-order
// byte stream, exactly like Conn.Read but at row granularity: each row is
// only ever handed to Read once it has already been reconstructed (or
// confirmed intact) from dataShards of its shards.
func (c *FECConn) Read(p []byte) (int, error) {
	c.rmu.Lock()
	defer c.rmu.Unlock()

	if len(c.readBuf) == 0 {
		if c.complete() {
			return 0, io.EOF
		}
		if err := c.getPermErr(); err != nil {
			if row, ok := c.pending[c.nextSeq]; ok {
				delete(c.pending, c.nextSeq)
				c.readBuf = row.data
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
			c.readBuf = row.data
			c.nextSeq++
			continue
		}

		var totalKnown <-chan struct{}
		if atomic.LoadInt32(&c.haveTotal) == 0 {
			totalKnown = c.totalKnown
		}

		timer := time.NewTimer(c.stallTimeout)
		select {
		case row := <-c.rowCh:
			timer.Stop()
			c.stash(row)

		case <-totalKnown:
			timer.Stop()
			c.drainAvailable()

		case err := <-c.errCh:
			timer.Stop()
			c.setPermErr(err)
			c.drainAvailable()
			if len(c.readBuf) == 0 && !c.complete() {
				if _, ok := c.pending[c.nextSeq]; !ok {
					return 0, c.getPermErr()
				}
			}

		case <-timer.C:
			stallErr := fmt.Errorf("striping: FEC stalled waiting for row %d for %s", c.nextSeq, c.stallTimeout)
			c.setPermErr(stallErr)
			c.teardown()
			return 0, stallErr
		}
	}

	n := copy(p, c.readBuf)
	c.readBuf = c.readBuf[n:]
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
