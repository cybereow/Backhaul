package handlers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/musix/backhaul/internal/utils"
	"github.com/musix/backhaul/internal/web"
	"github.com/sirupsen/logrus"
)

// PromoteHandshakeTimeout bounds the whole promotion handshake (freeze, count
// exchange, wrapper construction, install). See PumpSwapper.Promote. A variable
// only so tests can shorten it; production never changes it.
var PromoteHandshakeTimeout = 10 * time.Second

// ErrPromoteUnavailable means the flow can no longer (or not right now) be
// promoted: a direction already ended, it was aborted, or a freeze is pending.
var ErrPromoteUnavailable = errors.New("promotable pump: flow cannot be promoted")

// PumpSwapper runs one flow as two independent pumps and can migrate it from
// its current tunnel to a new one mid-stream, any number of times, without
// dropping or duplicating a byte.
//
// Byte ownership: the upload pump exclusively owns app reads and tunnel writes;
// the download pump exclusively owns tunnel reads and app writes. The state
// below is guarded by mu, which is never held across I/O (only non-blocking
// SetReadDeadline calls, used to wake an owner that is parked in a blocking
// Read).
//
// Offsets: UpBytes and DlBytes count payload bytes from the START OF THE FLOW,
// not of the current tunnel, and are what the swap handshake exchanges. A swap
// therefore needs no per-tunnel base: "deliver until DlBytes == the peer's
// count" means the same thing at every swap.
//
// One swap:
//  1. FreezeUp asks the upload pump to stop at its next write boundary. The pump
//     finishes any in-flight write, then acks; UpBytes at that moment is the
//     final number of bytes committed to the current tunnel.
//  2. The peer's count arrives (raw legs, before any striped wrapper exists) and
//     Install hands over the new tunnel. The download pump then delivers exactly
//     that many bytes in total from the current tunnel and switches; the upload
//     pump starts on the new tunnel.
//  3. Once both directions have switched, the replaced tunnel is released and the
//     flow is ready for the next swap. FreezeUp refuses (ErrPromoteUnavailable)
//     while a swap is still in progress.
//
// A direction that already ended (END sent or received) has nothing to stop or
// switch: its end is simply re-sent on the new tunnel, so a half-closed flow
// can still be moved while the other direction keeps running.
//
// Failing before the freeze ack leaves the flow on its current tunnel. Failing
// after it is not recoverable (the peer may have frozen) and ends in Abort.
type PumpSwapper struct {
	app net.Conn

	upBytes atomic.Uint64 // payload bytes committed to a tunnel, from the start of the flow
	dlBytes atomic.Uint64 // payload bytes delivered to the app, from the start of the flow
	done    chan struct{}

	// Replay (level B): nil unless EnableReplay was called before Start.
	replay     *replayState
	freezeWake chan struct{} // FreezeUp nudges an upload pump parked on a full replay ring
	upDst      net.Conn      // the tunnel the upload pump writes to (guarded by mu); acks go there too

	// What Start needs.
	ctx        context.Context
	usage      *web.Usage
	remotePort int
	sniffer    bool
	startOnce  sync.Once

	mu  sync.Mutex
	cur net.Conn // tunnel in service; replaced when a swap completes

	// The swap in progress (zero values = none).
	freezeReq  bool // FreezeUp asked; upload pump must stop at the next boundary
	frozen     bool // upload acked (or had already ended); upLimit is final
	installed  bool
	upSwitched bool // the upload direction is on next (or has ended)
	dlSwitched bool // the download direction is on next (or has ended)
	next       net.Conn
	upLimit    uint64
	dlLimit    uint64

	upEnded bool // the upload direction finished normally
	dlEnded bool // the download direction finished normally
	aborted bool
	swaps   uint64 // completed swaps

	upAck     chan struct{} // closed when the upload pump acks the freeze
	installCh chan struct{} // closed by Install
	abortCh   chan struct{} // closed by Abort
}

func (p *PumpSwapper) DoneWait() <-chan struct{} {
	return p.done
}

// UpBytes is the number of payload bytes committed to a tunnel since the start
// of the flow.
func (p *PumpSwapper) UpBytes() uint64 { return p.upBytes.Load() }

// DlBytes is the number of payload bytes delivered to the app since the start of
// the flow.
func (p *PumpSwapper) DlBytes() uint64 { return p.dlBytes.Load() }

// Swaps is the number of swaps that have fully completed.
func (p *PumpSwapper) Swaps() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.swaps
}

// switched records that one direction is on the new tunnel; the second one
// completes the swap: the replaced tunnel is released (outside any lock) and the
// swap state is reset for the next one.
func (p *PumpSwapper) switched(up bool) {
	p.mu.Lock()
	if up {
		p.upSwitched = true
	} else {
		p.dlSwitched = true
	}
	if !p.upSwitched || !p.dlSwitched {
		p.mu.Unlock()
		return
	}
	old := p.cur
	p.cur, p.next = p.next, nil
	p.freezeReq, p.frozen, p.installed = false, false, false
	p.upSwitched, p.dlSwitched = false, false
	p.upAck = make(chan struct{})
	p.installCh = make(chan struct{})
	p.swaps++
	p.mu.Unlock()
	go old.Close()
	if p.replay != nil {
		// An ACK sent on the old tunnel after the peer stopped reading it never
		// arrived, and with no more data nothing would trigger another: say again,
		// on the new tunnel, how much has been delivered (ACKs are cumulative, so
		// repeating one is harmless).
		p.replay.ackSent.Store(0)
		p.kickAck()
	}
}

// Abort tears the whole flow down: every conn is closed (destinations that can
// mark a stream as truncated are told first, so the far end sees a failure and
// not a clean EOF) and both pumps and any waiting handshake are released.
// Idempotent and safe from any goroutine.
func (p *PumpSwapper) Abort() {
	p.mu.Lock()
	if p.aborted {
		p.mu.Unlock()
		return
	}
	p.aborted = true
	close(p.abortCh)
	conns := [3]net.Conn{p.app, p.cur, p.next}
	p.mu.Unlock()

	for _, c := range conns {
		if c == nil {
			continue
		}
		if aw, ok := c.(interface{ AbortWrite() }); ok {
			aw.AbortWrite()
		}
		c.Close()
	}
}

// PromotablePump replaces io.CopyBuffer with a custom loop for promotable flows.
// It is NewPromotablePump followed by Start.
func PromotablePump(
	ctx context.Context, proxyProtocol bool, app net.Conn, tunnel net.Conn,
	logger *logrus.Logger, usage *web.Usage, remotePort int, sniffer bool,
) *PumpSwapper {
	p := NewPromotablePump(ctx, proxyProtocol, app, tunnel, logger, usage, remotePort, sniffer)
	if p != nil {
		p.Start()
	}
	return p
}

// NewPromotablePump builds the swapper without running it, so the caller can
// publish it (for a peer's attach to find) before any byte moves, and then call
// Start. Nil means the PROXY protocol header could not be written and the flow's
// conns are already closed.
func NewPromotablePump(
	ctx context.Context, proxyProtocol bool, app net.Conn, tunnel net.Conn,
	logger *logrus.Logger, usage *web.Usage, remotePort int, sniffer bool,
) *PumpSwapper {
	p := &PumpSwapper{
		app:        app,
		cur:        tunnel,
		upDst:      tunnel,
		freezeWake: make(chan struct{}, 1),
		done:       make(chan struct{}),
		upAck:      make(chan struct{}),
		installCh:  make(chan struct{}),
		abortCh:    make(chan struct{}),
		ctx:        ctx,
		usage:      usage,
		remotePort: remotePort,
		sniffer:    sniffer,
	}

	if proxyProtocol {
		// The header travels the tunnel like any byte and the peer delivers it to
		// its app, so it is payload: it counts in UpBytes, or the count exchanged
		// at a swap would be short by its length and the peer would switch early.
		header, err := ProxyProtocolHeader(app.RemoteAddr(), tunnel.RemoteAddr())
		if err == nil {
			var w int
			w, err = writeFull(tunnel, header)
			p.upBytes.Add(uint64(w))
		}
		if err != nil {
			logger.Error(err)
			app.Close()
			tunnel.Close()
			return nil
		}
	}
	return p
}

// Start runs the two pumps. Idempotent.
func (p *PumpSwapper) Start() {
	p.startOnce.Do(p.start)
}

func (p *PumpSwapper) start() {
	if p.replay != nil {
		p.attachAcks(p.cur)
	}
	go func() {
		select {
		case <-p.ctx.Done():
			p.Abort()
		case <-p.done:
		}
	}()

	var wg sync.WaitGroup
	wg.Add(2)

	// App -> Tunnel (Upload direction relative to the tunnel's writes)
	go func() {
		defer wg.Done()
		p.pumpAppToTunnel(p.usage, p.remotePort, p.sniffer)
	}()

	// Tunnel -> App (Download direction relative to the tunnel's reads)
	go func() {
		defer wg.Done()
		p.pumpTunnelToApp(p.usage, p.remotePort, p.sniffer)
	}()

	go func() {
		wg.Wait()
		close(p.done)
		p.app.Close()
		p.mu.Lock()
		cur, next := p.cur, p.next
		p.mu.Unlock()
		if cur != nil {
			cur.Close()
		}
		if next != nil {
			next.Close()
		}
	}()
}

// Swappable reports whether a swap could be started right now: the flow is not
// aborted or finished and no swap is in progress. It is a hint for answering an
// attach before anything freezes, not a reservation: FreezeUp still decides.
func (p *PumpSwapper) Swappable() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.aborted && !p.freezeReq && !(p.upEnded && p.dlEnded)
}

// FreezeUp stops the upload direction at a write boundary and returns the final
// number of bytes committed to the current tunnel (counted from the start of the
// flow). It blocks until the upload pump has completed any in-flight write,
// bounded by ctx. On failure it withdraws the request: the flow keeps running on
// its current tunnel (ErrPromoteUnavailable, ctx error, or an aborted/finished
// flow). A swap that is still in progress also yields ErrPromoteUnavailable.
func (p *PumpSwapper) FreezeUp(ctx context.Context) (uint64, error) {
	p.mu.Lock()
	if p.aborted || p.freezeReq || (p.upEnded && p.dlEnded) {
		err := fmt.Errorf("%w (aborted=%v finished=%v swap pending=%v)", ErrPromoteUnavailable, p.aborted, p.upEnded && p.dlEnded, p.freezeReq)
		p.mu.Unlock()
		return 0, err
	}
	p.freezeReq = true
	ack := p.upAck
	if p.upEnded {
		// The upload already ended on the current tunnel: there is nothing to stop
		// and the count is final. Install re-sends the end on the new tunnel.
		p.frozen = true
		p.upLimit = p.upBytes.Load()
		n := p.upLimit
		p.mu.Unlock()
		close(ack)
		return n, nil
	}
	// The upload pump may be parked in app.Read: wake it. It owns that read
	// direction and clears the deadline itself. Or it may be parked waiting for
	// ACKs on a full replay ring, which can only arrive once the swap completes
	// (the peer acks on the tunnel it is about to switch to): nudge it too.
	_ = p.app.SetReadDeadline(time.Unix(1, 0))
	select {
	case p.freezeWake <- struct{}{}:
	default:
	}
	p.mu.Unlock()

	var cause error
	select {
	case <-ack:
		p.mu.Lock()
		n := p.upLimit
		p.mu.Unlock()
		return n, nil
	case <-ctx.Done():
		cause = ctx.Err()
	case <-p.abortCh:
		cause = ErrPromoteUnavailable
	case <-p.done:
		cause = ErrPromoteUnavailable
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-ack: // the ack raced the failure: the freeze did happen
		return p.upLimit, nil
	default:
	}
	p.freezeReq = false
	_ = p.app.SetReadDeadline(time.Time{})
	return 0, cause
}

// Install hands over the new tunnel after a successful freeze. dlLimit is the
// peer's committed byte count, from the start of the flow: exactly that many
// bytes in total are delivered before the download switches. An error means the
// flow ended or was aborted meanwhile; the caller must then Abort (post-freeze).
func (p *PumpSwapper) Install(newTunnel net.Conn, dlLimit uint64) error {
	p.mu.Lock()
	if p.aborted || !p.frozen || p.installed || (p.upEnded && p.dlEnded) {
		p.mu.Unlock()
		return ErrPromoteUnavailable
	}
	p.next = newTunnel
	p.dlLimit = dlLimit
	p.installed = true
	if p.replay != nil {
		// The new tunnel's ACKs are parsed by whoever reads it: the download pump
		// once it switches, or nobody if the download already ended.
		p.attachAcks(newTunnel)
		if p.dlEnded {
			if sv, ok := newTunnel.(interface{ ServeAcks() }); ok {
				sv.ServeAcks()
			}
		}
	}
	resendEnd := p.upEnded
	if p.upEnded {
		p.upSwitched = true // no pump left to switch
		p.upDst = newTunnel // ... so its ACKs must be pointed at the new tunnel here
	}
	if p.dlEnded {
		p.dlSwitched = true
	} else {
		// A download read already sitting at the boundary would wait forever for
		// bytes the peer will never send: wake it.
		_ = p.cur.SetReadDeadline(time.Unix(1, 0))
	}
	ic := p.installCh
	p.mu.Unlock()
	close(ic)
	if resendEnd {
		closeWrite(newTunnel) // the upload's EOF goes to the new tunnel too
	}
	return nil
}

// Promote runs the whole handshake for one new striped group whose raw legs are
// legs (leg 0 carries the counts). Order: freeze, exchange counts on the raw leg
// with no wrapper (hence no second reader) yet, build the striped wrapper,
// install. The whole thing is bounded by PromoteHandshakeTimeout and ctx.
// A failure before the freeze ack closes the legs and leaves the flow plain; a
// later failure closes the legs and aborts the flow.
func (p *PumpSwapper) Promote(ctx context.Context, legs []net.Conn, build func() (net.Conn, error)) error {
	ctx, cancel := context.WithTimeout(ctx, PromoteHandshakeTimeout)
	defer cancel()

	own, err := p.FreezeUp(ctx)
	if err != nil {
		closeAll(legs)
		return err
	}

	peer, err := exchangeCounts(ctx, legs[0], own)
	if err == nil {
		var striped net.Conn
		if striped, err = build(); err == nil {
			if err = p.Install(striped, peer); err != nil {
				if aw, ok := striped.(interface{ AbortWrite() }); ok {
					aw.AbortWrite()
				}
				striped.Close()
			}
		}
	}
	if err != nil {
		closeAll(legs)
		p.Abort()
		return err
	}
	return nil
}

func closeAll(conns []net.Conn) {
	for _, c := range conns {
		c.Close()
	}
}

// exchangeCounts swaps the two 8-byte committed counts on a raw leg, under a
// deadline derived from ctx that is cleared before the leg carries data.
func exchangeCounts(ctx context.Context, leg net.Conn, own uint64) (uint64, error) {
	if dl, ok := ctx.Deadline(); ok {
		_ = leg.SetDeadline(dl)
	}
	stop := context.AfterFunc(ctx, func() { _ = leg.SetDeadline(time.Unix(1, 0)) })

	var peer uint64
	err := utils.WriteCount(leg, own)
	if err == nil {
		peer, err = utils.ReadCount(leg)
	}
	stop()
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return 0, err
	}
	_ = leg.SetDeadline(time.Time{})
	return peer, nil
}

// writeFull writes all of b, returning how many bytes were accepted.
func writeFull(dst net.Conn, b []byte) (int, error) {
	written := 0
	for written < len(b) {
		w, err := dst.Write(b[written:])
		written += w
		if err != nil {
			return written, err
		}
		if w == 0 {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

func (p *PumpSwapper) pumpAppToTunnel(usage *web.Usage, remotePort int, sniffer bool) {
	app := p.app
	bufPtr := copyBufferPool.Get().(*[]byte)
	buf := *bufPtr
	defer copyBufferPool.Put(bufPtr)

	p.mu.Lock()
	dst := p.cur
	p.mu.Unlock()
	var srcErr error // how the app read ended; delivered to whichever tunnel is current

	for { // one pass per tunnel the flow is carried on
		var installCh chan struct{}

		// app -> dst, until the app ends or a freeze is requested.
	run:
		for {
			if srcErr == nil {
				n, err := app.Read(buf)
				if n > 0 {
					if p.replay != nil {
						// Keep what is being sent until the peer acknowledges it, and
						// wait here (not in the app's socket) while a full window of it
						// is unacknowledged.
						if !p.waitReplayRoom(n) {
							p.Abort()
							return
						}
						p.replay.ring.append(buf[:n])
					}
					w, werr := writeFull(dst, buf[:n])
					p.upBytes.Add(uint64(w))
					if sniffer {
						usage.AddOrUpdatePort(remotePort, uint64(n))
					}
					if werr != nil {
						p.Abort()
						return
					}
				}
				srcErr = err
			}

			p.mu.Lock()
			if p.freezeReq && !p.frozen {
				// Boundary: every byte read from the app so far is written to the
				// tunnel, so UpBytes is final. A timeout here is our own wakeup.
				p.frozen = true
				p.upLimit = p.upBytes.Load()
				_ = app.SetReadDeadline(time.Time{})
				ack := p.upAck
				installCh = p.installCh
				p.mu.Unlock()
				close(ack)
				if isTimeout(srcErr) {
					srcErr = nil
				}
				break run
			}
			if srcErr != nil && isTimeout(srcErr) {
				// A wakeup whose freeze was withdrawn: not an application error.
				_ = app.SetReadDeadline(time.Time{})
				srcErr = nil
			}
			if srcErr != nil {
				p.upEnded = true
				p.mu.Unlock()
				if srcErr == io.EOF {
					closeWrite(dst) // the upload's EOF goes to its destination
				} else {
					p.Abort()
				}
				return
			}
			p.mu.Unlock()
		}

		// Frozen: wait for the peer's count and the new tunnel (or the flow's end).
		select {
		case <-installCh:
		case <-p.abortCh:
			return
		}
		p.mu.Lock()
		dst = p.next // published by Install before installCh was closed
		p.upDst = dst
		p.mu.Unlock()
		p.switched(true)
	}
}

func (p *PumpSwapper) pumpTunnelToApp(usage *web.Usage, remotePort int, sniffer bool) {
	app := p.app
	bufPtr := copyBufferPool.Get().(*[]byte)
	buf := *bufPtr
	defer copyBufferPool.Put(bufPtr)

	p.mu.Lock()
	src := p.cur
	p.mu.Unlock()

	for { // one pass per tunnel the flow is carried on
		// src -> app. Before Install there is no limit (the peer can never have
		// written more than its final count); after it, never read past the limit.
		for {
			p.mu.Lock()
			installed, limit := p.installed && !p.dlSwitched, p.dlLimit
			p.mu.Unlock()
			total := p.dlBytes.Load()
			if installed {
				if total > limit {
					p.Abort() // the peer promised fewer bytes than were already delivered
					return
				}
				if total == limit {
					break
				}
			}

			toRead := len(buf)
			if installed {
				if rem := limit - total; rem < uint64(toRead) {
					toRead = int(rem)
				}
			}
			n, err := src.Read(buf[:toRead])
			if n > 0 {
				w, werr := writeFull(app, buf[:n])
				p.dlBytes.Add(uint64(w))
				if sniffer {
					usage.AddOrUpdatePort(remotePort, uint64(n))
				}
				if werr != nil {
					p.Abort()
					return
				}
				p.noteDelivered()
			}
			if err == nil {
				continue
			}
			if isTimeout(err) {
				_ = src.SetReadDeadline(time.Time{}) // Install's wakeup; re-evaluate
				continue
			}

			// The stream ended or failed. If our upload is already frozen this is
			// the peer releasing its old tunnel after its own transition, which can
			// overtake our Install: the promised count is not known yet, so decide
			// once it is. It need not look like a clean EOF: a peer that closes an
			// enveloped tunnel without an END surfaces as an error (unexpected EOF,
			// or its ABORT). A genuinely failed tunnel still ends the flow: the
			// handshake that is waiting for the Install is itself bounded. Otherwise
			// this direction is over.
			p.mu.Lock()
			waitInstall := !p.installed && p.frozen
			installCh := p.installCh
			p.mu.Unlock()
			if waitInstall {
				select {
				case <-installCh:
				case <-p.abortCh:
					return
				}
			}
			p.mu.Lock()
			installed, limit = p.installed && !p.dlSwitched, p.dlLimit
			p.mu.Unlock()
			total = p.dlBytes.Load()
			switch {
			case installed && total >= limit:
				// The whole promised prefix arrived; a late EOF/error on the
				// stream is irrelevant.
			case !installed && err == io.EOF:
				p.mu.Lock()
				p.dlEnded = true
				p.mu.Unlock()
				if p.replay != nil {
					p.kickAck() // the peer is still waiting to learn how much arrived
				}
				closeWrite(app) // clean end of the download
				return
			default:
				p.Abort() // truncation or transport error
				return
			}
			break
		}

		p.mu.Lock()
		src = p.next
		p.mu.Unlock()
		p.switched(false)
	}
}

// --- replay (level B) ----------------------------------------------------------

// ackConn is a tunnel that can carry the replay protocol's ACK records: the
// half-close envelope.
type ackConn interface {
	SendAck(off uint64) error
	SetAckHandler(fn func(uint64))
}

// replayState is what a flow carries when it can be resumed after its tunnel
// dies: the unacknowledged bytes it sent, and the bookkeeping for acknowledging
// what it received.
type replayState struct {
	ring     *replayRing
	ackEvery uint64 // acknowledge after this many newly delivered bytes ...

	ackSent  atomic.Uint64 // highest offset acknowledged to the peer so far
	ackBusy  atomic.Bool   // a flushAck is running
	timerSet atomic.Bool   // a delayed ack is armed
}

// ackDelay bounds how long delivered bytes go unacknowledged when too few arrive
// to reach ackEvery, so the peer's replay buffer does not hold them for ever.
const ackDelay = 200 * time.Millisecond

// minReplayLimit keeps the limit a few times the pumps' read size (64 KiB): the
// sender waits for room for a whole read, so a limit close to it would make the
// ring drain completely before every read.
const minReplayLimit = 256 << 10

// EnableReplay makes the flow keep up to limit unacknowledged bytes it sends, and
// acknowledge what it receives, over tunnels that carry ACK records. It must be
// called before Start; it fails if the current tunnel cannot carry ACKs.
func (p *PumpSwapper) EnableReplay(limit int) error {
	if limit < minReplayLimit {
		limit = minReplayLimit
	}
	if _, ok := p.cur.(ackConn); !ok {
		return errors.New("replay needs a tunnel that carries ACK records")
	}
	p.replay = &replayState{ring: newReplayRing(limit), ackEvery: uint64(limit / 16)}
	return nil
}

// ReplayLen is the number of sent bytes not yet acknowledged (0 without replay).
func (p *PumpSwapper) ReplayLen() int {
	if p.replay == nil {
		return 0
	}
	return p.replay.ring.len()
}

// attachAcks routes the ACK records arriving on c to the replay ring.
func (p *PumpSwapper) attachAcks(c net.Conn) {
	if ac, ok := c.(ackConn); ok {
		ac.SetAckHandler(p.replay.ring.ackTo)
	}
}

// noteDelivered is called after bytes were delivered to the app: acknowledge them
// soon, in batches, from a goroutine of their own so that a tunnel that cannot
// take the ACK right now never stalls the download that produced it.
func (p *PumpSwapper) noteDelivered() {
	r := p.replay
	if r == nil {
		return
	}
	if p.dlBytes.Load()-r.ackSent.Load() >= r.ackEvery {
		p.kickAck()
		return
	}
	p.armAckTimer()
}

// armAckTimer makes sure delivered-but-unacknowledged bytes are acknowledged within
// ackDelay even if no more arrive.
func (p *PumpSwapper) armAckTimer() {
	r := p.replay
	if r.timerSet.CompareAndSwap(false, true) {
		time.AfterFunc(ackDelay, func() {
			r.timerSet.Store(false)
			p.kickAck()
		})
	}
}

func (p *PumpSwapper) kickAck() {
	r := p.replay
	if r.ackBusy.CompareAndSwap(false, true) {
		go p.flushAck()
	}
}

// flushAck sends the acknowledgement for everything delivered so far on the
// tunnel the upload direction writes to (the peer reads it from there), and again
// if more was delivered meanwhile.
func (p *PumpSwapper) flushAck() {
	r := p.replay
	for {
		if dl := p.dlBytes.Load(); dl > r.ackSent.Load() {
			p.mu.Lock()
			c := p.upDst
			p.mu.Unlock()
			ac, ok := c.(ackConn)
			if !ok || ac.SendAck(dl) != nil {
				r.ackBusy.Store(false) // a dead tunnel is for the resume logic to notice
				return
			}
			r.ackSent.Store(dl)
		}
		r.ackBusy.Store(false)
		// More may have been delivered while this one was being sent. A kick that
		// arrived meanwhile was dropped (busy), so whatever is left must either be
		// sent now or get its own timer: nothing else will remember it.
		rest := p.dlBytes.Load() - r.ackSent.Load()
		if rest == 0 {
			return
		}
		if rest < r.ackEvery {
			p.armAckTimer()
			return
		}
		if !r.ackBusy.CompareAndSwap(false, true) {
			return
		}
	}
}

// waitReplayRoom waits until the replay ring can take n more bytes. It gives up
// waiting when a freeze is pending: the sender must be able to reach its boundary
// without ACKs (they may only arrive after the swap), so the ring may exceed its
// limit by this one read. It reports false when the flow was aborted.
func (p *PumpSwapper) waitReplayRoom(n int) bool {
	for {
		err := p.replay.ring.waitFree(p.ctx, p.abortCh, p.freezeWake, n)
		switch {
		case err == nil:
			return true
		case err == errReplayWake:
			p.mu.Lock()
			pending := p.freezeReq && !p.frozen
			p.mu.Unlock()
			if pending {
				return true
			}
			// A stale nudge from a freeze that has since been withdrawn.
		default:
			return false
		}
	}
}
