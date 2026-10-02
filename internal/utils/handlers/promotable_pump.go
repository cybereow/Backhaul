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

// errTunnelNoAcks: a flow with replay can only move onto tunnels that carry ACK
// records (the half-close envelope); anything else would stall once its ring fills.
var errTunnelNoAcks = errors.New("promotable pump: replay needs a tunnel that carries ACK records")

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
	replay      *replayState
	replayGrow  func(cur int) int // nil: the ring keeps its size
	freezeWake  chan struct{}     // FreezeUp nudges an upload pump parked on a full replay ring
	upDst       net.Conn          // the tunnel the upload pump writes to (guarded by mu); acks go there too
	proxyHeader []byte            // the PROXY protocol header written before Start: payload that replay must retain

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

	// Suspension (level B): the tunnel died without the flow ending. See Suspend.
	suspended    bool
	suspendGen   uint64
	suspendCh    chan struct{} // closed while suspended; replaced when the flow resumes
	resumeCh     chan struct{} // closed by ResumeFinish to release the parked pumps
	parkedCh     chan struct{} // closed once every live pump is parked
	parkedClosed bool
	upParked     bool
	dlParked     bool
	resumeConn   net.Conn // the tunnel the parked pumps carry on with
	resumeFrom   uint64   // the peer's received offset: replay starts there
	resumeWindow time.Duration
	resumes      uint64

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
		// A flushAck already running on the old tunnel notices the new generation
		// and neither records its result nor gives up on it.
		p.replay.resetAcked()
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
		suspendCh:  make(chan struct{}),
		resumeCh:   make(chan struct{}),
		parkedCh:   make(chan struct{}),
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
			p.proxyHeader = header // a resume may have to send it again
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
		p.app.Close()
		p.lingerForAcks()
		close(p.done)
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
	return !p.aborted && !p.suspended && !p.freezeReq && !(p.upEnded && p.dlEnded)
}

// FreezeUp stops the upload direction at a write boundary and returns the final
// number of bytes committed to the current tunnel (counted from the start of the
// flow). It blocks until the upload pump has completed any in-flight write,
// bounded by ctx. On failure it withdraws the request: the flow keeps running on
// its current tunnel (ErrPromoteUnavailable, ctx error, or an aborted/finished
// flow). A swap that is still in progress also yields ErrPromoteUnavailable.
func (p *PumpSwapper) FreezeUp(ctx context.Context) (uint64, error) {
	p.mu.Lock()
	if p.aborted || p.suspended || p.freezeReq || (p.upEnded && p.dlEnded) {
		err := fmt.Errorf("%w (aborted=%v suspended=%v finished=%v swap pending=%v)", ErrPromoteUnavailable, p.aborted, p.suspended, p.upEnded && p.dlEnded, p.freezeReq)
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
	sc := p.suspendCh
	p.mu.Unlock()

	var cause error
	select {
	case <-ack:
		p.mu.Lock()
		n := p.upLimit
		p.mu.Unlock()
		return n, nil
	case <-sc:
		cause = ErrPromoteUnavailable // the tunnel died: the swap was cancelled
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
	if !p.suspended { // a suspension's own wakeup must stay armed
		p.freezeReq = false
		_ = p.app.SetReadDeadline(time.Time{})
	}
	return 0, cause
}

// Install hands over the new tunnel after a successful freeze. dlLimit is the
// peer's committed byte count, from the start of the flow: exactly that many
// bytes in total are delivered before the download switches. An error means the
// flow ended or was aborted meanwhile; the caller must then Abort (post-freeze).
func (p *PumpSwapper) Install(newTunnel net.Conn, dlLimit uint64) error {
	if p.replay != nil {
		if _, ok := newTunnel.(ackConn); !ok {
			return errTunnelNoAcks
		}
	}
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
	fctx, cancel := context.WithTimeout(ctx, PromoteHandshakeTimeout)
	own, err := p.FreezeUp(fctx)
	cancel()
	if err != nil {
		closeAll(legs)
		return err
	}
	return p.PromoteFrozen(ctx, own, legs, build)
}

// PromoteFrozen finishes a swap whose upload FreezeUp already stopped (own is its
// result). A side that must decide whether to accept a swap uses this to reserve
// the flow first: once FreezeUp succeeded the flow cannot be refused any more, so
// a failure from here on aborts it.
func (p *PumpSwapper) PromoteFrozen(ctx context.Context, own uint64, legs []net.Conn, build func() (net.Conn, error)) error {
	ctx, cancel := context.WithTimeout(ctx, PromoteHandshakeTimeout)
	defer cancel()

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
		if !p.Suspend() { // a flow that can be resumed survives a failed swap
			p.Abort()
		}
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
	pend := 0        // bytes of buf read from the app but not yet retained or sent

	for { // one pass per tunnel the flow is carried on
		var installCh, suspendCh chan struct{}

		// app -> dst, until the app ends or a freeze is requested.
	run:
		for {
			if srcErr == nil || pend > 0 {
				var n int
				var err error
				if pend > 0 {
					// Bytes read before a freeze or suspension that could not be sent
					// then go first, on whichever tunnel the flow is on now.
					n, err = pend, srcErr
					pend, srcErr = 0, nil
				} else {
					n, err = app.Read(buf)
				}
				if n > 0 {
					interrupted := false
					if p.replay != nil {
						// Keep what is being sent until the peer acknowledges it, and
						// wait here (not in the app's socket) while a full window of it
						// is unacknowledged.
						var ok bool
						if ok, interrupted = p.waitReplayRoom(n); !ok {
							p.Abort()
							return
						}
						if interrupted {
							// A swap or suspension needs this pump at its boundary, and
							// the ACKs that would make room only come after it: hold the
							// read instead of letting the ring grow past its limit.
							pend = n
						} else {
							p.replay.ring.append(buf[:n])
						}
					}
					if !interrupted {
						w, werr := writeFull(dst, buf[:n])
						p.upBytes.Add(uint64(w))
						if sniffer {
							usage.AddOrUpdatePort(remotePort, uint64(n))
						}
						if werr != nil && !p.Suspend() {
							p.Abort()
							return
						}
					}
				}
				srcErr = err
			}

			p.mu.Lock()
			if p.suspended {
				p.mu.Unlock()
				ndst, ok := p.parkUp()
				if !ok {
					return
				}
				dst = ndst
				if isTimeout(srcErr) {
					srcErr = nil
				}
				continue run
			}
			if p.freezeReq && !p.frozen {
				// Boundary: every byte read from the app so far is written to the
				// tunnel, so UpBytes is final. A timeout here is our own wakeup.
				p.frozen = true
				p.upLimit = p.upBytes.Load()
				_ = app.SetReadDeadline(time.Time{})
				ack := p.upAck
				installCh = p.installCh
				suspendCh = p.suspendCh
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
			if pend > 0 {
				// The freeze that interrupted the send was withdrawn: send it now.
				p.mu.Unlock()
				continue run
			}
			if srcErr != nil {
				p.upEnded = true
				p.checkParkedLocked()
				p.mu.Unlock()
				if srcErr == io.EOF {
					// The upload's EOF goes to its destination. If that fails on a
					// flow that can resume, the resume sends it again (the upload is
					// marked ended, so ResumeFinish replays and re-ends it).
					if err := closeWriteErr(dst); err != nil && p.replay != nil {
						p.Suspend()
					}
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
		case <-suspendCh:
		case <-p.abortCh:
			return
		}
		p.mu.Lock()
		if p.suspended {
			// The tunnel died and the swap with it: carry on after the resume.
			p.mu.Unlock()
			ndst, ok := p.parkUp()
			if !ok {
				return
			}
			dst = ndst
			continue
		}
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

dl:
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
			installCh, suspendCh := p.installCh, p.suspendCh
			p.mu.Unlock()
			if waitInstall {
				select {
				case <-installCh:
				case <-suspendCh: // the swap died with the tunnel
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
				p.checkParkedLocked()
				p.mu.Unlock()
				if p.replay != nil {
					p.kickAck() // the peer is still waiting to learn how much arrived
				}
				closeWrite(app) // clean end of the download
				return
			default:
				// A tunnel that failed (not one the peer aborted on purpose) does not
				// end a flow that can be resumed on another.
				if p.replay != nil && !errors.Is(err, errHalfCloseAborted) && p.Suspend() {
					nsrc, ok := p.parkDl()
					if !ok {
						return
					}
					src = nsrc
					continue dl
				}
				p.Abort() // truncation or transport error
				return
			}
			break
		}

		p.mu.Lock()
		if p.suspended {
			p.mu.Unlock()
			nsrc, ok := p.parkDl()
			if !ok {
				return
			}
			src = nsrc
			continue
		}
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

	ackSent atomic.Uint64 // highest offset acknowledged to the peer so far
	ackGen  atomic.Uint64 // bumped when ackSent is reset by a swap
	ackMu   sync.Mutex    // makes "record the ack" and "reset by a swap" exclusive
	ackBusy atomic.Bool   // a flushAck is running
	// endAckSent: the ACK that told the peer this side's download ended has been
	// sent on the current tunnel. endAcked: the peer's said the same of ours.
	endAckSent atomic.Bool
	endAcked   atomic.Bool
	timerSet   atomic.Bool // a delayed ack is armed
}

// resetAcked forgets what was acknowledged: the new tunnel has not heard any of it.
func (r *replayState) resetAcked() {
	r.ackMu.Lock()
	r.ackGen.Add(1)
	r.ackSent.Store(0)
	r.endAckSent.Store(false)
	r.ackMu.Unlock()
}

// noteAcked records that dl was acknowledged, unless a swap reset the bookkeeping
// since gen was read (the ack then went to a tunnel that is gone).
func (r *replayState) noteAcked(gen, dl uint64, ended bool) {
	r.ackMu.Lock()
	if r.ackGen.Load() == gen {
		r.ackSent.Store(dl)
		if ended {
			r.endAckSent.Store(true)
		}
	}
	r.ackMu.Unlock()
}

// endAckFlag, set in the offset of an ACK, says the sender's download has ended:
// the peer's END arrived, so the peer need not resend it after a cut.
const endAckFlag = 1 << 63

// ackDelay bounds how long delivered bytes go unacknowledged when too few arrive
// to reach ackEvery, so the peer's replay buffer does not hold them for ever.
const ackDelay = 200 * time.Millisecond

// minReplayLimit keeps the limit a few times the pumps' read size (64 KiB): the
// sender waits for room for a whole read, so a limit close to it would make the
// ring drain completely before every read.
const minReplayLimit = 256 << 10

// replayAckEvery: a flow acknowledges after this many delivered bytes. It is
// independent of the ring size, because the peer's ring may be far bigger than
// ours: acknowledging only every limit/16 of a small ring would starve a peer whose
// ring has grown.
const replayAckEvery = 64 << 10

// SetReplayGrower lets the replay ring grow when the sender finds it full: grow
// gets the current limit and returns the new one (the same value = no more room).
// The ring's size bounds the flow's rate to about limit per round trip, so a flow
// that actually needs a bigger one gets it, within the process-wide budget, while
// idle ones stay small. Call before Start.
func (p *PumpSwapper) SetReplayGrower(grow func(cur int) int) { p.replayGrow = grow }

// growReplay tries to enlarge the full replay ring; it reports whether it did.
func (p *PumpSwapper) growReplay() bool {
	if p.replayGrow == nil {
		return false
	}
	cur := p.replay.ring.getLimit()
	if next := p.replayGrow(cur); next > cur {
		p.replay.ring.setLimit(next)
		return true
	}
	return false
}

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
	ring := newReplayRing(limit)
	if len(p.proxyHeader) > 0 {
		ring.append(p.proxyHeader) // already written, and counted in UpBytes: keep it until the peer has it
	}
	p.replay = &replayState{ring: ring, ackEvery: replayAckEvery}
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
		r := p.replay
		ac.SetAckHandler(func(off uint64) {
			if off&endAckFlag != 0 {
				off &^= endAckFlag
				r.endAcked.Store(true)
			}
			r.ring.ackTo(off)
		})
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
		gen := r.ackGen.Load()
		p.mu.Lock()
		ended := p.dlEnded
		p.mu.Unlock()
		if dl := p.dlBytes.Load(); dl > r.ackSent.Load() || (ended && !r.endAckSent.Load()) {
			p.mu.Lock()
			c := p.upDst
			p.mu.Unlock()
			v := dl
			if ended {
				v |= endAckFlag // also tells the peer its END arrived
			}
			ac, ok := c.(ackConn)
			if !ok || ac.SendAck(v) != nil {
				if r.ackGen.Load() != gen {
					continue // the tunnel it failed on was just replaced: send on the new one
				}
				r.ackBusy.Store(false) // a dead tunnel is for the resume logic to notice
				return
			}
			r.noteAcked(gen, dl, ended)
		}
		r.ackBusy.Store(false)
		// More may have been delivered while this one was being sent. A kick that
		// arrived meanwhile was dropped (busy), so whatever is left must either be
		// sent now or get its own timer: nothing else will remember it.
		p.mu.Lock()
		ended = p.dlEnded // may have ended while the ACK above was in flight
		p.mu.Unlock()
		rest := p.dlBytes.Load() - r.ackSent.Load()
		if rest == 0 && (!ended || r.endAckSent.Load()) {
			return
		}
		if rest < r.ackEvery && !(ended && !r.endAckSent.Load()) {
			p.armAckTimer()
			return
		}
		if !r.ackBusy.CompareAndSwap(false, true) {
			return
		}
	}
}

// waitReplayRoom waits until the replay ring can take n more bytes. It stops
// waiting (interrupted) when a freeze or suspension is pending: the sender must be
// able to reach its boundary without ACKs, which may only arrive after the swap or
// resume, so the caller holds its read back rather than overfilling the ring. ok is
// false when the flow was aborted.
func (p *PumpSwapper) waitReplayRoom(n int) (ok, interrupted bool) {
	for {
		p.mu.Lock()
		suspended := p.suspended
		p.mu.Unlock()
		if suspended {
			return true, true // nothing can be acknowledged until the flow resumes
		}
		if p.replay.ring.free() < n && p.growReplay() {
			continue // a bigger ring may already fit it
		}
		err := p.replay.ring.waitFree(p.ctx, p.abortCh, p.freezeWake, n)
		switch {
		case err == nil:
			return true, false
		case err == errReplayWake:
			p.mu.Lock()
			pending := (p.freezeReq && !p.frozen) || p.suspended
			p.mu.Unlock()
			if pending {
				return true, true
			}
			// A stale nudge from a freeze that has since been withdrawn.
		default:
			return false, false
		}
	}
}
