package handlers

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"
)

// DefaultResumeWindow is how long a suspended flow waits for a tunnel to resume
// on before it gives up and aborts.
const DefaultResumeWindow = 30 * time.Second

// ErrNotSuspended is returned by the resume calls on a flow that is not suspended.
var ErrNotSuspended = errors.New("promotable pump: flow is not suspended")

// Suspension and resume (level B).
//
// A flow with replay enabled does not end when its tunnel fails without an END or
// ABORT: it is suspended. Both pumps park (the upload pump stops reading the app,
// so the app only sees its socket buffers fill; the download pump stops
// delivering), the dead tunnel is dropped, and the flow waits, up to the resume
// window, for a new tunnel. Resuming is a handshake on that tunnel in which each
// side tells the other how many payload bytes it has delivered (ResumeBegin); each
// then replays, from its replay ring, everything from the other's count on
// (ResumeFinish). Nothing is lost or repeated because the counts are offsets from
// the start of the flow, the same ones swaps use.
//
// A flow whose peer aborted it on purpose, or whose own app failed, is not
// suspended: those end the flow as before.

// SetResumeWindow sets how long a suspended flow waits to be resumed; zero means
// DefaultResumeWindow. Call before Start.
func (p *PumpSwapper) SetResumeWindow(d time.Duration) { p.resumeWindow = d }

// Resumes is the number of times the flow resumed after a suspension.
func (p *PumpSwapper) Resumes() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.resumes
}

// SuspendedCh is closed while the flow is suspended and replaced by a fresh
// channel when it resumes: read it again after each resume.
func (p *PumpSwapper) SuspendedCh() <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.suspendCh
}

// Suspend takes the flow out of service until a new tunnel is resumed onto. It is
// what a failing pump does, and what the owner calls when it learns the tunnel is
// gone before the pumps do (its session died) or when the peer asks to resume a
// flow whose old tunnel still looks alive. It reports whether the flow is now
// suspended: false when it cannot be (no replay, or replay given up at a
// promotion; aborted; or finished).
func (p *PumpSwapper) Suspend() bool {
	p.mu.Lock()
	if p.replaying() == nil || p.aborted || (p.upEnded && p.dlEnded && p.replay.settled()) {
		p.mu.Unlock()
		return false
	}
	if p.suspended {
		p.mu.Unlock()
		return true
	}
	p.suspended = true
	p.suspendGen++
	gen := p.suspendGen
	old, next := p.cur, p.next

	// A swap in progress dies with the tunnel it was moving the flow off.
	p.freezeReq, p.frozen, p.installed = false, false, false
	p.upSwitched, p.dlSwitched = false, false
	p.next = nil
	p.upAck = make(chan struct{})
	p.installCh = make(chan struct{})
	p.upParked, p.dlParked = false, false
	p.parkedClosed = false
	p.parkedCh = make(chan struct{})
	close(p.suspendCh)
	p.checkParkedLocked() // a direction that already ended has nothing to park

	// Wake whatever the pumps are blocked in: the app read, a wait for ring room,
	// and a write the app is not taking - an app that has stopped reading (a
	// paused download) would otherwise keep the download pump from parking, and
	// the flow from resuming, for as long as it does. The tunnel read and write
	// fail on their own once the tunnel is dropped.
	_ = p.app.SetReadDeadline(time.Unix(1, 0))
	_ = p.app.SetWriteDeadline(time.Unix(1, 0))
	select {
	case p.freezeWake <- struct{}{}:
	default:
	}
	window := p.resumeWindow
	if window <= 0 {
		window = DefaultResumeWindow
	}
	p.mu.Unlock()

	dropTunnel(old)
	dropTunnel(next)
	time.AfterFunc(window, func() {
		p.mu.Lock()
		stale := !p.suspended || p.suspendGen != gen
		p.mu.Unlock()
		if !stale {
			p.Abort()
		}
	})
	return true
}

// dropTunnel closes a tunnel without telling the peer the flow is over.
func dropTunnel(c net.Conn) {
	if c == nil {
		return
	}
	if d, ok := c.(interface{ Drop() }); ok {
		d.Drop()
		return
	}
	c.Close()
}

// checkParkedLocked closes parkedCh once every pump that is still running is
// parked. The caller holds mu.
func (p *PumpSwapper) checkParkedLocked() {
	if !p.suspended || p.parkedClosed {
		return
	}
	if (p.upEnded || p.upParked) && (p.dlEnded || p.dlParked) {
		p.parkedClosed = true
		close(p.parkedCh)
	}
}

// settled reports that the peer has acknowledged every byte this side sent and
// the end of its upload, and that the peer has been told this side's download
// ended too: nothing is left that only a resume could deliver, and nobody waits
// for this side's last ACK.
func (r *replayState) settled() bool {
	return r.ring.len() == 0 && r.endAcked.Load() && r.endAckSent.Load()
}

// lingerForAcks keeps a resumable flow whose pumps have both finished alive until
// the peer has acknowledged everything it sent, bounded by the resume window: until
// then a cut tunnel would leave the peer short of bytes only this side can replay.
// It returns at once for a flow without replay or one that was aborted.
func (p *PumpSwapper) lingerForAcks() {
	if p.replaying() == nil {
		return
	}
	window := p.resumeWindow
	if window <= 0 {
		window = DefaultResumeWindow
	}
	deadline := time.NewTimer(window)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for !p.replay.settled() {
		select {
		case <-tick.C:
		case <-deadline.C:
			return
		case <-p.abortCh:
			return
		}
	}
}

// parkUp parks the upload pump until the flow resumes (or is aborted), then
// replays what the peer missed on the new tunnel and returns it. ok is false when
// the flow was aborted.
func (p *PumpSwapper) parkUp() (dst net.Conn, ok bool) {
	for {
		p.mu.Lock()
		_ = p.app.SetReadDeadline(time.Time{}) // the suspension's wakeup has done its job
		p.upParked = true
		p.checkParkedLocked()
		rc := p.resumeCh
		p.mu.Unlock()

		select {
		case <-rc:
		case <-p.abortCh:
			return nil, false
		}

		p.mu.Lock()
		dst, from := p.resumeConn, p.resumeFrom
		p.mu.Unlock()
		if err := p.replayTo(dst, from); err != nil {
			if p.Suspend() {
				continue // the new tunnel died while replaying: wait for another
			}
			p.Abort()
			return nil, false
		}
		return dst, true
	}
}

// parkDl parks the download pump until the flow resumes and returns the new
// tunnel to read from.
func (p *PumpSwapper) parkDl() (src net.Conn, ok bool) {
	p.mu.Lock()
	_ = p.app.SetWriteDeadline(time.Time{}) // the suspension's wakeup has done its job
	p.dlParked = true
	p.checkParkedLocked()
	rc := p.resumeCh
	p.mu.Unlock()

	select {
	case <-rc:
	case <-p.abortCh:
		return nil, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.resumeConn, true
}

// replayTo sends dst every payload byte from flow offset from up to everything
// the flow has read from its app, and records that as committed.
func (p *PumpSwapper) replayTo(dst net.Conn, from uint64) error {
	// A peer that stops reading must not hold the replay forever: a failure here
	// suspends the flow again and the next attempt starts over.
	_ = dst.SetWriteDeadline(time.Now().Add(PromoteHandshakeTimeout))
	defer dst.SetWriteDeadline(time.Time{})
	ring := p.replay.ring
	end := ring.end()
	bufPtr := copyBufferPool.Get().(*[]byte)
	defer copyBufferPool.Put(bufPtr)
	buf := *bufPtr
	for off := from; off < end; {
		n := ring.readAt(off, buf)
		if n == 0 {
			return fmt.Errorf("replay: offset %d is no longer retained", off)
		}
		n = int(min(uint64(n), end-off))
		if _, err := writeFull(dst, buf[:n]); err != nil {
			return err
		}
		off += uint64(n)
	}
	p.upBytes.Store(end)
	return nil
}

// ResumeBegin waits until every pump of the suspended flow is parked and returns
// the number of payload bytes it has delivered to the app: what the peer must
// replay from. It changes nothing, so a resume that fails afterwards can simply be
// tried again on another tunnel (until the resume window ends).
func (p *PumpSwapper) ResumeBegin(ctx context.Context) (uint64, error) {
	p.mu.Lock()
	if p.aborted || !p.suspended {
		p.mu.Unlock()
		return 0, ErrNotSuspended
	}
	pc := p.parkedCh
	p.mu.Unlock()
	select {
	case <-pc:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-p.abortCh:
		return 0, ErrNotSuspended
	}
	return p.dlBytes.Load(), nil
}

// ResumeFinish puts the suspended flow on tunnel, whose peer has delivered
// peerRecv bytes of this side's upload. It returns an error, and aborts the flow,
// when peerRecv cannot be right (beyond what was sent, or older than the replay
// ring retains); a failure of the tunnel itself leaves the flow suspended.
func (p *PumpSwapper) ResumeFinish(tunnel net.Conn, peerRecv uint64) error {
	if _, ok := tunnel.(ackConn); !ok {
		return errTunnelNoAcks
	}
	p.mu.Lock()
	if p.aborted || !p.suspended {
		p.mu.Unlock()
		return ErrNotSuspended
	}
	ring := p.replay.ring
	if end, base := ring.end(), ring.firstOffset(); peerRecv > end || peerRecv < base {
		p.mu.Unlock()
		p.Abort()
		return fmt.Errorf("resume: peer claims %d bytes received, this side retains %d..%d", peerRecv, base, end)
	}
	ring.ackTo(peerRecv) // what the peer already has need not be kept

	p.cur, p.upDst = tunnel, tunnel
	p.attachAcks(tunnel)
	if p.dlEnded {
		if sv, ok := tunnel.(interface{ ServeAcks() }); ok {
			sv.ServeAcks() // nobody else reads this tunnel
		}
	}
	p.resumeConn, p.resumeFrom = tunnel, peerRecv
	p.suspended = false
	p.upParked, p.dlParked = false, false
	p.suspendCh = make(chan struct{})
	rc := p.resumeCh
	p.resumeCh = make(chan struct{})
	p.resumes++
	upEnded := p.upEnded
	p.mu.Unlock()

	close(rc)
	p.replay.resetAcked()
	p.kickAck()

	if upEnded {
		// No upload pump is left to replay and end the upload on the new tunnel.
		err := p.replayTo(tunnel, peerRecv)
		if err == nil {
			err = closeWriteErr(tunnel) // a lost END must be retried like lost data
		}
		if err != nil && !p.Suspend() {
			p.Abort()
		}
		return err
	}
	return nil
}

// Resume runs the whole handshake on a fresh raw leg, the counterpart of Promote:
// wait for the pumps to park, exchange delivered counts on the leg, wrap it, and
// resume onto it. The leg is closed on failure; the flow stays suspended for
// another attempt unless the counts are inconsistent.
func (p *PumpSwapper) Resume(ctx context.Context, leg net.Conn, build func() (net.Conn, error)) error {
	ctx, cancel := context.WithTimeout(ctx, PromoteHandshakeTimeout)
	defer cancel()

	own, err := p.ResumeBegin(ctx)
	if err != nil {
		leg.Close()
		return err
	}
	peer, err := exchangeCounts(ctx, leg, own)
	if err != nil {
		leg.Close()
		return err
	}
	tunnel, err := build()
	if err != nil {
		leg.Close()
		return err
	}
	if err := p.ResumeFinish(tunnel, peer); err != nil {
		dropTunnel(tunnel)
		return err
	}
	return nil
}
