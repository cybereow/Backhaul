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
// its plain (old) tunnel to a striped (new) one mid-stream without dropping or
// duplicating a byte.
//
// Byte ownership: the upload pump exclusively owns app reads and old-tunnel
// writes; the download pump exclusively owns old-tunnel reads and app writes.
// The state below is guarded by mu, which is never held across I/O (only
// non-blocking SetReadDeadline calls, used to wake an owner that is parked in a
// blocking Read).
//
// Transition:
//  1. FreezeUp asks the upload pump to stop at its next write boundary. The pump
//     finishes any in-flight old-tunnel write, then acks; the acked count is the
//     final number of bytes committed to the old tunnel.
//  2. The peer's count arrives (raw legs, before any striped wrapper exists) and
//     Install hands over the new tunnel. The download pump then delivers exactly
//     dlLimit bytes from the old tunnel and switches; the upload pump starts on
//     the new tunnel.
//  3. Once both directions have switched, the old tunnel is released.
//
// Failing before the freeze ack leaves the flow plain. Failing after it is not
// recoverable (the peer may have frozen) and ends in Abort.
type PumpSwapper struct {
	app net.Conn
	old net.Conn // plain tunnel, immutable

	upBytes atomic.Uint64
	dlBytes atomic.Uint64
	phases  atomic.Int32 // directions still on the old tunnel
	done    chan struct{}

	mu        sync.Mutex
	newTunnel net.Conn
	freezeReq bool // FreezeUp asked; upload pump must stop at the next boundary
	frozen    bool // upload pump acked; upLimit is final
	installed bool
	ended     bool // a direction finished on the old tunnel: no promotion any more
	aborted   bool
	upLimit   uint64
	dlLimit   uint64

	upAck     chan struct{} // closed when the upload pump acks the freeze
	installCh chan struct{} // closed by Install
	abortCh   chan struct{} // closed by Abort

	oldOnce sync.Once
}

func (p *PumpSwapper) DoneWait() <-chan struct{} {
	return p.done
}

func (p *PumpSwapper) UpBytes() uint64 { return p.upBytes.Load() }
func (p *PumpSwapper) DlBytes() uint64 { return p.dlBytes.Load() }

// phaseDone records that one direction has switched to the new tunnel; the last
// one releases the old tunnel, outside any lock.
func (p *PumpSwapper) phaseDone() {
	if p.phases.Add(-1) == 0 {
		go p.closeOld()
	}
}

func (p *PumpSwapper) closeOld() {
	p.oldOnce.Do(func() { p.old.Close() })
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
	conns := [3]net.Conn{p.app, p.old, p.newTunnel}
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
func PromotablePump(
	ctx context.Context, proxyProtocol bool, app net.Conn, tunnel net.Conn,
	logger *logrus.Logger, usage *web.Usage, remotePort int, sniffer bool,
) *PumpSwapper {

	if proxyProtocol {
		if err := WriteProxyProtocol(app, tunnel); err != nil {
			logger.Error(err)
			app.Close()
			tunnel.Close()
			return nil
		}
	}

	p := &PumpSwapper{
		app:       app,
		old:       tunnel,
		done:      make(chan struct{}),
		upAck:     make(chan struct{}),
		installCh: make(chan struct{}),
		abortCh:   make(chan struct{}),
	}
	p.phases.Store(2)

	go func() {
		select {
		case <-ctx.Done():
			p.Abort()
		case <-p.done:
		}
	}()

	var wg sync.WaitGroup
	wg.Add(2)

	// App -> Tunnel (Upload direction relative to the plain tunnel's writes)
	go func() {
		defer wg.Done()
		p.pumpAppToTunnel(usage, remotePort, sniffer)
	}()

	// Tunnel -> App (Download direction relative to the plain tunnel's reads)
	go func() {
		defer wg.Done()
		p.pumpTunnelToApp(usage, remotePort, sniffer)
	}()

	go func() {
		wg.Wait()
		close(p.done)
		app.Close()
		p.closeOld()
		p.mu.Lock()
		nt := p.newTunnel
		p.mu.Unlock()
		if nt != nil {
			nt.Close()
		}
	}()

	return p
}

// FreezeUp stops the upload direction at a write boundary and returns the final
// number of bytes committed to the old tunnel. It blocks until the upload pump
// has completed any in-flight old-tunnel write, bounded by ctx. On failure it
// withdraws the request: the flow keeps running plain (ErrPromoteUnavailable,
// ctx error, or an aborted/finished flow).
func (p *PumpSwapper) FreezeUp(ctx context.Context) (uint64, error) {
	p.mu.Lock()
	if p.aborted || p.ended || p.freezeReq {
		err := fmt.Errorf("%w (aborted=%v ended=%v freeze pending=%v)", ErrPromoteUnavailable, p.aborted, p.ended, p.freezeReq)
		p.mu.Unlock()
		return 0, err
	}
	p.freezeReq = true
	// The upload pump may be parked in app.Read: wake it. It owns that read
	// direction and clears the deadline itself.
	_ = p.app.SetReadDeadline(time.Unix(1, 0))
	p.mu.Unlock()

	var cause error
	select {
	case <-p.upAck:
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
	case <-p.upAck: // the ack raced the failure: the freeze did happen
		return p.upLimit, nil
	default:
	}
	p.freezeReq = false
	_ = p.app.SetReadDeadline(time.Time{})
	return 0, cause
}

// Install hands over the new tunnel after a successful freeze. dlLimit is the
// peer's committed old-tunnel byte count: exactly that many bytes are delivered
// from the old tunnel before the download switches. An error means the flow
// ended or was aborted meanwhile; the caller must then Abort (post-freeze).
func (p *PumpSwapper) Install(newTunnel net.Conn, dlLimit uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.aborted || p.ended || !p.frozen || p.installed {
		return ErrPromoteUnavailable
	}
	p.newTunnel = newTunnel
	p.dlLimit = dlLimit
	p.installed = true
	// A download read already sitting at the boundary would wait forever for
	// bytes the peer will never send: wake it.
	_ = p.old.SetReadDeadline(time.Unix(1, 0))
	close(p.installCh)
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
	app, old := p.app, p.old
	bufPtr := copyBufferPool.Get().(*[]byte)
	buf := *bufPtr
	defer copyBufferPool.Put(bufPtr)

	var total uint64 // bytes committed to the old tunnel
	var srcErr error // how the app read ended; delivered to whichever tunnel is current

	// Phase 1: app -> old tunnel, until the app ends or a freeze is requested.
	for {
		if srcErr == nil {
			n, err := app.Read(buf)
			if n > 0 {
				w, werr := writeFull(old, buf[:n])
				total += uint64(w)
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
		if p.freezeReq {
			// Boundary: every byte read from the app so far is written to the old
			// tunnel, so total is final. A timeout here is our own wakeup.
			p.frozen = true
			p.upLimit = total
			_ = app.SetReadDeadline(time.Time{})
			close(p.upAck)
			p.mu.Unlock()
			if isTimeout(srcErr) {
				srcErr = nil
			}
			break
		}
		if srcErr != nil && isTimeout(srcErr) {
			// A wakeup whose freeze was withdrawn: not an application error.
			_ = app.SetReadDeadline(time.Time{})
			srcErr = nil
		}
		if srcErr != nil {
			p.ended = true
			p.mu.Unlock()
			if srcErr == io.EOF {
				closeWrite(old) // the upload's EOF goes to its destination
			} else {
				p.Abort()
			}
			return
		}
		p.mu.Unlock()
	}

	// Frozen: wait for the peer's count and the new tunnel (or the flow's end).
	select {
	case <-p.installCh:
	case <-p.abortCh:
		return
	}
	dst := p.newTunnel // published by Install before installCh was closed
	p.phaseDone()

	// Phase 2: app -> new tunnel.
	for {
		if srcErr == nil {
			n, err := app.Read(buf)
			if n > 0 {
				_, werr := writeFull(dst, buf[:n])
				p.upBytes.Add(uint64(n))
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
		if srcErr != nil {
			if srcErr == io.EOF {
				closeWrite(dst)
			} else {
				p.Abort()
			}
			return
		}
	}
}

func (p *PumpSwapper) pumpTunnelToApp(usage *web.Usage, remotePort int, sniffer bool) {
	app, old := p.app, p.old
	bufPtr := copyBufferPool.Get().(*[]byte)
	buf := *bufPtr
	defer copyBufferPool.Put(bufPtr)

	var total uint64 // bytes delivered to the app
	var src net.Conn

	// Phase 1: old tunnel -> app. Before Install there is no limit (the peer
	// can never have written more than its final count); after it, never read
	// past the limit.
	for {
		p.mu.Lock()
		installed, limit := p.installed, p.dlLimit
		p.mu.Unlock()
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
		n, err := old.Read(buf[:toRead])
		if n > 0 {
			w, werr := writeFull(app, buf[:n])
			total += uint64(w)
			p.dlBytes.Add(uint64(w))
			if sniffer {
				usage.AddOrUpdatePort(remotePort, uint64(n))
			}
			if werr != nil {
				p.Abort()
				return
			}
		}
		if err == nil {
			continue
		}
		if isTimeout(err) {
			_ = old.SetReadDeadline(time.Time{}) // Install's wakeup; re-evaluate
			continue
		}

		// The old stream ended. If our upload is already frozen this is the
		// peer releasing its old tunnel after its own transition, which can
		// overtake our Install: the promised count is not known yet, so decide
		// once it is. Otherwise the plain flow is over.
		p.mu.Lock()
		waitInstall := !p.installed && p.frozen && err == io.EOF
		if !p.installed && !waitInstall {
			p.ended = true
		}
		p.mu.Unlock()
		if waitInstall {
			select {
			case <-p.installCh:
			case <-p.abortCh:
				return
			}
		}
		p.mu.Lock()
		installed, limit = p.installed, p.dlLimit
		p.mu.Unlock()
		switch {
		case installed && total >= limit:
			// The whole promised prefix arrived; a late EOF/error on the old
			// stream is irrelevant.
		case !installed && err == io.EOF:
			closeWrite(app) // clean end of the plain download
			return
		default:
			p.Abort() // truncation or transport error
			return
		}
		break
	}

	p.mu.Lock()
	src = p.newTunnel
	p.mu.Unlock()
	p.phaseDone()

	// Phase 2: new tunnel -> app.
	for {
		n, err := src.Read(buf)
		if n > 0 {
			_, werr := writeFull(app, buf[:n])
			p.dlBytes.Add(uint64(n))
			if sniffer {
				usage.AddOrUpdatePort(remotePort, uint64(n))
			}
			if werr != nil {
				p.Abort()
				return
			}
		}
		if err != nil {
			if err == io.EOF {
				closeWrite(app)
			} else {
				p.Abort()
			}
			return
		}
	}
}
