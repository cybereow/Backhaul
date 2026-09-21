package transport

import (
	"fmt"
	"time"

	"github.com/musix/backhaul/internal/utils/network"
)

// controlGraceWindow is how long the pool is kept alive after the control
// channel drops while waiting for the client to reattach: long enough to ride
// out a CDN max-age reset plus a few dial retries, short enough that a client
// which really is gone doesn't leave a stale pool serving nothing.
// ponytail: a constant, not a knob - nothing to tune until a deployment needs
// a different window.
const controlGraceWindow = 30 * time.Second

// maxControlGraceHold caps the total time the pool is held with no control
// channel. Re-arming the grace window forever (as long as a session looks
// alive) is unsafe when smux keepalive is disabled: a silently-dropped client's
// sessions never report closed, so the tunnel would stay "Connected" with no
// working data path until a manual restart. Past this cap we restart regardless
// - a control channel absent this long means the client really is gone, and a
// clean rebuild reconnects it. Brief CDN control-channel flaps reattach in
// seconds and never reach the cap, so in-flight flows are still preserved.
const maxControlGraceHold = 90 * time.Second

// onControlLost handles a control channel that died on its own, as opposed to
// the client deliberately going away. Everything that actually carries traffic
// - the pool sessions, the port listeners, the handle loops - is independent of
// the control channel, so it all stays up and only the control channel is
// dropped. If the client hasn't reattached one within controlGraceWindow, fall
// back to the old behaviour and rebuild the whole transport.
//
// g is the generation the dying handler belongs to; the grace timer captures it
// (and the loss epoch) instead of rereading mutable state when it fires.
func (s *WsMuxTransport) onControlLost(g *wsGeneration, conn *network.WebSocketConn) {
	s.controlMu.Lock()
	if s.controlChannel != conn {
		// Already cleared, or the client has since reattached: this is a late
		// error from a connection nothing uses any more.
		s.controlMu.Unlock()
		conn.Close()
		return
	}
	s.controlChannel = nil

	// A new loss is a new epoch: whatever timer the previous one left is stale.
	s.invalidateGraceLocked()
	s.graceStart = time.Now()
	// Published in the same critical section as the loss (and as adoptControl
	// publishes Connected), so a late status can never overwrite a newer one.
	s.config.TunnelStatus = fmt.Sprintf("Reconnecting (%s)", s.config.Mode)
	if !g.isStopped() {
		// A stopped generation is being torn down; nothing to hold or restart.
		s.armControlGrace(g, s.graceEpoch)
	}
	s.controlMu.Unlock()

	conn.Close()
	s.recordEvent("control_lost", fmt.Sprintf("control channel dropped; holding pool up to %s for reattach (flows keep running)", controlGraceWindow))
	s.logger.Warnf("control channel lost, holding the pool for up to %s for the client to reattach", controlGraceWindow)
}

// invalidateGraceLocked ends the current loss epoch: a callback armed for it, or
// already running, will find the epoch changed and do nothing. Caller holds
// controlMu. Timer.Stop does not wait for a callback that has already started,
// which is why the epoch - not the stop - is what makes a stale callback inert.
func (s *WsMuxTransport) invalidateGraceLocked() {
	s.graceEpoch++
	if s.graceTimer != nil {
		s.graceTimer.Stop()
		s.graceTimer = nil
	}
}

// graceCurrentLocked reports whether a callback armed for (g, epoch) is still
// the one entitled to act: same loss epoch, still no control channel, and the
// generation not already stopping. Caller holds controlMu.
func (s *WsMuxTransport) graceCurrentLocked(g *wsGeneration, epoch uint64) bool {
	return s.graceEpoch == epoch && s.controlChannel == nil && !g.isStopped()
}

// adoptControl is the one place a new control channel is admitted, and it
// arbitrates against the grace callback's restart claim under controlMu: either
// the adoption lands first and invalidates the epoch (the callback then does
// nothing), or the restart claim landed first and the connection is refused, so
// it is never accepted into a generation that is committed to teardown (the
// client redials into the next one). ok is false when refused. stale is a
// control channel that was still registered and must be closed by the caller,
// outside the lock. first reports whether this is the generation's first
// control channel (which starts the pool machinery).
func (s *WsMuxTransport) adoptControl(g *wsGeneration, conn *network.WebSocketConn) (stale *network.WebSocketConn, first, ok bool) {
	// The end of a grace period is recorded (after controlMu is released) when
	// this adoption is what ends it: a loss was recorded and no channel is
	// registered. A first channel, or one replacing a still-registered channel
	// (control_replaced, recorded by the caller), is not a grace ending.
	var note string
	defer func() {
		if note != "" {
			s.recordEvent("control_reattached", note)
		}
	}()
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	if g != nil && (s.restartClaim == g || g.isStopped()) {
		return nil, false, false
	}
	if s.handlersStarted && s.controlChannel == nil {
		note = "control channel reattached; pool preserved"
		if !s.graceStart.IsZero() {
			note = fmt.Sprintf("control channel reattached after %s; pool preserved", time.Since(s.graceStart).Round(time.Second))
		}
	}
	// A control channel arriving while one is still registered is not a second
	// client - it is the same client reattaching after a drop this side has not
	// noticed yet (see the tunnel handler).
	stale = s.controlChannel
	first = !s.handlersStarted
	s.handlersStarted = true
	s.controlChannel = conn
	s.invalidateGraceLocked()
	s.config.TunnelStatus = fmt.Sprintf("Connected (%s)", s.config.Mode)
	return stale, first, true
}

// armControlGrace (re)starts the grace timer that decides what to do when the
// control channel has not reattached. Caller holds controlMu. It is self-
// re-arming: the control channel carries no user data, only heartbeats and
// new-connection requests, so as long as the pool still has live sessions
// carrying flows there is nothing to gain from a restart - it would just drop
// every in-flight flow because a CDN was slow to reconnect a side channel.
// Under prolonged CDN flakiness (502/521 storms) that turned every reconnect
// delay into a full restart, which then cascaded to the client via SG_Closed.
// So restart only once the pool has actually drained (nothing left to
// preserve); until then keep holding and re-checking.
//
// The callback carries the generation and epoch it was armed for; re-arming the
// same loss reuses the epoch (and graceStart), so total held time still counts
// from the original loss.
func (s *WsMuxTransport) armControlGrace(g *wsGeneration, epoch uint64) {
	s.graceTimer = time.AfterFunc(controlGraceWindow, func() { s.onControlGraceExpired(g, epoch) })
}

// graceShouldHold decides whether to keep holding the pool (re-arm the grace
// window) instead of restarting: hold only while at least one pool session is
// genuinely still open and the total hold has not exceeded the cap. Split out as
// a pure function so the decision is unit-testable without driving a restart.
func graceShouldHold(liveSessions int, held time.Duration) bool {
	return liveSessions > 0 && held < maxControlGraceHold
}

// onControlGraceExpired runs when a grace window ends for the loss identified by
// (g, epoch). It decides in three steps so nothing is held across I/O: snapshot
// under controlMu, gather the live-session count without it, then revalidate
// under controlMu and either re-arm the same epoch or claim the restart. The
// claim is what adoptControl races against, so a control channel that reattached
// while the count was being gathered wins (the epoch is gone and this returns),
// and one that arrives after the claim is refused instead of being torn down
// together with a transport it just recovered. Restart itself runs after the
// lock is released.
func (s *WsMuxTransport) onControlGraceExpired(g *wsGeneration, epoch uint64) {
	s.controlMu.Lock()
	if !s.graceCurrentLocked(g, epoch) {
		// Reattached, restarted, or superseded by a newer loss: stale timer.
		s.controlMu.Unlock()
		return
	}
	held := time.Since(s.graceStart)
	s.controlMu.Unlock()

	// Count sessions that are genuinely still open, not the raw sessionCounter:
	// that counter is only decremented when each handleSession loop notices its
	// own session die, which lags a mass client disconnect and - with smux
	// keepalive disabled - may never happen at all. Holding on a stale positive
	// count is exactly the "tunnel shows Connected but nothing flows until a
	// manual restart" failure.
	live := s.liveSessionCount()
	if s.graceRevalidateHook != nil {
		s.graceRevalidateHook()
	}

	s.controlMu.Lock()
	if !s.graceCurrentLocked(g, epoch) {
		s.controlMu.Unlock()
		return
	}
	// Hold only while the pool is genuinely alive AND we are within the cap. The
	// cap guarantees recovery: a client that dropped silently (sessions never
	// report closed) is torn down and cleanly rebuilt instead of held forever.
	if graceShouldHold(live, held) {
		s.armControlGrace(g, epoch)
		s.controlMu.Unlock()
		s.logger.Warnf("control channel still not reattached, but %d live pool session(s) (held %s/%s); holding instead of restarting", live, held.Round(time.Second), maxControlGraceHold)
		s.recordEvent("control_hold", fmt.Sprintf("no control channel after %s but %d live session(s), held %s; holding (flows preserved)", controlGraceWindow, live, held.Round(time.Second)))
		return
	}

	// Claim the restart: one per epoch (the epoch ends here, so a repeated
	// callback is inert), and adoptControl refuses this generation from now on.
	s.restartClaim = g
	s.invalidateGraceLocked()
	s.controlMu.Unlock()

	reason := "pool is empty"
	if live > 0 {
		reason = fmt.Sprintf("holding %d session(s) exceeded the %s cap", live, maxControlGraceHold)
	}
	s.logger.Warnf("control channel did not reattach (%s), restarting server", reason)
	s.recordEvent("restart", fmt.Sprintf("control channel did not reattach within %s (%s); full restart", held.Round(time.Second), reason))
	s.Restart()
}
