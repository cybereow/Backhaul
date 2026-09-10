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
func (s *WsMuxTransport) onControlLost(conn *network.WebSocketConn) {
	s.controlMu.Lock()
	if s.controlChannel != conn {
		// Already cleared, or the client has since reattached: this is a late
		// error from a connection nothing uses any more.
		s.controlMu.Unlock()
		conn.Close()
		return
	}
	s.controlChannel = nil

	if s.graceTimer != nil {
		s.graceTimer.Stop()
	}
	s.graceStart = time.Now()
	s.armControlGrace()
	s.controlMu.Unlock()

	conn.Close()
	s.controlMu.Lock()
	s.config.TunnelStatus = fmt.Sprintf("Reconnecting (%s)", s.config.Mode)
	s.controlMu.Unlock()
	s.recordEvent("control_lost", fmt.Sprintf("control channel dropped; holding pool up to %s for reattach (flows keep running)", controlGraceWindow))
	s.logger.Warnf("control channel lost, holding the pool for up to %s for the client to reattach", controlGraceWindow)
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
func (s *WsMuxTransport) armControlGrace() {
	s.graceTimer = time.AfterFunc(controlGraceWindow, s.onControlGraceExpired)
}

// graceShouldHold decides whether to keep holding the pool (re-arm the grace
// window) instead of restarting: hold only while at least one pool session is
// genuinely still open and the total hold has not exceeded the cap. Split out as
// a pure function so the decision is unit-testable without driving a restart.
func graceShouldHold(liveSessions int, held time.Duration) bool {
	return liveSessions > 0 && held < maxControlGraceHold
}

func (s *WsMuxTransport) onControlGraceExpired() {
	s.controlMu.Lock()
	if s.controlChannel != nil {
		// Reattached in the meantime; nothing to do.
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

	// Hold only while the pool is genuinely alive AND we are within the cap. The
	// cap guarantees recovery: a client that dropped silently (sessions never
	// report closed) is torn down and cleanly rebuilt instead of held forever.
	if graceShouldHold(live, held) {
		s.controlMu.Lock()
		if s.controlChannel == nil { // re-check under lock before re-arming
			s.armControlGrace()
		}
		s.controlMu.Unlock()
		s.logger.Warnf("control channel still not reattached, but %d live pool session(s) (held %s/%s); holding instead of restarting", live, held.Round(time.Second), maxControlGraceHold)
		s.recordEvent("control_hold", fmt.Sprintf("no control channel after %s but %d live session(s), held %s; holding (flows preserved)", controlGraceWindow, live, held.Round(time.Second)))
		return
	}

	reason := "pool is empty"
	if live > 0 {
		reason = fmt.Sprintf("holding %d session(s) exceeded the %s cap", live, maxControlGraceHold)
	}
	s.logger.Warnf("control channel did not reattach (%s), restarting server", reason)
	s.recordEvent("restart", fmt.Sprintf("control channel did not reattach within %s (%s); full restart", held.Round(time.Second), reason))
	s.Restart()
}
