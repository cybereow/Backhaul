package transport

import (
	"fmt"
	"sync/atomic"
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

func (s *WsMuxTransport) onControlGraceExpired() {
	s.controlMu.Lock()
	if s.controlChannel != nil {
		// Reattached in the meantime; nothing to do.
		s.controlMu.Unlock()
		return
	}
	if live := atomic.LoadInt32(&s.sessionCounter); live > 0 {
		// Pool still carrying flows: hold, don't tear everything down.
		s.armControlGrace()
		s.controlMu.Unlock()
		s.logger.Warnf("control channel still not reattached, but %d pool session(s) alive; holding instead of restarting", live)
		s.recordEvent("control_hold", fmt.Sprintf("no control channel after %s but %d pool session(s) alive; holding (flows preserved)", controlGraceWindow, live))
		return
	}
	s.controlMu.Unlock()
	s.logger.Warn("control channel did not reattach and the pool is empty, restarting server")
	s.recordEvent("restart", fmt.Sprintf("control channel did not reattach within %s and pool is empty; full restart", controlGraceWindow))
	s.Restart()
}
