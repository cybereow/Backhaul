package transport

import (
	"context"
	"time"

	"github.com/musix/backhaul/internal/smux"
	"github.com/musix/backhaul/internal/utils"
)

// Rotation is derived from one number, the max connection age of the CDN/LB in
// front of the server (cdn_max_age), so nobody has to work jitter and dial slack
// out by hand.
//
//   - A pool connection is retired at rotateFraction of that age, +/- rotateJitter.
//     The worst case, rotateFraction*(1+rotateJitter) of the age, still leaves
//     the replacement ~20% of the age to be dialled before the CDN would cut it.
//   - A retired connection is drained for the rest of the age, measured from its
//     retirement: past that the CDN closes it anyway, so waiting longer buys
//     nothing and only keeps surplus connections open.
const (
	rotateFraction = 0.7
	rotateJitter   = 0.1
)

// RotationPlan turns the CDN's max connection age into the age a pool connection
// is retired at and the longest it is then drained for. Zero disables rotation.
func RotationPlan(cdnMaxAge time.Duration) (maxConnAge, maxDrain time.Duration) {
	if cdnMaxAge <= 0 {
		return 0, 0
	}
	maxConnAge = time.Duration(float64(cdnMaxAge) * rotateFraction)
	return maxConnAge, cdnMaxAge - maxConnAge
}

// rotateAge is one connection's jittered retirement age; the connections of the
// initial pool are dialled together, so a fixed age would rotate them in lockstep.
func rotateAge(base time.Duration) time.Duration {
	return utils.JitterFraction(base, rotateJitter)
}

// rotateRetryInterval is how long rotation waits before re-checking for the
// replacement connection it asked for. Deliberately unhurried: the connection
// is only aging, and the client may be unable to dial at all for minutes.
const rotateRetryInterval = 30 * time.Second

// retireSettle is the pause before a drained session is actually closed, so a
// stream opened by a flow that picked the session just before it was retired is
// seen and drained instead of cut.
const retireSettle = 200 * time.Millisecond

// requestReplacement asks the client to bring up one more pool connection.
func (s *WsMuxTransport) requestReplacement() {
	select {
	case s.reqNewConnChan <- struct{}{}:
	default:
		s.logger.Warn("failed to request a replacement connection for rotation. channel is full")
	}
}

// retireSession takes a pool session out of service before it is old enough
// for a CDN/LB max-age reset to kill it mid-flow, waiting for the streams
// already running on it to finish. Callers only get here once a replacement
// connection has actually joined the pool (see awaitReplacement), so this
// never shrinks the pool. The caller closes the session once this returns.
//
// Draining is the point: a long-lived flow - an SSH session, a large download -
// is pinned to one connection because smux cannot migrate a live stream, so
// cutting the connection at rotation time would cut the flow. Instead the
// retiring connection takes no new streams and stays up until its last one
// ends. That buys such a flow the whole window up to the CDN's hard limit; the
// limit itself is not something client-side code can extend.
//
// The drain cap bounds that wait: once it elapses the session is given up on and the
// caller closes it, cutting whatever streams are still on it. It only ever runs
// after the replacement has joined the pool, so it never shrinks the pool - a
// failed replacement dial keeps the aging session in service (awaitReplacement)
// and never reaches here.
func (s *WsMuxTransport) retireSession(session *smux.Session) {
	s.retireSessionWithin(session, s.config.MaxDrain)
}

// retireSessionWithin is retireSession with the drain budget given explicitly
// (0 = unbounded): rotation spends part of the configured cap moving flows off the
// session first and drains only for what is left, so the whole retirement still
// ends inside max_drain.
func (s *WsMuxTransport) retireSessionWithin(session *smux.Session, budget time.Duration) {
	s.logger.Debugf("retiring pool session at its rotation age, %d live stream(s) to drain", session.NumStreams())

	// A nil channel (no drain cap) blocks forever in the select: drain unbounded.
	var deadline <-chan time.Time
	if budget > 0 {
		t := time.NewTimer(budget)
		defer t.Stop()
		deadline = t.C
	}

	// ponytail: poll for drain rather than wiring per-stream completion
	// signalling; a 1s tick is plenty for a connection on its way out.
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		if session.IsClosed() {
			return
		}
		if session.NumStreams() == 0 {
			// A flow placed on this session just before it left the registry may
			// not have registered its stream yet (selection and OpenStream are not
			// atomic); give it a moment to show up rather than closing under it.
			select {
			case <-s.ctx.Done():
				return
			case <-time.After(retireSettle):
			}
			if session.NumStreams() == 0 {
				s.logger.Debug("retired pool session drained, closing")
				return
			}
		}
		select {
		case <-s.ctx.Done():
			return
		case <-deadline:
			s.logger.Debugf("retired pool session still has %d stream(s) after the drain cap (%s), closing", session.NumStreams(), budget)
			return
		case <-ticker.C:
		}
	}
}

// rotateStripedSession runs the rotation of one pool session, striped or not
// (the name is historical: every pool session is rotated through here, from
// handleLoop). At its rotation age the session waits for a replacement, is pulled
// out of the leg pool so nothing new lands on it, has its resumable flows moved to
// other sessions, and is then drained and closed. The sessionCounter decrement is
// left to the CloseChan watcher registered in handleLoop.
//
// The session stays owned by the generation (see wsGeneration) while it drains:
// unregistering it only stops new legs landing on it, so a restart during the
// drain still closes it.
func (s *WsMuxTransport) rotateStripedSession(g *wsGeneration, session *smux.Session) {
	rotateTimer := time.NewTimer(rotateAge(s.config.MaxConnAge))
	defer rotateTimer.Stop()

	select {
	case <-g.ctx.Done():
		return
	case <-session.CloseChan():
		return
	case <-rotateTimer.C:
	}

	// Make before break. Unlike the non-striped path this can block: the session
	// keeps serving legs from the registry until awaitReplacement takes it out,
	// so waiting here costs no capacity. On true the session is already
	// unregistered and the decision gate already released: draining below must
	// not hold up another rotation's decision.
	if !s.awaitReplacement(g, session) {
		return
	}

	// Move the resumable flows off first: they would otherwise be pinned to this
	// session until the CDN cuts it. Moving them and draining share the one
	// max_drain budget.
	start := time.Now()
	budget := s.config.MaxDrain
	mctx := g.ctx
	if budget > 0 {
		var cancel context.CancelFunc
		mctx, cancel = context.WithTimeout(g.ctx, budget)
		defer cancel()
	}
	s.migrateFlowsOff(mctx, session)
	if budget > 0 {
		if budget -= time.Since(start); budget < time.Millisecond {
			budget = time.Millisecond
		}
	}

	s.retireSessionWithin(session, budget)
	session.Close()
}

// rotatePollEvery is how often a rotation re-checks the registry for its
// replacement (WsMuxTransport.rotatePoll overrides it, tests shorten it).
const rotatePollEvery = time.Second

// awaitReplacement is one rotation's make-before-break decision. It takes the
// generation's decision gate, snapshots which sessions the pool has right now,
// asks for a replacement, and waits until a session outside that snapshot is
// registered and live. Only then, in one step under the registry lock, it takes
// the old session out of eligibility, and releases the gate (before the caller
// drains it). Returns true if the caller now owns the draining of session, false
// if the generation ended or the session died while waiting - in both cases there
// is nothing left to rotate. It never gives up otherwise: retiring a connection
// the pool has no replacement for would leave less capacity than before.
//
// Why the gate and a per-decision snapshot: a bare admission counter let two
// rotations share one increment, or count a replacement that had already died,
// so both retired against a single (or no) successor. Serial decisions, each
// snapshotting only after it holds the gate, make a successor usable once: the
// next rotation's snapshot already contains it. The old session stays eligible
// (and serving) for the whole wait, re-asking on every retry because the client's
// tunnelDialer abandons a failed dial for good.
//
// ponytail: decisions are serial - one rotation waits for its replacement while
// the others queue behind it. Rotation is minutes apart, so per-request
// replacement tickets are unnecessary until measured rotation throughput says so.
func (s *WsMuxTransport) awaitReplacement(g *wsGeneration, session *smux.Session) bool {
	ctx := s.ctx
	if g != nil {
		ctx = g.ctx
	}
	if !g.acquireRotate(ctx, session) {
		return false
	}
	defer g.releaseRotate()

	// Snapshot first, ask second: only a session admitted from here on counts.
	seen := s.snapshotSessions()
	s.requestReplacement()

	// ponytail: poll the registry instead of signalling admissions to whoever is
	// waiting. Rotation is not latency-sensitive - a second either way is noise
	// against a rotation age measured in minutes.
	pollEvery := s.rotatePoll
	if pollEvery <= 0 {
		pollEvery = rotatePollEvery
	}
	poll := time.NewTicker(pollEvery)
	defer poll.Stop()
	reask := time.NewTicker(utils.JitterDuration(rotateRetryInterval))
	defer reask.Stop()

	// One event per decision for a replacement that is late (the anomaly worth
	// seeing), one when the aging session is retired: two per rotation at most,
	// so rotation cannot flush the ring of control-channel history.
	deferred := false
	for {
		select {
		case <-ctx.Done():
			return false
		case <-session.CloseChan():
			return false
		case <-reask.C:
			s.logger.Debugf("rotation deferred: replacement pool connection is not up, keeping the aging one in service (re-asking every ~%s)", rotateRetryInterval)
			if !deferred {
				deferred = true
				s.recordEvent("rotation_deferred", "replacement pool connection not up; aging session kept in service, re-asking")
			}
			s.requestReplacement()
		case <-poll.C:
		}
		if s.retireIfReplaced(session, seen) {
			s.recordEvent("rotation_retired", "replacement admitted; aging session out of eligibility, draining")
			return true
		}
	}
}

// acquireRotate takes the generation's rotation decision gate, giving up when
// ctx ends or session dies first (a nil generation has no gate to take).
func (g *wsGeneration) acquireRotate(ctx context.Context, session *smux.Session) bool {
	if g == nil {
		return true
	}
	select {
	case g.rotate <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	case <-session.CloseChan():
		return false
	}
}

func (g *wsGeneration) releaseRotate() {
	if g != nil {
		<-g.rotate
	}
}

// snapshotSessions is the set of pool sessions registered right now.
func (s *WsMuxTransport) snapshotSessions() map[*smux.Session]struct{} {
	s.sessionsMu.Lock()
	defer s.sessionsMu.Unlock()
	seen := make(map[*smux.Session]struct{}, len(s.sessions))
	for _, ps := range s.sessions {
		seen[ps.session] = struct{}{}
	}
	return seen
}

// retireIfReplaced authorizes retiring old if the registry holds a live session
// that was not in seen, and in that same critical section takes old out of
// eligibility, so no other rotation can observe old as still eligible after it
// has been paid for. A successor that has died since it was admitted does not
// count.
func (s *WsMuxTransport) retireIfReplaced(old *smux.Session, seen map[*smux.Session]struct{}) bool {
	s.sessionsMu.Lock()
	defer s.sessionsMu.Unlock()
	for _, ps := range s.sessions {
		if ps.session == old || ps.session == nil || ps.session.IsClosed() {
			continue
		}
		if _, known := seen[ps.session]; known {
			continue
		}
		s.unregisterLocked(old)
		return true
	}
	return false
}
