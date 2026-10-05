package transport

import (
	"fmt"
	"time"
)

// A pool connection that has stopped getting through.
//
// The path between the two ends runs through CDNs, and a connection on it can
// stop passing data for most of a minute without being closed: nothing is
// acknowledged, nothing arrives, and then it carries on as if nothing had been.
// It happens to several connections of a pool at once while the others, and new
// ones, work. Everything multiplexed on such a connection waits with it - and so
// did every new flow placed on it, which is what turned a third of the pool
// stalling into nothing getting through at all: a stalled connection's streams
// end as their users give up, so by load it looked like the best one to pick.
//
// Two signs are read, either of which marks the connection as stalled:
//
//   - the kernel's retransmission backoff on its socket (stallBackoff timeouts in
//     a row without an acknowledgement): the leg from here to the CDN is silent;
//   - an RTT probe that got no answer: the far leg, or the peer, is.
//
// A stalled connection is charged stallPenalty at placement, so nothing new is
// put on it while any other will do. And once it has been stalled for
// stallMoveAfter, the flows on it that keep replay state are taken off it the
// way they would be had it been cut: suspended here, and resumed by driveResume
// on a connection that is getting through, replaying what was in flight.
//
// ponytail: flows without replay state (a promoted flow's striped group, a flow
// opened while the budget was spent) stay and wait the stall out; moving them
// needs the stalled connection itself to deliver their last bytes.
const (
	stallBackoff   = 2
	stallPenalty   = 1000.0
	stallMoveAfter = 3 * time.Second
)

// noteHealth records what the latest look at the session showed: socketStalled
// from its TCP state, together with its RTT probes.
func (ps *pooledSession) noteHealth(now time.Time, socketStalled bool) {
	if !socketStalled && ps.probeFails.Load() == 0 {
		ps.stalledSince.Store(0)
		return
	}
	ps.stalledSince.CompareAndSwap(0, now.UnixNano())
}

// withoutStalled drops the stalled sessions from avail if n that are not remain.
// The charge at placement is not enough where sessions are picked one per CDN
// first (selectLegs): the only session of a CDN is taken however it scores.
func withoutStalled(avail []*pooledSession, n int) []*pooledSession {
	ok := make([]*pooledSession, 0, len(avail))
	for _, ps := range avail {
		if ps.stalledSince.Load() == 0 {
			ok = append(ok, ps)
		}
	}
	if len(ok) < n {
		return avail
	}
	return ok
}

// stalledFor is how long the session has been stalled (0 = it is not).
func (ps *pooledSession) stalledFor(now time.Time) time.Duration {
	since := ps.stalledSince.Load()
	if since == 0 {
		return 0
	}
	return now.Sub(time.Unix(0, since))
}

// moveOffStalled takes the flows that can be resumed elsewhere off every session
// that has been stalled for stallMoveAfter.
func (s *WsMuxTransport) moveOffStalled(g *wsGeneration, sessions []*pooledSession, now time.Time) {
	// A flow is only taken off a stalled connection when there is somewhere for
	// it to go: suspended with every connection stalled, it would be resumed onto
	// one of those, fail, and be given up at the end of its resume window - a
	// reset for a flow that would have carried on had the stall simply passed.
	healthy := 0
	for _, ps := range sessions {
		if ps.stalledSince.Load() == 0 && !ps.session.IsClosed() {
			healthy++
		}
	}
	for _, ps := range sessions {
		if ps.stalledFor(now) < stallMoveAfter || ps.session.IsClosed() {
			continue
		}
		if healthy == 0 {
			s.requestGrowth() // a new connection may get through where these do not
			return
		}
		moved := 0
		for _, f := range s.flowsOn(ps.session) {
			if f.isStriped() || !f.sw.Replaying() || f.session() != ps.session {
				continue
			}
			select {
			case <-f.sw.SuspendedCh():
				continue // suspended on an earlier pass, its resume still under way
			default:
			}
			if f.sw.Suspend() {
				moved++
			}
		}
		if moved > 0 {
			s.logger.Warnf("a pool connection (%s) has not been getting through for %s: resuming %d flow(s) on others", ps.host, ps.stalledFor(now).Round(time.Second), moved)
			s.recordEvent("session_stalled", fmt.Sprintf("%s silent for %s; %d flow(s) resumed on other connections", ps.host, ps.stalledFor(now).Round(time.Second), moved))
		}
	}
}
