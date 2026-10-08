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
// Four signs are read, any of which marks the connection as stalled:
//
//   - the kernel's retransmission backoff on its socket (stallBackoff timeouts in
//     a row without an acknowledgement): the leg from here to the CDN is silent;
//   - an RTT probe still unanswered while not one frame of any kind has arrived
//     on the connection for probeSilent: the far leg, or the peer, is. (The CDN
//     acknowledges what is sent to it whatever becomes of it beyond, so the
//     socket shows nothing then.)
//   - an RTT probe unanswered for several times what the connection's echoes
//     have been taking (echoOverdue), while frames do still arrive: the way to
//     the peer has stopped and the way back has not, or is still emptying what
//     the CDN held. An echo that late on a connection whose echoes were prompt
//     is not one held up behind the connection's own traffic - that shows in
//     the echoes before it;
//   - an RTT probe that failed some other way.
//
// A stalled connection is charged stallPenalty at placement, so nothing new is
// put on it while any other will do. And the flows on it that keep replay state
// are taken off it the way they would be had it been cut: suspended here, and
// resumed by driveResume on a connection that is getting through, replaying what
// was in flight. How soon depends on the sign. The first three leave no doubt -
// each is already a second or so of something that should have happened and did
// not - and every further moment is one a user waits, so the flows go at once
// (stallMoveAfterSure). The last says little by itself, and moving flows off a
// connection that works costs their replay: it has to last stallMoveAfter.
//
// ponytail: flows without replay state (a promoted flow's striped group, a flow
// opened while the budget was spent) stay and wait the stall out; moving them
// needs the stalled connection itself to deliver their last bytes.
const (
	stallBackoff       = 2
	stallPenalty       = 1000.0
	stallMoveAfter     = 3 * time.Second
	stallMoveAfterSure = 0
	probeSilent        = time.Second
	stallEvery         = 250 * time.Millisecond
	// echoLateFactor: an echo this many times later than the connection's echoes
	// have been (its RTT estimate) is not coming.
	echoLateFactor = 4
	// overdueQuiet: how long after a connection's echo was taken for overdue the
	// next late echo on it is not (see echoIsOverdue).
	overdueQuiet = time.Minute
)

// echoSilent reports the second sign: a probe sent at sent (unix nanos, 0 = none
// is out) is still unanswered, and for probeSilent since it left nothing at all
// has arrived - counted from the last frame (lastRecv) when one came in after
// the probe had gone: the connection can stop between the two, and a frame that
// was already on its way says nothing about what follows it.
func echoSilent(sent, lastRecv int64, now time.Time) bool {
	return sent != 0 && now.UnixNano()-max(sent, lastRecv) >= int64(probeSilent)
}

// echoOverdue reports the third sign: a probe sent at sent is still unanswered
// after echoLateFactor times rtt, the connection's RTT estimate in nanoseconds,
// and never sooner than probeSilent. It is no sign at all on a connection whose
// echoes have been slow (or never timed): there a late echo is what a full
// connection looks like, and taking its flows off it only moves the queue. A
// probe that times out counts into the estimate (see probeSessionRTT), so a
// connection that has become slow stops qualifying after a probe or two.
func echoOverdue(sent, rtt int64, now time.Time) bool {
	if sent == 0 || rtt == 0 || echoLateFactor*rtt > int64(rttProbeTimeout) {
		return false
	}
	return now.UnixNano()-sent >= max(echoLateFactor*rtt, int64(probeSilent))
}

// echoIsOverdue applies the third sign to the session, with a memory. A late
// echo is also what a connection that has just filled up shows, before its RTT
// estimate has caught up - and taking its flows off it fills the next one, whose
// echo is then late in turn. So a connection is believed once: the sign stands
// for as long as that same probe stays unanswered, and after it a late echo on
// this connection says nothing for overdueQuiet.
func (ps *pooledSession) echoIsOverdue(sent int64, now time.Time) bool {
	if sent == ps.overdueSent.Load() {
		return true
	}
	if !echoOverdue(sent, ps.rtt.Load(), now) {
		return false
	}
	if at := ps.overdueAt.Load(); at != 0 && now.UnixNano()-at < int64(overdueQuiet) {
		return false
	}
	ps.overdueSent.Store(sent)
	ps.overdueAt.Store(now.UnixNano())
	return true
}

// noteHealth records what the latest look at the session showed: socketStalled
// from its TCP state, together with its RTT probes.
func (ps *pooledSession) noteHealth(now time.Time, socketStalled bool) {
	sure := socketStalled
	if sent := ps.probeSent.Load(); !sure && sent != 0 {
		sure = echoSilent(sent, ps.session.LastRecv(), now) || ps.echoIsOverdue(sent, now)
	}
	if sure {
		ps.sureSince.CompareAndSwap(0, now.UnixNano())
	} else {
		ps.sureSince.Store(0)
	}
	if !sure && ps.probeFails.Load() == 0 {
		ps.stalledSince.Store(0)
		return
	}
	ps.stalledSince.CompareAndSwap(0, now.UnixNano())
}

// dueToMove reports whether the session has been stalled long enough, for the
// sign it shows, for its flows to be taken off it.
func (ps *pooledSession) dueToMove(now time.Time) bool {
	if sure := ps.sureSince.Load(); sure != 0 && now.Sub(time.Unix(0, sure)) >= stallMoveAfterSure {
		return true
	}
	since := ps.stalledSince.Load()
	return since != 0 && now.Sub(time.Unix(0, since)) >= stallMoveAfter
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
// that has been stalled for long enough (dueToMove).
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
		if !ps.dueToMove(now) || ps.session.IsClosed() {
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
			// A flow nothing has come back for may be one the client has not heard
			// of yet, its opening held up on this very connection. A client that
			// can say so has it opened again elsewhere (reopenFlow). An older one
			// only refuses the resume, which ends the flow: there it is given
			// stallMoveAfter for the connection to get through after all.
			if !f.sw.HeardFromPeer() && !s.reopenSeen.Load() && ps.stalledFor(now) < stallMoveAfter {
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
