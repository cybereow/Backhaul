package transport

import (
	"time"

	"github.com/musix/backhaul/internal/smux"
	"github.com/musix/backhaul/internal/utils"
)

// probeSessionRTT keeps a session's RTT estimate current by opening a tiny ping
// stream every rttProbeEvery and timing the peer's echo, feeding the result into
// an EWMA. It runs only on mux_version >= 2 (flow kinds, so the peer can route
// the probe to its echoer) and only while striping is on, so a non-striped
// deployment pays nothing. It exits when the session closes or the transport
// shuts down.
func (s *WsMuxTransport) probeSessionRTT(g *wsGeneration, ps *pooledSession) {
	ticker := time.NewTicker(rttProbeEvery)
	defer ticker.Stop()
	// The first probe goes out at once: until one has been answered nothing says
	// that a new connection gets through at all, and a new one is the cheapest
	// to place flows on.
	first := make(chan struct{}, 1)
	first <- struct{}{}
	for {
		select {
		case <-g.ctx.Done():
			return
		case <-ps.session.CloseChan():
			return
		case <-first:
		case <-ticker.C:
		}
		rtt, err := measureRTT(ps.session)
		if err != nil {
			// A transient probe failure (a busy session refusing a stream)
			// shouldn't discard a good estimate; just try again next tick. But
			// an unanswered probe is also how a connection that has stopped
			// getting through shows from end to end (see wsmux_stall.go).
			ps.probeFails.Add(1)
			s.logger.Tracef("rtt probe on %s failed: %v", ps.cdn, err)
			continue
		}
		ps.probeFails.Store(0)
		// EWMA (7/8 old, 1/8 new) so a single jittery sample doesn't swing
		// selection; the first sample seeds it directly.
		if old := ps.rtt.Load(); old > 0 {
			ps.rtt.Store((old*7 + rtt) / 8)
		} else {
			ps.rtt.Store(rtt)
		}
	}
}

// probeReplayPromote asks the client, once per session, whether a promotable flow
// may keep replay state (utils.AttachProbe), and records a yes on the session. An
// older client refuses the mode; a probe that could not be made at all is tried
// again a few times, and until one is answered the session's promotable flows
// simply open without replay.
func (s *WsMuxTransport) probeReplayPromote(g *wsGeneration, ps *pooledSession) {
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-g.ctx.Done():
				return
			case <-ps.session.CloseChan():
				return
			case <-time.After(time.Second):
			}
		}
		stream, err := ps.session.OpenStream()
		if err != nil {
			continue
		}
		_ = stream.SetDeadline(time.Now().Add(rttProbeTimeout))
		accept := false
		if err = utils.SendFlowAttach(stream, 0, utils.AttachProbe, 0); err == nil {
			accept, _, err = utils.ReadAttachVerdict(stream)
		}
		stream.Close()
		if err != nil {
			continue
		}
		ps.replayPromote.Store(accept)
		return
	}
}

// measureRTT opens one ping stream, sends a nonce and times the echo. The stream
// carries no user data and is closed immediately after.
func measureRTT(session *smux.Session) (int64, error) {
	stream, err := session.OpenStream()
	if err != nil {
		return 0, err
	}
	defer stream.Close()
	if err := stream.SetDeadline(time.Now().Add(rttProbeTimeout)); err != nil {
		return 0, err
	}
	start := time.Now()
	if err := utils.SendFlowPing(stream, uint64(start.UnixNano())); err != nil {
		return 0, err
	}
	if _, err := utils.ReceiveFlowPing(stream); err != nil {
		return 0, err
	}
	return int64(time.Since(start)), nil
}
