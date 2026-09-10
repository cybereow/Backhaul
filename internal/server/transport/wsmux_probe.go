package transport

import (
	"time"

	"github.com/musix/backhaul/internal/utils"
	"github.com/xtaci/smux"
)

// probeSessionRTT keeps a session's RTT estimate current by opening a tiny ping
// stream every rttProbeEvery and timing the peer's echo, feeding the result into
// an EWMA. It runs only on mux_version >= 2 (flow kinds, so the peer can route
// the probe to its echoer) and only while striping is on, so a non-striped
// deployment pays nothing. It exits when the session closes or the transport
// shuts down.
func (s *WsMuxTransport) probeSessionRTT(ps *pooledSession) {
	ticker := time.NewTicker(rttProbeEvery)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ps.session.CloseChan():
			return
		case <-ticker.C:
			rtt, err := measureRTT(ps.session)
			if err != nil {
				// A transient probe failure (a busy session refusing a stream)
				// shouldn't discard a good estimate; just try again next tick.
				s.logger.Tracef("rtt probe on %s failed: %v", ps.cdn, err)
				continue
			}
			// EWMA (7/8 old, 1/8 new) so a single jittery sample doesn't swing
			// selection; the first sample seeds it directly.
			if old := ps.rtt.Load(); old > 0 {
				ps.rtt.Store((old*7 + rtt) / 8)
			} else {
				ps.rtt.Store(rtt)
			}
		}
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
