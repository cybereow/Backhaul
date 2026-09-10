package transport

import (
	"fmt"
	"net"
	"sort"
	"sync/atomic"
	"time"

	"github.com/xtaci/smux"
)

// pooledSession is one live pool connection plus the metadata leg selection
// scores it on: the CDN it arrived over (so a flow can be spread across distinct
// CDNs) and an EWMA round-trip estimate maintained by probeSessionRTT.
type pooledSession struct {
	session *smux.Session
	cdn     string       // CDN identity: the remote IP the pool connection arrived from
	rtt     atomic.Int64 // EWMA round-trip in nanoseconds; 0 until the first probe lands
}

// Leg-selection scoring. A leg's score is (open streams + 1) x its RTT in ms;
// the lowest score wins. This is capacity-weighted, not count-balanced: on a
// TCP-over-TCP-over-TLS leg a single stream's throughput ceiling is ~window/RTT,
// so RTT is a live proxy for how much a leg can carry. Weighting the placement
// cost by RTT makes a fast (low-RTT) CDN absorb proportionally more streams
// (~1/RTT) before its cost catches up to a slower CDN, while a slow CDN still
// takes a few - a weighted spread biased toward the good paths.
//
// This sits between two failure modes seen earlier. Even-count balancing (load
// the strongly dominant term, RTT a tiebreak) spread Ookla's parallel streams
// equally over every CDN, pouring half of them onto slow/high-RTT paths that
// each carried little, so the aggregate fell below the single best CDN. Pure
// additive RTT dominance did the opposite - it piled every stream onto one
// low-RTT session because load barely counted. The multiplicative form keeps
// load always significant (each stream multiplies the leg's cost) so it can
// never concentrate on one leg, yet still steers the bulk of the traffic toward
// the fastest CDNs instead of diluting it into the slow ones.
//
// unprobedRTTms is the neutral RTT charged to a session not yet probed (or on
// mux_version 1, where probing isn't possible), so a fresh session is neither
// unfairly preferred nor shunned before its first probe.
const (
	unprobedRTTms   = 40.0
	rttProbeEvery   = 5 * time.Second
	rttProbeTimeout = 10 * time.Second
)

// registerSession adds a pool session to the live registry and returns its
// wrapper so the caller can start probing it. unregisterSession removes it.
func (s *WsMuxTransport) registerSession(session *smux.Session) *pooledSession {
	ps := &pooledSession{session: session, cdn: cdnKey(session.RemoteAddr())}
	s.sessionsMu.Lock()
	s.sessions = append(s.sessions, ps)
	s.sessionsMu.Unlock()
	return ps
}

func (s *WsMuxTransport) unregisterSession(session *smux.Session) {
	s.sessionsMu.Lock()
	for i, ps := range s.sessions {
		if ps.session == session {
			s.sessions = append(s.sessions[:i], s.sessions[i+1:]...)
			break
		}
	}
	s.sessionsMu.Unlock()
}

// liveSessionCount reports how many registered pool sessions are still open.
// It reads the smux sessions' real state rather than the sessionCounter, which
// only drops when each handleSession loop individually notices its session die
// - a lag that a mass disconnect (or a config with mux keepalive disabled) can
// stretch indefinitely. The control-grace decision uses this so a dead pool is
// recognised as dead.
func (s *WsMuxTransport) liveSessionCount() int {
	s.sessionsMu.Lock()
	defer s.sessionsMu.Unlock()
	live := 0
	for _, ps := range s.sessions {
		if ps.session != nil && !ps.session.IsClosed() {
			live++
		}
	}
	return live
}

// cdnKey reduces a pool connection's remote address to a CDN identity - the host
// (IP) without the ephemeral port - so two connections that arrived over the
// same CDN edge count as the same path for spreading purposes.
func cdnKey(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}
	return host
}

// legScore ranks a session for leg selection: lower is better. It combines live
// load (open streams on the session) with the measured RTT so selection favours
// the least-loaded, lowest-latency connection.
func legScore(ps *pooledSession) float64 {
	return legScoreValue(ps.session.NumStreams(), ps.rtt.Load())
}

// legScoreValue is the pure scoring math, split out from legScore so it can be
// tested without a live smux session. The cost of placing a stream on a leg is
// (load + 1) x RTT: the +1 gives an idle leg a nonzero cost so idle legs are
// still ranked by RTT (rather than all tying at zero and concentrating on the
// first one), and multiplying by RTT makes each additional stream cost more on a
// slower leg, so streams accrue on a leg in proportion to 1/RTT - the
// capacity-weighted spread described above. An unprobed session (rttNanos <= 0)
// is charged the neutral unprobedRTTms rather than 0, so a fresh session isn't
// falsely ranked as the fastest path.
func legScoreValue(load int, rttNanos int64) float64 {
	rttMs := unprobedRTTms
	if rttNanos > 0 {
		rttMs = float64(rttNanos) / float64(time.Millisecond)
	}
	return float64(load+1) * rttMs
}

// selectLegs picks the n best sessions for a flow: lowest score first, but
// spread across distinct CDNs before doubling up on any one. Pass 1 takes the
// best session from each distinct CDN (so a striped flow rides several CDNs, and
// a single-leg UDP flow lands on the best CDN); pass 2 fills any remaining slots
// by best score regardless of CDN, for when a flow wants more legs than there
// are CDNs. avail is sorted in place.
func selectLegs(avail []*pooledSession, n int, score func(*pooledSession) float64) []*pooledSession {
	sort.SliceStable(avail, func(i, j int) bool {
		return score(avail[i]) < score(avail[j])
	})

	chosen := make([]*pooledSession, 0, n)
	usedCDN := make(map[string]bool)
	for _, ps := range avail {
		if len(chosen) == n {
			break
		}
		if usedCDN[ps.cdn] {
			continue
		}
		usedCDN[ps.cdn] = true
		chosen = append(chosen, ps)
	}
	if len(chosen) < n {
		inChosen := make(map[*pooledSession]bool, len(chosen))
		for _, ps := range chosen {
			inChosen[ps] = true
		}
		for _, ps := range avail {
			if len(chosen) == n {
				break
			}
			if !inChosen[ps] {
				chosen = append(chosen, ps)
			}
		}
	}
	return chosen
}

// openStripedLegs opens one stream on each of n live sessions, chosen by
// selectLegs: load-balanced across the pool (so a many-flow workload spreads
// over every CDN and aggregates to the pool's full width) with RTT breaking
// ties toward the lowest-latency CDN. Used by both the striped TCP dispatcher
// (n = legsPerFlow) and the plain/UDP path (n = 1).
//
// It requires n distinct sessions and errors if fewer are live, rather than
// silently opening a narrower group: a reduced-width stripe means the two ends
// disagree on how many data shards a FEC flow has (the server sizes the encoder
// from the configured StripeFactor, the client from the leg count it received),
// so a partial group either fails to decode or truncates one direction. Callers
// treat the error as "pool not wide enough yet" - the striped dispatcher
// requeues and the promotion path stays plain - so the flow waits for the pool
// to grow instead of running mis-striped.
func (s *WsMuxTransport) openStripedLegs(n int) ([]*smux.Stream, error) {
	s.sessionsMu.Lock()
	avail := make([]*pooledSession, len(s.sessions))
	copy(avail, s.sessions)
	s.sessionsMu.Unlock()

	if len(avail) < n {
		return nil, fmt.Errorf("striping needs %d live pool session(s), only %d available", n, len(avail))
	}

	chosen := selectLegs(avail, n, legScore)
	streams := make([]*smux.Stream, 0, n)
	for _, ps := range chosen {
		stream, err := ps.session.OpenStream()
		if err != nil {
			for _, st := range streams {
				st.Close()
			}
			return nil, fmt.Errorf("failed to open stripe leg: %w", err)
		}
		streams = append(streams, stream)
	}
	return streams, nil
}

// openPlainLeg opens one stream for a single-leg (plain, non-striped) flow,
// picking the session round-robin across the whole pool rather than always the
// single lowest-score one. selectLegs(_, 1, legScore) returns the one
// best-scoring session, which concentrates a burst of concurrent plain flows
// onto the same connection: when the pool is unprobed (RTT probing only runs
// for the striped/UDP paths) every session ties on score, and many flows opened
// at once all read the same near-zero load and pile onto the first session
// before NumStreams catches up. Every stream then shares one TCP connection to
// one CDN edge, and that single connection's origin->edge (upload) ceiling -
// well below its edge->origin (download) ceiling behind the CDN - caps the
// aggregate. The old per-session work-stealing loop never had this: each of N
// pool sessions pulled independently from the shared local channel, so N flows
// spread across N connections and their throughput summed past any single
// connection's asymmetry. Round-robin restores that spread with an atomic cursor
// so concurrent callers each get a distinct starting session; a closed session
// or a failed OpenStream just advances to the next.
func (s *WsMuxTransport) openPlainLeg() (*smux.Stream, error) {
	s.sessionsMu.Lock()
	avail := make([]*pooledSession, len(s.sessions))
	copy(avail, s.sessions)
	s.sessionsMu.Unlock()

	if len(avail) == 0 {
		return nil, fmt.Errorf("no live pool session available for a plain leg")
	}

	start := int(atomic.AddUint32(&s.plainRotation, 1))
	for i := 0; i < len(avail); i++ {
		ps := avail[(start+i)%len(avail)]
		if ps.session == nil || ps.session.IsClosed() {
			continue
		}
		stream, err := ps.session.OpenStream()
		if err != nil {
			s.logger.Tracef("plain leg: OpenStream on a pool session failed, trying the next: %v", err)
			continue
		}
		return stream, nil
	}
	return nil, fmt.Errorf("all %d pool session(s) failed to open a plain leg", len(avail))
}

// bestPerCDN returns the best-scoring session for each distinct CDN, so an
// aggregate run uses one connection per path rather than several on the same one.
func bestPerCDN(avail []*pooledSession, score func(*pooledSession) float64) []*pooledSession {
	ordered := make([]*pooledSession, len(avail))
	copy(ordered, avail)
	sort.SliceStable(ordered, func(i, j int) bool {
		return score(ordered[i]) < score(ordered[j])
	})
	seen := make(map[string]bool)
	out := make([]*pooledSession, 0, len(ordered))
	for _, ps := range ordered {
		if seen[ps.cdn] {
			continue
		}
		seen[ps.cdn] = true
		out = append(out, ps)
	}
	return out
}
