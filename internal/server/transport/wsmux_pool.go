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
	session   *smux.Session
	halfClose bool         // negotiated halfclose-v1: plain flows on it may use FlowPlainHC
	cdn       string       // CDN identity: the remote IP the pool connection arrived from
	rtt       atomic.Int64 // EWMA round-trip in nanoseconds; 0 until the first probe lands

	// pendingOpens counts plain-leg OpenStreams that have picked this session but
	// not yet returned (see openPlainLegPS). It is guarded by
	// WsMuxTransport.plainSelectMu, never read or written without it.
	pendingOpens int
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
func (s *WsMuxTransport) registerSession(session *smux.Session, halfClose bool) *pooledSession {
	ps := &pooledSession{session: session, halfClose: halfClose, cdn: cdnKey(session.RemoteAddr())}
	s.sessionsMu.Lock()
	s.sessions = append(s.sessions, ps)
	s.sessionsMu.Unlock()
	return ps
}

func (s *WsMuxTransport) unregisterSession(session *smux.Session) {
	s.sessionsMu.Lock()
	s.unregisterLocked(session)
	s.sessionsMu.Unlock()
}

// unregisterLocked is unregisterSession for a caller that holds sessionsMu, so
// rotation can validate its successor and remove the old session in one step.
func (s *WsMuxTransport) unregisterLocked(session *smux.Session) {
	for i, ps := range s.sessions {
		if ps.session == session {
			s.sessions = append(s.sessions[:i], s.sessions[i+1:]...)
			break
		}
	}
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
// placed by legScore so it lands on a fast, lightly-loaded CDN - the same
// capacity-weighted, latency-aware ranking the striped path uses - but with the
// selection serialized so a burst of concurrent flows actually spreads.
//
// The bug this fixes: dispatching each plain flow with the single lowest-score
// session concentrated a burst of them onto one connection. Selection reads a
// session's stream count, and OpenStream only bumps that count after the pick;
// N flows dispatched at once (an Ookla run opens ~16 in a burst) all read the
// same pre-OpenStream load and pick the same session before any of their
// OpenStreams register. Every stream then shares one TCP connection to one CDN
// edge, and that single connection's origin->edge (upload) ceiling - well below
// its edge->origin (download) ceiling behind the CDN - caps the aggregate,
// which is the fast-download / throttled-upload asymmetry seen in production.
// The old per-session work-stealing loop never hit this: each pool session
// pulled independently from the shared local channel, so flows spread across
// connections and their throughput summed.
//
// Placement is a reservation, not a held lock. Under plainSelectMu each opener
// scores the live sessions, picks the best and reserves it (pendingOpens++), so
// the next opener already sees that load; the lock is released BEFORE
// OpenStream, which can block on a stalled connection's SYN write, and the
// reservation is dropped under the lock when OpenStream returns either way. One
// stalled session therefore delays only the openers that picked it, and later
// flows are placed on the healthy sessions (the stalled one scores worse for
// every reservation it holds). RTT is probed on this path too (see handleLoop),
// so the spread favours the low-latency CDNs and leaves the slow tail unused
// rather than round-robining blindly onto it.
//
// Narrow overcount: smux registers the stream in NumStreams just before
// OpenStream returns, so for that instant the opener counts twice (stream +
// reservation). That only makes its session look slightly busier for a moment -
// conservative and self-correcting - and is not tracked more precisely.
//
// Lock order: plainSelectMu, then sessionsMu (only for the registry snapshot).
// Nothing takes them the other way round; register/unregister take sessionsMu
// alone.
func (s *WsMuxTransport) openPlainLeg() (*smux.Stream, error) {
	stream, _, err := s.openPlainLegPS()
	return stream, err
}

// openPlainLegPS is openPlainLeg that also says which pool session carries the
// stream, so the dispatcher can see what that session negotiated.
//
// A session that closes (or is rotated out) after it was picked makes OpenStream
// fail; it is then excluded and the flow re-scored over the rest, so no
// candidate is tried twice. A stream that arrives late is returned to the
// caller like any other (the setup attempt closes it if it has expired), and a
// session that started draining after the pick is not re-admitted: eligibility
// is only ever decided at selection, from the live registry.
func (s *WsMuxTransport) openPlainLegPS() (*smux.Stream, *pooledSession, error) {
	tried := make(map[*pooledSession]bool)
	for {
		ps, live := s.reservePlainLeg(tried)
		if ps == nil {
			if live == 0 {
				return nil, nil, fmt.Errorf("no live pool session available for a plain leg")
			}
			return nil, nil, fmt.Errorf("all %d pool session(s) failed to open a plain leg", live)
		}

		stream, err := ps.session.OpenStream() // may block: no lock held
		s.releasePlainLeg(ps)
		if err == nil {
			return stream, ps, nil
		}
		s.logger.Tracef("plain leg: OpenStream on a pool session failed, trying the next: %v", err)
		tried[ps] = true
	}
}

// reservePlainLeg picks the best-scoring live session not in tried and reserves
// it. It returns nil (and how many live sessions it saw) when none is left.
func (s *WsMuxTransport) reservePlainLeg(tried map[*pooledSession]bool) (*pooledSession, int) {
	s.plainSelectMu.Lock()
	defer s.plainSelectMu.Unlock()

	s.sessionsMu.Lock()
	avail := make([]*pooledSession, 0, len(s.sessions))
	live := 0
	for _, ps := range s.sessions {
		if ps.session == nil || ps.session.IsClosed() {
			continue
		}
		live++
		if !tried[ps] {
			avail = append(avail, ps)
		}
	}
	s.sessionsMu.Unlock()

	if len(avail) == 0 {
		return nil, live
	}
	// First lowest score wins, so equal scores keep registry order (as the old
	// stable sort did).
	best, bestScore := avail[0], placementScore(avail[0])
	for _, ps := range avail[1:] {
		if sc := placementScore(ps); sc < bestScore {
			best, bestScore = ps, sc
		}
	}
	best.pendingOpens++
	return best, live
}

func (s *WsMuxTransport) releasePlainLeg(ps *pooledSession) {
	s.plainSelectMu.Lock()
	ps.pendingOpens--
	s.plainSelectMu.Unlock()
}

// placementScore is legScore plus the plain opens already reserved on the
// session. Callers hold plainSelectMu. The striped path keeps using legScore
// (it never reserves); a new placement mode that shares sessions with plain
// flows must add pendingOpens too, or it can burst onto one session.
func placementScore(ps *pooledSession) float64 {
	return legScoreValue(ps.session.NumStreams()+ps.pendingOpens, ps.rtt.Load())
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
