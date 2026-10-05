package transport

import (
	"math"
	"net"
	"strings"
	"time"

	"github.com/musix/backhaul/internal/utils/network"
)

// Capacity-aware placement.
//
// RTT says how far away a pool connection is, not how much it carries. Two CDNs
// a few milliseconds apart can differ twentyfold in what one connection delivers
// from this server (measured: ~950 Mbps through one, ~55 Mbps through another
// whose edge never opens its receive window past 1 MiB), and legScore alone
// spreads flows over both as if they were equal. Every flow that lands on the
// slow one is pinned there for its life.
//
// So the server measures what a connection of each domain actually delivers and
// charges the slow domains for it. The measurement comes from the kernel
// (TCP_INFO): how many bytes the peer acknowledged over a capSampleEvery. Two
// things are learned from it:
//
//   - what a connection was seen to carry. Any interval says the path carries at
//     least that much.
//   - whether a connection was the limit: an interval during which it was
//     backlogged throughout - busy the whole time, with unsent data queued at
//     both ends. Only then did the path, not the sender, set the pace. A fast
//     path the server cannot fill is never seen like this.
//
// Both are kept per host - the domain the client dialed - not per connection:
// the connections of one domain share its path, a sample from any of them
// speaks for all (including ones that join later), and a single connection's
// own figures are too noisy to rank it against its siblings. Each is the best
// sample of the last two capBucket periods.
//
// A host is charged only if it has been seen limited, by how far the most any
// host was seen to carry exceeds the most this one was. The charge multiplies
// the placement score, so a slow domain still takes flows once the fast ones are
// loaded enough, and every domain stays in the pool. A host never seen limited
// is not charged: nothing is known against it. Estimates expire, so a path that
// recovers is tried again.
//
// What this measures is the server's sending direction, which carries the
// user's upload: the direction in which CDNs differ.
const (
	capSampleEvery = time.Second
	capBucket      = 5 * time.Minute
	// capMinProgress is the least a connection that was not backlogged must have
	// delivered in an interval for it to count: below this it is idle or just
	// starting, not showing what its path carries. A backlogged connection counts
	// whatever it delivered: with data queued throughout, a trickle is exactly
	// what the path carried.
	capMinProgress = 256 << 10
	// capBusyShare is how much of an interval a backlogged connection must have
	// been busy.
	capBusyShare = 0.9
	// capIgnoreBelow is the ratio to the best estimate under which a host is not
	// charged at all: equal paths sharing the server's uplink differ by this much.
	capIgnoreBelow = 2.0
	// capMaxPenalty bounds the charge, so a slow domain is still used when the
	// fast ones carry this many times its load.
	capMaxPenalty = 32.0
)

// peakRate is the highest rate seen in the current and the previous bucket.
type peakRate struct {
	cur, prev float64
	start     time.Time // of the current bucket
}

func (p *peakRate) roll(now time.Time) {
	if p.start.IsZero() {
		p.start = now
		return
	}
	switch age := now.Sub(p.start); {
	case age >= 2*capBucket:
		p.cur, p.prev, p.start = 0, 0, now
	case age >= capBucket:
		p.prev, p.cur, p.start = p.cur, 0, p.start.Add(capBucket)
	}
}

func (p *peakRate) add(now time.Time, v float64) {
	p.roll(now)
	if v > p.cur {
		p.cur = v
	}
}

func (p *peakRate) get(now time.Time) float64 {
	p.roll(now)
	return math.Max(p.cur, p.prev)
}

// hostCapacity is what the connections of one host were seen to do.
type hostCapacity struct {
	seen    peakRate // the most one of them delivered in an interval
	limited peakRate // the most one of them delivered while backlogged throughout
}

// estimate is the most a connection of the host was seen to carry, and whether
// the host has been seen limited (so that figure is a ceiling, not just a floor).
func (h *hostCapacity) estimate(now time.Time) (bps float64, limited bool) {
	return h.seen.get(now), h.limited.get(now) > 0
}

// capSample is one look at a session's socket.
type capSample struct {
	at    time.Time
	d     network.TCPDelivery
	valid bool
}

// deliveredRate is what the connection delivered between two looks, in bytes per
// second, and whether it was backlogged throughout. ok is false when the
// interval says nothing: no socket, nothing moved, or too little moved without
// a backlog.
func deliveredRate(prev, cur capSample) (rate float64, backlogged, ok bool) {
	if !prev.valid || !cur.valid {
		return 0, false, false
	}
	dt := cur.at.Sub(prev.at)
	if dt <= 0 || cur.d.BytesAcked < prev.d.BytesAcked {
		return 0, false, false
	}
	acked := cur.d.BytesAcked - prev.d.BytesAcked
	busy := cur.d.Busy - prev.d.Busy
	backlogged = prev.d.NotSent > 0 && cur.d.NotSent > 0 && !cur.d.AppLimited &&
		float64(busy) >= capBusyShare*float64(dt)
	if acked == 0 || (acked < capMinProgress && !backlogged) {
		return 0, false, false
	}
	return float64(acked) / dt.Seconds(), backlogged, true
}

// hostKey reduces an upgrade request's Host to the domain the client dialed.
func hostKey(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	return strings.ToLower(host)
}

// slowness is the session's placement penalty: 1 for a session whose domain is
// not known to be slow.
func (ps *pooledSession) slowness() float64 {
	slow := 1.0
	if b := ps.slow.Load(); b != 0 {
		slow = math.Float64frombits(b)
	}
	if ps.stalledSince.Load() != 0 {
		slow *= stallPenalty
	}
	return slow
}

// capacityPenalty is the charge for a host that carries est when the best
// carries best (both in the same unit; 0 = unknown).
func capacityPenalty(est, best float64) float64 {
	if est <= 0 || best <= 0 || best < est*capIgnoreBelow {
		return 1
	}
	return math.Min(best/est, capMaxPenalty)
}

// capacityLoop samples every pool session once a capSampleEvery and refreshes
// the penalties. It is the only goroutine that touches the estimates.
func (s *WsMuxTransport) capacityLoop(g *wsGeneration) {
	t := time.NewTicker(capSampleEvery)
	defer t.Stop()
	hosts := make(map[string]*hostCapacity)
	for {
		select {
		case <-g.ctx.Done():
			return
		case now := <-t.C:
			s.sessionsMu.Lock()
			sessions := make([]*pooledSession, len(s.sessions))
			copy(sessions, s.sessions)
			s.sessionsMu.Unlock()
			sampleCapacity(sessions, hosts, now, network.TCPDeliveryInfo)
			s.moveOffStalled(g, sessions, now)
		}
	}
}

// sampleCapacity is one pass of capacityLoop over sessions: look at each
// socket, credit its host with what it delivered since the last look, then
// recompute every penalty. info is network.TCPDeliveryInfo.
func sampleCapacity(sessions []*pooledSession, hosts map[string]*hostCapacity, now time.Time, info func(net.Conn) (network.TCPDelivery, bool)) {
	for _, ps := range sessions {
		cur := capSample{at: now}
		if ps.conn != nil {
			cur.d, cur.valid = info(ps.conn)
		}
		ps.noteHealth(now, cur.valid && cur.d.Backoff >= stallBackoff)
		rate, backlogged, ok := deliveredRate(ps.capLast, cur)
		ps.capLast = cur
		if !ok {
			continue
		}
		h := hosts[ps.host]
		if h == nil {
			h = &hostCapacity{}
			hosts[ps.host] = h
		}
		h.seen.add(now, rate)
		if backlogged {
			h.limited.add(now, rate)
		}
	}

	best := 0.0
	for k, h := range hosts {
		e, _ := h.estimate(now)
		if e == 0 { // nothing in two buckets: the host is forgotten
			delete(hosts, k)
			continue
		}
		best = math.Max(best, e)
	}
	for _, ps := range sessions {
		est, penalty := 0.0, 1.0
		if h := hosts[ps.host]; h != nil {
			var limited bool
			if est, limited = h.estimate(now); limited {
				penalty = capacityPenalty(est, best)
			}
		}
		ps.capEst.Store(uint64(est))
		ps.slow.Store(math.Float64bits(penalty))
	}
}
