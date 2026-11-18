package handlers

import "sync"

const (
	// ReplayFlowStart is the replay ring a flow starts with (the smallest useful
	// one). Memory is only held while bytes are in flight.
	ReplayFlowStart = minReplayLimit
	// replayStartCharge is what a starting ring counts against the budget. A ring
	// holds only what is unacknowledged, and an idle flow (a kept-alive
	// connection, SSH) holds next to nothing: charging every one of them its full
	// starting limit let a few hundred of them use up the budget of a process
	// held to a small heap, and the flows that were busy could then not grow
	// their rings, which caps a flow at its ring per acknowledgement round trip.
	// Growth is charged in full, so what a busy flow holds is always counted.
	replayStartCharge = ReplayFlowStart / 4
	// ReplayFlowMax is the most a flow's ring can grow to. The ring bounds the
	// flow's rate to about its size per acknowledgement round trip, so this is what
	// lets one flow fill a long fat path: 16 MiB at 80 ms RTT is about 200 MB/s.
	ReplayFlowMax = 16 << 20
	// ReplayBudgetTotal caps what all flows of a process may hold.
	ReplayBudgetTotal = 256 << 20
)

// ReplayBudget bounds the replay memory of a process. A flow starts with the
// smallest ring and only grows it (doubling, up to ReplayFlowMax) when it finds it
// full, so idle flows (SSH) cost almost nothing and a bulk flow gets what it needs
// while the budget lasts. The counted size is the ring's limit, i.e. the worst
// case, not what is allocated at the moment.
type ReplayBudget struct {
	mu    sync.Mutex
	total int
	used  int
	// How often the budget said no, for the pool health line: a flow refused a
	// ring runs without replay, one refused growth is held to its ring per
	// acknowledgement round trip.
	openRefused, growRefused int
}

// NewReplayBudget returns a budget of total bytes.
func NewReplayBudget(total int) *ReplayBudget { return &ReplayBudget{total: total} }

// replayHeapShare is the part of a heap limit that replay rings may hold in all.
// Measured, bulk flows through a process held to a 143 MiB heap spent twice the
// CPU per byte when their rings were free to fill 256 MiB: the heap sat at its
// limit and the collector ran 150 times a second. A quarter of the limit leaves
// room for everything else a flow holds and costs the flows no speed.
const (
	replayHeapShare = 4
	replayBudgetMin = 4 << 20
)

// ReplayBudgetFor is the replay budget that goes with a heap limit (0 = none): all
// of ReplayBudgetTotal without a limit, a share of the limit with one.
func ReplayBudgetFor(heapLimit int64) int {
	if heapLimit <= 0 {
		return ReplayBudgetTotal
	}
	return int(min(int64(ReplayBudgetTotal), max(int64(replayBudgetMin), heapLimit/replayHeapShare)))
}

// ReplayStats is what a budget allows, what its flows have been charged and how
// often it refused (see ReplayBudget).
type ReplayStats struct {
	Total, Used              int
	OpenRefused, GrowRefused int
}

// Stats returns the budget's figures now.
func (b *ReplayBudget) Stats() ReplayStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return ReplayStats{b.total, b.used, b.openRefused, b.growRefused}
}

// Used is what the flows have been charged so far.
func (b *ReplayBudget) Used() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}

// SetTotal changes what the budget allows. Rings that are already larger keep what
// they hold until their flows end; nothing new is granted past the new total.
func (b *ReplayBudget) SetTotal(total int) {
	b.mu.Lock()
	b.total = total
	b.mu.Unlock()
}

// DefaultReplayBudget is the process-wide budget.
var DefaultReplayBudget = NewReplayBudget(ReplayBudgetTotal)

// ReplayGrant is one flow's share of a budget.
type ReplayGrant struct {
	b    *ReplayBudget
	mu   sync.Mutex
	held int
}

// Open gives a flow its starting ring, or nil when the budget cannot even cover
// that (the flow then runs without replay). With force the budget may be exceeded,
// by at most a quarter: a flow whose peer already decided it uses replay must too
// (the two ends have to agree), but the process stays bounded; past that, nil.
func (b *ReplayBudget) Open(force bool) *ReplayGrant {
	b.mu.Lock()
	defer b.mu.Unlock()
	limit := b.total
	if force {
		limit += b.total / 4
	}
	if limit-b.used < replayStartCharge {
		b.openRefused++
		return nil
	}
	b.used += replayStartCharge
	return &ReplayGrant{b: b, held: replayStartCharge}
}

// Limit is the ring limit the flow starts with.
func (g *ReplayGrant) Limit() int { return ReplayFlowStart }

// Grow returns the next ring limit for a flow whose ring (cur) is full: double,
// up to ReplayFlowMax, if the budget allows; cur otherwise.
func (g *ReplayGrant) Grow(cur int) int {
	next := min(cur*2, ReplayFlowMax)
	if next <= cur {
		return cur
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if need := next - g.held; need > 0 {
		g.b.mu.Lock()
		defer g.b.mu.Unlock()
		if g.b.total-g.b.used < need {
			g.b.growRefused++
			return cur
		}
		g.b.used += need
		g.held = next
	}
	return next
}

// Shrink takes a grown ring back to its starting size and gives the budget the
// difference, for a flow whose ring has been empty for a while: a kept-alive
// connection that has finished a transfer would otherwise hold what that
// transfer needed for as long as it lives. It returns the starting limit.
func (g *ReplayGrant) Shrink() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.held > replayStartCharge {
		g.b.mu.Lock()
		g.b.used -= g.held - replayStartCharge
		g.b.mu.Unlock()
		g.held = replayStartCharge
	}
	return ReplayFlowStart
}

// Release gives everything back; safe to call more than once and on nil.
func (g *ReplayGrant) Release() {
	if g == nil {
		return
	}
	g.mu.Lock()
	held := g.held
	g.held = 0
	g.mu.Unlock()
	g.b.mu.Lock()
	g.b.used -= held
	g.b.mu.Unlock()
}
