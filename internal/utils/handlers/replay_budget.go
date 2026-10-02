package handlers

import "sync"

const (
	// ReplayFlowStart is the replay ring a flow starts with (the smallest useful
	// one). Memory is only held while bytes are in flight.
	ReplayFlowStart = minReplayLimit
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
}

// NewReplayBudget returns a budget of total bytes.
func NewReplayBudget(total int) *ReplayBudget { return &ReplayBudget{total: total} }

// DefaultReplayBudget is the process-wide budget.
var DefaultReplayBudget = NewReplayBudget(ReplayBudgetTotal)

// ReplayGrant is one flow's share of a budget.
type ReplayGrant struct {
	b    *ReplayBudget
	mu   sync.Mutex
	held int
}

// Open gives a flow its starting ring, or nil when the budget cannot even cover
// that (the flow then runs without replay). With force it always succeeds: a flow
// whose peer already decided it uses replay must too, the two ends have to agree.
func (b *ReplayBudget) Open(force bool) *ReplayGrant {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !force && b.total-b.used < ReplayFlowStart {
		return nil
	}
	b.used += ReplayFlowStart
	return &ReplayGrant{b: b, held: ReplayFlowStart}
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
			return cur
		}
		g.b.used += need
		g.held = next
	}
	return next
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
