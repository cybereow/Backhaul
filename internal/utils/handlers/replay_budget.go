package handlers

import "sync"

const (
	// ReplayFlowLimit is the replay ring a flow asks for: how many sent bytes may be
	// unacknowledged, which bounds the flow's rate to about this much per
	// acknowledgement round trip. Memory is only held while bytes are in flight.
	ReplayFlowLimit = 4 << 20
	// ReplayBudgetTotal caps what all flows of a process may reserve.
	ReplayBudgetTotal = 256 << 20
)

// ReplayBudget bounds the replay memory of a process. Each flow reserves its ring
// limit up front (a reservation, not an allocation: rings grow with use), so the
// worst case is known and a flow that does not fit simply runs without replay and
// keeps the planned-move behaviour.
type ReplayBudget struct {
	mu    sync.Mutex
	total int
	used  int
}

// NewReplayBudget returns a budget of total bytes.
func NewReplayBudget(total int) *ReplayBudget { return &ReplayBudget{total: total} }

// Reserve grants a ring limit: the full ReplayFlowLimit if it fits, else the
// smallest useful one, else 0 (no replay for this flow). release returns the
// reservation; it is safe to call more than once.
func (b *ReplayBudget) Reserve() (limit int, release func()) {
	b.mu.Lock()
	switch {
	case b.total-b.used >= ReplayFlowLimit:
		limit = ReplayFlowLimit
	case b.total-b.used >= minReplayLimit:
		limit = minReplayLimit
	}
	b.used += limit
	b.mu.Unlock()
	var once sync.Once
	return limit, func() {
		once.Do(func() {
			b.mu.Lock()
			b.used -= limit
			b.mu.Unlock()
		})
	}
}

// Force grants the smallest ring regardless of the budget, for a flow whose peer
// already decided it uses replay (the two ends must agree), and counts it.
func (b *ReplayBudget) Force() (limit int, release func()) {
	if limit, release = b.Reserve(); limit > 0 {
		return limit, release
	}
	b.mu.Lock()
	b.used += minReplayLimit
	b.mu.Unlock()
	var once sync.Once
	return minReplayLimit, func() {
		once.Do(func() {
			b.mu.Lock()
			b.used -= minReplayLimit
			b.mu.Unlock()
		})
	}
}

// DefaultReplayBudget is the process-wide budget.
var DefaultReplayBudget = NewReplayBudget(ReplayBudgetTotal)
