package proclimits

import (
	"context"
	"sync/atomic"
	"time"
)

// A process that is not getting its CPU time looks, from outside, like a network
// that has stopped: its reads happen late, its answers go out late, and the peer
// sees silence on a connection that is perfectly healthy. A container held to a
// fraction of a core (a CPU quota) is the plain case - the kernel stops the whole
// process for most of every scheduling period once the quota is spent - but a
// host that is oversubscribed or a VM that is stolen from looks the same.
//
// Anything that judges a connection by how late its answers are has to know this
// first, or it will take its own slowness for the path's and act on it. Monitor
// measures it where it shows, in the process itself: a ticker that fires on time
// when the process runs and late when it did not.

const (
	// starveTick is how often the monitor looks at the time.
	starveTick = 50 * time.Millisecond
	// starveLate is how late a look has to be to count: far more than a garbage
	// collection or an ordinary scheduling hiccup costs.
	starveLate = 40 * time.Millisecond
	// StarveHold is how long after a late look the process is still taken to be
	// starved: the answers it owes were held up with it, and arrive late after.
	StarveHold = 3 * time.Second
)

// Monitor measures how late the process's own timers fire. The zero value is
// ready: before Start it has seen nothing, and reports no starvation.
type Monitor struct {
	last     atomic.Int64 // when the last late look happened (unix nanos, 0 = never)
	worst    atomic.Int64 // the latest look's lateness, largest since Delay last reset it (nanos)
	prevTick atomic.Int64 // when the previous look happened (unix nanos)
	running  atomic.Bool
}

// Observe records a look at time now. A look is expected starveTick after the
// previous one; the excess is how long the process was kept from running.
func (m *Monitor) Observe(now time.Time) {
	prev := m.prevTick.Swap(now.UnixNano())
	if prev == 0 {
		return
	}
	late := now.UnixNano() - prev - int64(starveTick)
	if late <= 0 {
		return
	}
	for {
		w := m.worst.Load()
		if late <= w || m.worst.CompareAndSwap(w, late) {
			break
		}
	}
	if late >= int64(starveLate) {
		m.last.Store(now.UnixNano())
	}
}

// Starved reports whether the process was kept from running within the last
// StarveHold before now.
func (m *Monitor) Starved(now time.Time) bool {
	last := m.last.Load()
	return last != 0 && now.UnixNano()-last < int64(StarveHold)
}

// Delay returns the longest the process was kept from running since the previous
// call, and starts counting again.
func (m *Monitor) Delay() time.Duration {
	return time.Duration(m.worst.Swap(0))
}

// Start runs the monitor until ctx ends. It is one monitor's worth of looking: a
// second Start while one is running does nothing.
func (m *Monitor) Start(ctx context.Context) {
	if !m.running.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer m.running.Store(false)
		m.prevTick.Store(0)
		t := time.NewTicker(starveTick)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-t.C:
				m.Observe(now)
			}
		}
	}()
}

// Sched is the process's own monitor.
var Sched Monitor
