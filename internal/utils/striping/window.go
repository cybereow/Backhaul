package striping

import (
	"net"
	"sync/atomic"
	"time"
)

// Leg receive windows.
//
// A leg's transport lets the sender keep a window of bytes outstanding on it:
// written, not yet read here. On a leg that delivers 6 MB/s, a 16 MiB window is
// almost three seconds of chunks queued ahead of the one Read is waiting for -
// and whatever the other legs deliver in those seconds has to be held, out of
// order, against the reassembly budget. A fast leg next to a slow one fills the
// budget in a fraction of a second, so a striped flow over unequal paths was
// only ever as fast as its slowest leg, or died.
//
// How far a leg may run behind is therefore decided here, on the side that pays
// for it. Each leg may have outstanding only what it delivers in the time the
// other legs need to fill half the budget:
//
//	window(i) = rate(i) * (budget/2) / sum of the other legs' rates
//
// so the slow leg of a fast flow is held to a fraction of a second and still
// carries what it can, and equal legs keep large windows. Rates are what each
// leg was seen to deliver here; they fade together when the flow goes quiet, so
// a pause leaves the windows as they were. A window at most doubles per tick,
// starting small: a leg has to show what it delivers before it is trusted with
// more.
//
// This keeps the legs together in the steady state. It cannot see a change
// coming - a leg that stalls, a flow that starts - so the budget itself is
// enforced by making the legs that ran ahead wait (Conn.admit).
//
// Only legs whose transport can change its window take part (an smux stream);
// any other leg is left as it is.
const (
	// windowTick is how often the leg windows are recomputed.
	windowTick = 100 * time.Millisecond
	// windowInitial is a leg's window until it has been measured. It is what an
	// smux sender assumes before the first update, so starting here costs nothing.
	windowInitial = 256 << 10
	// windowMinChunks is the least a leg is ever allowed outstanding, in chunks:
	// enough to keep it measured, so a leg that recovers is seen to.
	windowMinChunks = 4
	// windowRateDecay is how much of a leg's rate estimate survives a tick in
	// which it delivered less: the estimate follows a rise at once and forgets a
	// burst in about half a second.
	windowRateDecay = 0.85
)

type receiveWindowSetter interface{ SetReceiveWindow(int) error }

// legWindow is the window for a leg delivering own bytes/s next to legs that
// together deliver others, given its current window cur.
func legWindow(cur, own, others, target, floor float64) float64 {
	w := target // nothing else is delivering: nothing to run behind
	if others > 0 {
		w = min(target, own*target/others)
	}
	return max(min(w, 2*cur), floor)
}

// HoldLeg puts a leg on its initial window. New does it for every leg, but only
// once it has them all; a transport that announces its full window on the first
// read (smux does) has by then let a leg that arrived early be filled with
// seconds of data. So the side that accepts legs calls this the moment it knows
// a stream is one - before waiting for the rest. Calling it again changes
// nothing.
func HoldLeg(leg net.Conn) {
	if s, ok := leg.(receiveWindowSetter); ok {
		_ = s.SetReceiveWindow(windowInitial)
	}
}

// windowLoop keeps every leg's receive window in step with what the legs
// deliver. It ends with the Conn.
func (c *Conn) windowLoop() {
	setters := make([]receiveWindowSetter, len(c.legs))
	any := false
	for i, leg := range c.legs {
		if s, ok := leg.(receiveWindowSetter); ok {
			setters[i], any = s, true
		}
	}
	if !any || len(c.legs) < 2 {
		return
	}

	target := float64(c.budget.limit / 2)
	floor := float64(windowMinChunks * (c.chunkSize + headerSize))
	rate := make([]float64, len(c.legs))
	win := make([]float64, len(c.legs))
	for i := range win {
		win[i] = windowInitial
	}

	t := time.NewTicker(windowTick)
	defer t.Stop()
	last := time.Now()
	for {
		select {
		case <-c.closed:
			return
		case now := <-t.C:
			dt := now.Sub(last).Seconds()
			last = now
			if dt <= 0 {
				continue
			}
			sum := 0.0
			for i := range rate {
				keep := rate[i] * windowRateDecay
				if atomic.SwapUint32(&c.legWaited[i], 0) != 0 || atomic.LoadUint64(&c.parkAt[i]) != 0 {
					// It was made to wait here, for a leg that fell behind. That
					// says nothing about what it delivers, and taking it for slower
					// would widen the window of the very leg it is waiting for.
					keep = rate[i]
				}
				rate[i] = max(float64(atomic.SwapInt64(&c.legRead[i], 0))/dt, keep)
				sum += rate[i]
			}
			for i, s := range setters {
				win[i] = legWindow(win[i], rate[i], sum-rate[i], target, floor)
				if s != nil {
					_ = s.SetReceiveWindow(int(win[i])) // a dead leg fails the Conn through readLeg
				}
			}
		}
	}
}
