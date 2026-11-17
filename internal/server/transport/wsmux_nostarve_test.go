package transport

import "time"

// setProcessStarved makes processStarved answer with f until the returned
// function is called; nil puts the real monitor back.
func setProcessStarved(f func(time.Time) bool) (restore func()) {
	old := starvedOverride.Load()
	if f == nil {
		starvedOverride.Store(nil)
	} else {
		starvedOverride.Store(&f)
	}
	return func() { starvedOverride.Store(old) }
}

// The stall tests run in a process full of other tests that keep the CPU busy:
// whether it was starved is not what they are about, and is not theirs to see.
func init() {
	setProcessStarved(func(time.Time) bool { return false })
}
