package transport

import "time"

// The stall tests run in a process full of other tests that keep the CPU busy:
// whether it was starved is not what they are about, and is not theirs to see.
func init() {
	processStarved = func(time.Time) bool { return false }
}
