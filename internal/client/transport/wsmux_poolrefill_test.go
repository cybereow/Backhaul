package transport

import "testing"

// TestPoolRefillCount guards the proactive pool top-up: after pool connections
// die (CDN reset, idle edge drop) the maintainer must rebuild them up to the
// configured floor, without double-counting in-flight dials and without dialing
// more than the per-tick cap. This is the fix for the "works, then after another
// idle it doesn't, until I reconnect" stall - a drained pool that never refilled
// because the initial fill ran once and the dynamic sizing only grew on load.
func TestPoolRefillCount(t *testing.T) {
	const perTick = 4
	cases := []struct {
		name                  string
		target, live, pending int
		want                  int
	}{
		{"full pool needs nothing", 32, 32, 0, 0},
		{"over target (dynamic growth) needs nothing", 32, 40, 0, 0},
		{"empty pool refills up to the per-tick cap", 32, 0, 0, perTick},
		{"in-flight dials count toward the deficit", 32, 28, 4, 0},
		{"small deficit dials exactly the deficit", 32, 31, 0, 1},
		{"deficit above cap is clamped to the cap", 32, 20, 0, perTick},
		{"pending covers part of the deficit", 32, 30, 1, 1},
		{"negative slack (stragglers) never dials", 8, 9, 2, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := poolRefillCount(tc.target, tc.live, tc.pending, perTick); got != tc.want {
				t.Errorf("poolRefillCount(%d,%d,%d,%d) = %d, want %d",
					tc.target, tc.live, tc.pending, perTick, got, tc.want)
			}
		})
	}
}
