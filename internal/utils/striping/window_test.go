package striping

import "testing"

// A leg may have outstanding what it delivers while the others fill the target:
// the slow leg of a fast flow is held short, equal legs and a leg alone are not,
// a pause changes nothing, and no window more than doubles at a time or drops
// below the floor.
func TestLegWindow(t *testing.T) {
	const (
		MB     = 1 << 20
		target = 16 * MB
		floor  = 64 << 10
	)
	for _, tc := range []struct {
		name                   string
		cur, own, others, want float64
	}{
		{"slow leg next to a fast one", 4 * MB, 6 * MB, 100 * MB, 0.96 * MB},
		{"fast leg next to a slow one", 16 * MB, 100 * MB, 6 * MB, 16 * MB},
		{"equal legs", 16 * MB, 50 * MB, 50 * MB, 16 * MB},
		{"one of six equal legs", 8 * MB, 17 * MB, 85 * MB, 3.2 * MB},
		{"alone", 16 * MB, 3 * MB, 0, 16 * MB},
		{"after a pause", 1 * MB, 6, 100, 0.96 * MB},
		{"nothing delivered yet", 256 << 10, 0, 0, 256 << 10},
		{"quiet for long", 3 * MB, 0, 0, 3 * MB},
		{"grows by doubling", 256 << 10, 50 * MB, 50 * MB, 512 << 10},
		{"idle next to a busy one", 4 * MB, 0, 100 * MB, floor},
		{"never below the floor", floor, 0.1 * MB, 100 * MB, floor},
	} {
		if got := legWindow(tc.cur, tc.own, tc.others, target, floor); got < tc.want*0.99 || got > tc.want*1.01 {
			t.Errorf("%s: window %.0f, want %.0f", tc.name, got, tc.want)
		}
	}
}
