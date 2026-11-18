package proclimits

import (
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"testing"
	"time"
)

func cgroup(t *testing.T, files map[string]string) {
	t.Helper()
	dir := t.TempDir()
	for name, body := range files {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	old := root
	root = dir
	t.Cleanup(func() { root = old })
}

func TestReadV2(t *testing.T) {
	cgroup(t, map[string]string{
		"cpu.max":        "150000 100000\n",
		"memory.max":     "1073741824\n",
		"memory.current": "536870912\n",
		"cpu.stat":       "usage_usec 2000000\nuser_usec 1\nnr_periods 50\nnr_throttled 7\nthrottled_usec 300000\n",
	})
	if l := Read(); l.CPUs != 1.5 || l.Memory != 1<<30 {
		t.Fatalf("limits = %+v", l)
	}
	u := ReadUsage()
	if u.CPU != 2*time.Second || u.Throttles != 7 || u.Throttled != 300*time.Millisecond || u.Memory != 512<<20 {
		t.Fatalf("usage = %+v", u)
	}
}

func TestReadV2Unlimited(t *testing.T) {
	cgroup(t, map[string]string{"cpu.max": "max 100000\n", "memory.max": "max\n"})
	if l := Read(); l != (Limits{}) {
		t.Fatalf("limits = %+v, want none", l)
	}
}

func TestReadV1(t *testing.T) {
	cgroup(t, map[string]string{
		"cpu/cpu.cfs_quota_us":         "50000\n",
		"cpu/cpu.cfs_period_us":        "100000\n",
		"cpu/cpu.stat":                 "nr_periods 9\nnr_throttled 4\nthrottled_time 250000000\n",
		"cpuacct/cpuacct.usage":        "3000000000\n",
		"memory/memory.limit_in_bytes": "268435456\n",
		"memory/memory.usage_in_bytes": "1048576\n",
	})
	if l := Read(); l.CPUs != 0.5 || l.Memory != 256<<20 {
		t.Fatalf("limits = %+v", l)
	}
	u := ReadUsage()
	if u.CPU != 3*time.Second || u.Throttles != 4 || u.Throttled != 250*time.Millisecond || u.Memory != 1<<20 {
		t.Fatalf("usage = %+v", u)
	}
}

func TestReadV1Unlimited(t *testing.T) {
	cgroup(t, map[string]string{
		"cpu/cpu.cfs_quota_us":         "-1\n",
		"cpu/cpu.cfs_period_us":        "100000\n",
		"memory/memory.limit_in_bytes": "9223372036854771712\n",
	})
	if l := Read(); l != (Limits{}) {
		t.Fatalf("limits = %+v, want none", l)
	}
}

func TestApply(t *testing.T) {
	t.Setenv("GOMAXPROCS", "")
	t.Setenv("GOMEMLIMIT", "")
	before := runtime.GOMAXPROCS(8)
	defer runtime.GOMAXPROCS(before)
	defer debug.SetMemoryLimit(debug.SetMemoryLimit(-1))

	procs, heap := Apply(Limits{CPUs: 1.5, Memory: 1 << 30})
	if procs != 2 || runtime.GOMAXPROCS(0) != 2 {
		t.Fatalf("procs = %d, runtime has %d, want 2", procs, runtime.GOMAXPROCS(0))
	}
	if want := int64(768 << 20); heap != want || debug.SetMemoryLimit(-1) != want || HeapLimit() != want {
		t.Fatalf("heap limit = %d, want %d", heap, want)
	}

	// Never more threads than there were, and nothing without a limit.
	runtime.GOMAXPROCS(2)
	if procs, _ := Apply(Limits{CPUs: 16}); procs != 0 || runtime.GOMAXPROCS(0) != 2 {
		t.Fatalf("a quota above the thread count changed it: %d", runtime.GOMAXPROCS(0))
	}
	if procs, heap := Apply(Limits{}); procs != 0 || heap != 0 {
		t.Fatalf("no limits, yet set %d threads and %d bytes", procs, heap)
	}

	// The operator's own setting wins.
	runtime.GOMAXPROCS(8)
	t.Setenv("GOMAXPROCS", "8")
	t.Setenv("GOMEMLIMIT", "1GiB")
	if procs, heap := Apply(Limits{CPUs: 1, Memory: 1 << 30}); procs != 0 || heap != 0 {
		t.Fatalf("operator settings overridden: %d threads, %d bytes", procs, heap)
	}
}

func TestSocketBufferCap(t *testing.T) {
	for _, c := range []struct {
		mem  int64
		want int
	}{
		{0, 0},                // no limit: left alone
		{64 << 20, 512 << 10}, // never less than half a megabyte
		{190 << 20, (190 << 20) / 128},
		{64 << 30, 8 << 20}, // never more than 8 MiB
	} {
		if got := socketBufferCap(c.mem); got != c.want {
			t.Errorf("socketBufferCap(%d MiB) = %d, want %d", c.mem>>20, got, c.want)
		}
	}
}
