// Package proclimits reads what the container this process runs in allows it -
// CPU time and memory, from its cgroup - and what it has used of them.
//
// The Go runtime (before 1.25) sizes itself by the machine: one running thread
// per core of the host. In a container that is allowed one core of a large host
// that is exactly wrong. The threads together spend the whole quota of a
// scheduling period in a fraction of it, and the kernel then stops the process
// until the next period: every goroutine, for tens of milliseconds, many times a
// second. A tunnel under load then answers late on every connection at once while
// its sockets look idle. Apply sizes the runtime by the container instead.
package proclimits

import (
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"strconv"
	"strings"
	"time"
)

// root is where the process's own cgroup is mounted (tests point it elsewhere).
var root = "/sys/fs/cgroup"

// Limits is what the container allows; a zero field means no limit was found.
type Limits struct {
	CPUs   float64 // cores' worth of CPU time
	Memory int64   // bytes
}

// Usage is what the process (its container, for the cgroup figures) has used.
// CPU, Throttles and Throttled only grow: compare two readings.
type Usage struct {
	CPU        time.Duration // CPU time used
	Throttles  int64         // scheduling periods cut short by the CPU quota
	Throttled  time.Duration // time spent stopped for it
	Memory     int64         // bytes charged to the container
	Heap       int64         // bytes of Go heap in use
	Goroutines int
	Procs      int // threads the runtime may run at once
}

func readFile(name string) string {
	b, err := os.ReadFile(root + "/" + name)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func readInt(name string) (int64, bool) {
	v, err := strconv.ParseInt(readFile(name), 10, 64)
	return v, err == nil
}

// stat reads a "key value" file into a map.
func stat(name string) map[string]int64 {
	m := map[string]int64{}
	for _, line := range strings.Split(readFile(name), "\n") {
		if f := strings.Fields(line); len(f) == 2 {
			if v, err := strconv.ParseInt(f[1], 10, 64); err == nil {
				m[f[0]] = v
			}
		}
	}
	return m
}

// noMemoryLimit is above any real limit: cgroup v1 reports "none" as a number
// near the top of int64.
const noMemoryLimit = 1 << 50

// Read reports the container's limits, from cgroup v2 or v1.
func Read() (l Limits) {
	if f := strings.Fields(readFile("cpu.max")); len(f) == 2 { // v2: "<quota|max> <period>"
		quota, err1 := strconv.ParseFloat(f[0], 64)
		period, err2 := strconv.ParseFloat(f[1], 64)
		if err1 == nil && err2 == nil && quota > 0 && period > 0 {
			l.CPUs = quota / period
		}
	} else {
		for _, dir := range []string{"cpu", "cpu,cpuacct"} {
			quota, ok1 := readInt(dir + "/cpu.cfs_quota_us")
			period, ok2 := readInt(dir + "/cpu.cfs_period_us")
			if ok1 && ok2 && quota > 0 && period > 0 {
				l.CPUs = float64(quota) / float64(period)
				break
			}
		}
	}
	for _, name := range []string{"memory.max", "memory/memory.limit_in_bytes"} {
		if v, ok := readInt(name); ok && v > 0 && v < noMemoryLimit {
			l.Memory = v
			break
		}
	}
	return l
}

// ReadUsage reports what has been used so far.
func ReadUsage() (u Usage) {
	if s := stat("cpu.stat"); len(s) > 0 { // v2 (v1 keeps this file in a subdirectory)
		u.CPU = time.Duration(s["usage_usec"]) * time.Microsecond
		u.Throttles = s["nr_throttled"]
		u.Throttled = time.Duration(s["throttled_usec"]) * time.Microsecond
	} else {
		for _, dir := range []string{"cpu", "cpu,cpuacct"} {
			if s := stat(dir + "/cpu.stat"); len(s) > 0 {
				u.Throttles = s["nr_throttled"]
				u.Throttled = time.Duration(s["throttled_time"]) // nanoseconds
				break
			}
		}
		for _, name := range []string{"cpuacct/cpuacct.usage", "cpu,cpuacct/cpuacct.usage"} {
			if v, ok := readInt(name); ok {
				u.CPU = time.Duration(v) // nanoseconds
				break
			}
		}
	}
	if u.CPU == 0 {
		// No cgroup figure: what the runtime itself accounts for.
		cpuSample := []metrics.Sample{{Name: "/cpu/classes/total:cpu-seconds"}, {Name: "/cpu/classes/idle:cpu-seconds"}}
		metrics.Read(cpuSample)
		if cpuSample[0].Value.Kind() == metrics.KindFloat64 && cpuSample[1].Value.Kind() == metrics.KindFloat64 {
			u.CPU = time.Duration((cpuSample[0].Value.Float64() - cpuSample[1].Value.Float64()) * float64(time.Second))
		}
	}
	for _, name := range []string{"memory.current", "memory/memory.usage_in_bytes"} {
		if v, ok := readInt(name); ok {
			u.Memory = v
			break
		}
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	u.Heap = int64(ms.HeapInuse)
	u.Goroutines = runtime.NumGoroutine()
	u.Procs = runtime.GOMAXPROCS(0)
	return u
}

// memoryShare is how much of the container's memory the Go heap is told to stay
// within. The rest is for what the kernel charges to the container besides: the
// socket buffers of every tunnel connection above all.
const memoryShare = 0.75

// Apply sizes the Go runtime by the container: as many running threads as the
// CPU quota covers, and a heap that is collected harder as it nears the memory
// limit instead of growing until the kernel kills the process. A setting the
// operator made (GOMAXPROCS, GOMEMLIMIT) is left alone. It reports what it set;
// zero means that setting was not changed.
func Apply(l Limits) (procs int, heap int64) {
	if l.CPUs > 0 && os.Getenv("GOMAXPROCS") == "" {
		if n := max(1, int(math.Ceil(l.CPUs))); n < runtime.GOMAXPROCS(0) {
			runtime.GOMAXPROCS(n)
			procs = n
		}
	}
	if l.Memory > 0 && os.Getenv("GOMEMLIMIT") == "" {
		heap = int64(float64(l.Memory) * memoryShare)
		debug.SetMemoryLimit(heap)
	}
	return procs, heap
}

// HeapLimit is the soft memory limit the Go runtime is under (from GOMEMLIMIT or
// Apply), 0 when there is none.
func HeapLimit() int64 {
	l := debug.SetMemoryLimit(-1)
	if l == math.MaxInt64 {
		return 0
	}
	return l
}
