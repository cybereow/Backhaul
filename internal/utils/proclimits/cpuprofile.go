package proclimits

import (
	"bytes"
	"fmt"
	"runtime/pprof"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/pprof/profile"
)

// Where a process spends its CPU is the one thing a log of counters cannot say,
// and the one thing needed when a container is held to a fraction of a core and a
// tunnel still moves less than it should. TopCPU samples the process for d and
// describes the result in a line, so that a debug log carries the answer.

// TopCPU profiles the process for d and returns a one-line summary: the functions
// that used the most CPU themselves (flat), and the application's own functions
// that used the most including what they called (cum), as shares of the samples.
// It fails when another CPU profile is running (the operator's own pprof).
func TopCPU(d time.Duration, n int) (string, error) {
	var buf bytes.Buffer
	if err := pprof.StartCPUProfile(&buf); err != nil {
		return "", err
	}
	time.Sleep(d)
	pprof.StopCPUProfile()
	p, err := profile.Parse(&buf)
	if err != nil {
		return "", err
	}
	return summarize(p, n), nil
}

// summarize turns a CPU profile into the line TopCPU returns.
func summarize(p *profile.Profile, n int) string {
	idx := -1
	for i, st := range p.SampleType {
		if st.Type == "cpu" {
			idx = i
		}
	}
	if idx < 0 {
		return "no cpu samples"
	}
	flat, cum := map[string]int64{}, map[string]int64{}
	var total int64
	for _, s := range p.Sample {
		v := s.Value[idx]
		total += v
		seen := map[string]bool{}
		for i, loc := range s.Location {
			for _, ln := range loc.Line {
				name := ln.Function.Name
				if i == 0 && ln == loc.Line[0] {
					flat[name] += v
				}
				if strings.Contains(name, "/backhaul/") && !seen[name] {
					seen[name] = true
					cum[name] += v
				}
			}
		}
	}
	if total == 0 {
		return "no cpu samples"
	}
	cpu := time.Duration(total)
	return fmt.Sprintf("cpu profile (%v of samples): flat %s | app cum %s", cpu.Round(time.Millisecond), top(flat, total, n, false), top(cum, total, n, true))
}

func top(m map[string]int64, total int64, n int, short bool) string {
	type kv struct {
		k string
		v int64
	}
	all := make([]kv, 0, len(m))
	for k, v := range m {
		all = append(all, kv{k, v})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].v > all[j].v })
	if len(all) > n {
		all = all[:n]
	}
	out := make([]string, len(all))
	for i, e := range all {
		name := e.k
		if short { // keep the package and function, drop the module path
			if j := strings.Index(name, "/backhaul/"); j >= 0 {
				name = name[j+len("/backhaul/"):]
			}
		}
		out[i] = fmt.Sprintf("%s %d%%", name, 100*e.v/total)
	}
	return strings.Join(out, ", ")
}

// Sampler takes a CPU profile now and then, from a busy process only: the caller
// says when it is busy. One profile at a time, and never closer together than
// every.
type Sampler struct {
	last    atomic.Int64
	running atomic.Bool
}

// Maybe starts a profile of d in the background, unless one is running or the
// last was less than every ago, and passes its summary (top n functions) to log.
func (s *Sampler) Maybe(now time.Time, every, d time.Duration, n int, log func(string)) {
	if last := s.last.Load(); last != 0 && now.UnixNano()-last < int64(every) {
		return
	}
	if !s.running.CompareAndSwap(false, true) {
		return
	}
	s.last.Store(now.UnixNano())
	go func() {
		defer s.running.Store(false)
		if line, err := TopCPU(d, n); err == nil {
			log(line)
		}
	}()
}
