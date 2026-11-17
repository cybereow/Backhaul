// Package poolhealth turns what the kernel and smux know about the connections
// of a tunnel pool into one line that says where the pool's data is waiting.
//
// A tunnel that slows down under load can be slow for very different reasons, and
// they look alike from outside: the path is full (bytes queue in this end's send
// buffer), the far end is not reading (this end is held by the peer's receive
// window), this end's own send buffer is too small, or smux has stopped reading a
// whole session because its receive budget ran out. Each asks for a different
// cure, so the line reports all of them, measured over the time since the last one.
package poolhealth

import (
	"fmt"
	"net"
	"sort"
	"time"

	"github.com/musix/backhaul/internal/smux"
	"github.com/musix/backhaul/internal/utils/handlers"
	"github.com/musix/backhaul/internal/utils/network"
	"github.com/musix/backhaul/internal/utils/proclimits"
)

// Member is one pool connection: its socket, the session on it, and how long its
// oldest RTT probe has been waiting for its echo (0 = none is out, or unknown).
type Member struct {
	Conn     net.Conn
	Session  *smux.Session
	ProbeOut time.Duration
}

type sample struct {
	at     time.Time
	path   network.TCPPath
	halts  int64
	halted time.Duration
}

// Reporter remembers the previous look at each session. It is not safe for
// concurrent use: one goroutine owns it.
type Reporter struct {
	role string
	prev map[*smux.Session]sample
	// proc is the process's own use at the previous look, procAt when it was taken.
	proc   proclimits.Usage
	procAt time.Time
	// the replay budget's refusals at the previous look
	replayOpenRefused, replayGrowRefused int
}

// New returns a Reporter whose lines are marked with role ("server", "client").
func New(role string) *Reporter {
	return &Reporter{role: role, prev: map[*smux.Session]sample{}}
}

// minRate is the traffic below which the pool counts as idle and no line is
// worth writing.
const minRate = 256 << 10 // bytes per second, both directions together

// Report looks at members now and describes the time since the previous call.
// ok is false when there is nothing to say: no earlier look to compare with, no
// socket to read, or a pool that is idle.
func (r *Reporter) Report(now time.Time, members []Member) (line string, ok bool) {
	next := make(map[*smux.Session]sample, len(members))
	var (
		n, streams, probes  int
		out, in             float64
		rtts, unsent, drain []float64
		unread              []float64
		rwnd, sndbuf, busy  []float64
		retrans             uint32
		halts               int64
		haltMax, probeMax   time.Duration
	)
	for _, m := range members {
		if m.Session == nil || m.Session.IsClosed() {
			continue
		}
		p, okp := network.TCPPathInfo(m.Conn)
		if !okp {
			continue
		}
		h, hd := m.Session.BudgetHalts()
		cur := sample{at: now, path: p, halts: h, halted: hd}
		next[m.Session] = cur
		streams += m.Session.NumStreams()
		if m.ProbeOut > 0 {
			probes++
			probeMax = max(probeMax, m.ProbeOut)
		}
		old, seen := r.prev[m.Session]
		if !seen {
			continue
		}
		dt := cur.at.Sub(old.at)
		if dt <= 0 {
			continue
		}
		n++
		out += float64(p.BytesAcked-old.path.BytesAcked) / dt.Seconds()
		in += float64(p.BytesRecv-old.path.BytesRecv) / dt.Seconds()
		rtts = append(rtts, float64(p.RTT.Milliseconds()))
		unsent = append(unsent, float64(p.NotSent)/1024)
		unread = append(unread, float64(p.Unread)/1024)
		if p.DeliveryRate > 0 {
			drain = append(drain, float64(p.NotSent)/float64(p.DeliveryRate)*1000)
		}
		share := func(a, b time.Duration) float64 { return 100 * float64(a-b) / float64(dt) }
		rwnd = append(rwnd, share(p.RwndLimited, old.path.RwndLimited))
		sndbuf = append(sndbuf, share(p.SndbufLimited, old.path.SndbufLimited))
		busy = append(busy, share(p.Busy, old.path.Busy))
		retrans += p.Retrans - old.path.Retrans
		halts += h - old.halts
		haltMax = max(haltMax, hd-old.halted)
	}
	r.prev = next
	process := r.process(now)
	if n == 0 || out+in < minRate {
		return "", false
	}
	return fmt.Sprintf("pool health (%s): %d conns, %d streams | out %.1f MB/s, in %.1f MB/s | rtt ms min/med/max %.0f/%.0f/%.0f | unsent KB med/max %.0f/%.0f | est. wait behind unsent ms med/max %.0f/%.0f | held by peer's window %% avg/max %.0f/%.0f | held by own send buffer %% avg/max %.0f/%.0f | busy %% avg %.0f | retrans %d | session budget halts %d (longest %v) | probes waiting %d (oldest %v) | unread KB med/max %.0f/%.0f | %s",
		r.role, n, streams, out/(1<<20), in/(1<<20),
		minOf(rtts), median(rtts), maxOf(rtts),
		median(unsent), maxOf(unsent),
		median(drain), maxOf(drain),
		mean(rwnd), maxOf(rwnd),
		mean(sndbuf), maxOf(sndbuf),
		mean(busy), retrans,
		halts, haltMax.Round(time.Millisecond),
		probes, probeMax.Round(time.Millisecond),
		median(unread), maxOf(unread), process), true
}

// process describes what this process used since the previous look, next to what
// its container allows. A pool whose sockets are all idle and whose answers are
// still late is a process that is not getting to run: held back by its CPU quota
// (throttled), or busy collecting a heap that has reached its memory limit.
func (r *Reporter) process(now time.Time) string {
	u, l := proclimits.ReadUsage(), proclimits.Read()
	old, oldAt := r.proc, r.procAt
	r.proc, r.procAt = u, now
	cores := 0.0
	if dt := now.Sub(oldAt); !oldAt.IsZero() && dt > 0 {
		cores = float64(u.CPU-old.CPU) / float64(dt)
	}
	limit := "no limit"
	if l.CPUs > 0 {
		limit = fmt.Sprintf("%.2f allowed", l.CPUs)
	}
	mem := "no limit"
	if l.Memory > 0 {
		mem = fmt.Sprintf("%d", l.Memory>>20)
	}
	rs := handlers.DefaultReplayBudget.Stats()
	replay := fmt.Sprintf("replay budget %d of %d MB, refused %d new flows and %d growths", rs.Used>>20, rs.Total>>20, rs.OpenRefused-r.replayOpenRefused, rs.GrowRefused-r.replayGrowRefused)
	r.replayOpenRefused, r.replayGrowRefused = rs.OpenRefused, rs.GrowRefused
	return replay + " | " + fmt.Sprintf("process: cpu %.2f cores (%s), throttled %d times for %v, longest wait for a CPU %v, memory %d of %s MB, heap %d MB, goroutines %d, threads allowed %d",
		cores, limit, u.Throttles-old.Throttles, (u.Throttled-old.Throttled).Round(time.Millisecond),
		proclimits.Sched.Delay().Round(time.Millisecond),
		u.Memory>>20, mem, u.Heap>>20, u.Goroutines, u.Procs)
}

func median(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	return s[len(s)/2]
}

func maxOf(v []float64) (m float64) {
	for _, x := range v {
		m = max(m, x)
	}
	return m
}

func minOf(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	m := v[0]
	for _, x := range v[1:] {
		m = min(m, x)
	}
	return m
}

func mean(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	var s float64
	for _, x := range v {
		s += x
	}
	return s / float64(len(v))
}
