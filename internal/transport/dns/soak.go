package dnsx

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/miekg/dns"
	"github.com/sirupsen/logrus"
)

// SoakStream is one sustained carrier under test: a fixed (resolver, record
// type, transport, response size). The soak drives many sequential round-trips
// on it so we learn behaviour under continuous load, not from a single sample.
type SoakStream struct {
	Resolver  string
	RRType    uint16
	Transport string
	RespSize  int
}

// SoakParams configures the sustained bidirectional path test. Load is ramped by
// growing the in-flight window per stream from 1 up to MaxInflight over Duration
// (a single synchronous loop is capped at ~1/RTT; concurrency is the load knob).
type SoakParams struct {
	Domain      string
	Key         string
	QueryBudget int
	EDNS        bool
	Timeout     time.Duration
	Duration    time.Duration
	Streams     []SoakStream
	MaxInflight int           // peak concurrent in-flight per stream (ramped 1..N)
	NoRamp      bool          // start all MaxInflight workers immediately (fixed load, for a concurrency sweep)
	SampleEvery time.Duration // progress log cadence
}

// SoakResult is one stream's aggregate over the whole run. Goodput counts only
// useful bytes actually confirmed each way (up: bytes the responder received;
// down: leading bytes returned byte-exact), so corruption/truncation is excluded.
type SoakResult struct {
	Resolver  string `json:"resolver"`
	RRType    string `json:"rr_type"`
	Transport string `json:"transport"`
	RespSize  int    `json:"resp_size"`

	Seconds     float64 `json:"seconds"`
	Exchanges   int     `json:"exchanges"`
	Successes   int     `json:"successes"`
	SuccessRate float64 `json:"success_rate"`

	UpBytes        int64   `json:"up_bytes"`
	DownBytes      int64   `json:"down_bytes"`
	UpGoodputBps   float64 `json:"up_goodput_bps"`
	DownGoodputBps float64 `json:"down_goodput_bps"`

	RTTp50Ms int64 `json:"rtt_p50_ms"`
	RTTp90Ms int64 `json:"rtt_p90_ms"`
	RTTmaxMs int64 `json:"rtt_max_ms"`

	LongestStallMs int64 `json:"longest_stall_ms"` // max wall-clock gap with no success
	MaxConsecFails int   `json:"max_consec_fails"`
	Recovered      bool  `json:"recovered"` // latest exchange was a success
	IntegrityFails int   `json:"integrity_fails"`

	Errs map[string]int `json:"errs,omitempty"`
}

// event is one completed exchange, kept for post-hoc streak/stall analysis.
type event struct {
	at   time.Time
	ok   bool
	rtt  int64
	up   int
	down int
	bad  bool // integrity failure (down corrupted / up short of request)
	err  string
}

type streamStat struct {
	mu     sync.Mutex
	events []event
}

func (s *streamStat) add(e event) {
	s.mu.Lock()
	s.events = append(s.events, e)
	s.mu.Unlock()
}

// RunSoak runs every stream for Duration and returns one aggregate per stream.
func RunSoak(ctx context.Context, params SoakParams, logger *logrus.Logger) []SoakResult {
	pr := NewProber(ProberParams{
		Domain:      params.Domain,
		Key:         params.Key,
		QueryBudget: params.QueryBudget,
		Timeout:     params.Timeout,
	}, logger)

	inflight := params.MaxInflight
	if inflight < 1 {
		inflight = 1
	}

	ctx, cancel := context.WithTimeout(ctx, params.Duration)
	defer cancel()
	start := time.Now()

	stats := make([]*streamStat, len(params.Streams))
	var wg sync.WaitGroup
	for i, s := range params.Streams {
		stats[i] = &streamStat{}
		wg.Add(1)
		go func(st *streamStat, s SoakStream) {
			defer wg.Done()
			runStream(ctx, pr, params, s, st)
		}(stats[i], s)
	}

	if params.SampleEvery > 0 {
		go progress(ctx, params, stats, start, logger)
	}
	wg.Wait()

	out := make([]SoakResult, len(params.Streams))
	for i, s := range params.Streams {
		out[i] = summarize(s, stats[i], time.Since(start).Seconds())
	}
	return out
}

// runStream ramps the in-flight window from 1 to params.MaxInflight over the run
// and keeps that many workers looping round-trips until the deadline.
func runStream(ctx context.Context, pr *Prober, params SoakParams, s SoakStream, st *streamStat) {
	inflight := params.MaxInflight
	if inflight < 1 {
		inflight = 1
	}
	step := params.Duration / time.Duration(inflight)

	var wg sync.WaitGroup
	launch := func() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				a := pr.probe(ctx, s.Resolver, s.RRType, s.Transport, params.EDNS, s.RespSize)
				// The conn read deadline derives from ctx's and can fire a hair before
				// ctx.Err() turns non-nil, so also compare against the deadline itself.
				if dl, ok := ctx.Deadline(); a.stage != StageOK && (ctx.Err() != nil || (ok && !time.Now().Before(dl))) {
					return // deadline hit mid-flight; don't record a phantom failure
				}
				bad := a.stage == StageOK && a.err != ""
				st.add(event{
					at:   time.Now(),
					ok:   a.stage == StageOK && a.err == "",
					rtt:  a.rttMs,
					up:   a.qBytes,
					down: a.respBytes,
					bad:  bad,
					err:  a.err,
				})
				if a.stage != StageOK || a.err != "" {
					// Back off after a failure so a fast-failing path (TCP reset, refused)
					// isn't hot-spun into thousands of phantom exchanges.
					select {
					case <-ctx.Done():
						return
					case <-time.After(250 * time.Millisecond):
					}
				}
			}
		}()
	}

	launch() // first worker immediately
	for w := 1; w < inflight; w++ {
		if params.NoRamp {
			launch()
			continue
		}
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case <-time.After(step):
			launch()
		}
	}
	wg.Wait()
}

// progress logs cumulative per-stream success + goodput so a detached run is
// observably alive.
func progress(ctx context.Context, params SoakParams, stats []*streamStat, start time.Time, logger *logrus.Logger) {
	t := time.NewTicker(params.SampleEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			el := time.Since(start).Seconds()
			for i, s := range params.Streams {
				r := summarize(s, stats[i], el)
				logger.Infof("[soak %ds] %s/%s/%s sz%d: %d/%d ok, up %.0f B/s down %.0f B/s, p90 %dms, stall %dms",
					int(el), stripPort(s.Resolver), dns.TypeToString[s.RRType], s.Transport, s.RespSize,
					r.Successes, r.Exchanges, r.UpGoodputBps, r.DownGoodputBps, r.RTTp90Ms, r.LongestStallMs)
			}
		}
	}
}

// summarize folds a stream's events into a SoakResult.
func summarize(s SoakStream, st *streamStat, seconds float64) SoakResult {
	st.mu.Lock()
	evs := append([]event(nil), st.events...)
	st.mu.Unlock()
	sort.Slice(evs, func(i, j int) bool { return evs[i].at.Before(evs[j].at) })

	r := SoakResult{
		Resolver:  s.Resolver,
		RRType:    dns.TypeToString[s.RRType],
		Transport: s.Transport,
		RespSize:  s.RespSize,
		Seconds:   seconds,
		Errs:      map[string]int{},
	}
	var rtts []int64
	var lastOK time.Time
	haveOK := false
	consec := 0
	for _, e := range evs {
		r.Exchanges++
		if e.bad {
			r.IntegrityFails++
		}
		if e.ok {
			r.Successes++
			r.UpBytes += int64(e.up)
			r.DownBytes += int64(e.down)
			rtts = append(rtts, e.rtt)
			if haveOK {
				if gap := e.at.Sub(lastOK).Milliseconds(); gap > r.LongestStallMs {
					r.LongestStallMs = gap
				}
			}
			lastOK, haveOK = e.at, true
			consec = 0
			r.Recovered = true
		} else {
			consec++
			if consec > r.MaxConsecFails {
				r.MaxConsecFails = consec
			}
			r.Recovered = false
			if e.err != "" {
				r.Errs[e.err]++
			}
		}
	}
	if r.Exchanges > 0 {
		r.SuccessRate = float64(r.Successes) / float64(r.Exchanges)
	}
	if seconds > 0 {
		r.UpGoodputBps = float64(r.UpBytes) / seconds
		r.DownGoodputBps = float64(r.DownBytes) / seconds
	}
	if len(rtts) > 0 {
		_, r.RTTp50Ms, r.RTTp90Ms = percentiles(rtts) // returns min, p50, p90
		for _, v := range rtts {
			if v > r.RTTmaxMs {
				r.RTTmaxMs = v
			}
		}
	}
	if len(r.Errs) == 0 {
		r.Errs = nil
	}
	return r
}
