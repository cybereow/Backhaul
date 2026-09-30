package dnsx

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/miekg/dns"
)

// ResolverScore is the measured quality of one candidate resolver.
type ResolverScore struct {
	Resolver string        `json:"resolver"`
	OK       int           `json:"ok"`
	N        int           `json:"n"`
	Best     string        `json:"best,omitempty"` // the (record type/transport) pair that scored best
	Mean     time.Duration `json:"mean_ns"`        // mean RTT of answered probes; stalls count, so flaky resolvers rank lower
	Score    float64       `json:"score"`
}

// DiscoverOpts tunes DiscoverResolvers. Zero values take the defaults noted.
type DiscoverOpts struct {
	Domain, Key string
	RRTypes     []uint16      // record types a resolver may carry (default TXT, MX, AAAA, A): a candidate is kept if ANY works
	Timeout     time.Duration // per probe (1.5s)
	Conc        int           // DNS probes in flight, counted per probe (64)
	QPS         int           // DNS probes started per second, counted per probe, so a big candidate list is not a flood (150)
	Samples     int           // stage-2 probes per (record type, transport) pair (3)
	Keep        int           // best resolvers returned (8)
	CachePath   string        // if set: reuse a fresh cache, and write the result
	CacheTTL    time.Duration // (30m)
	Refresh     bool          // ignore the cache (e.g. after an outage with the cached set), but still write it
	Logf        func(format string, args ...any)

	lim *limiter
}

const maxStage2 = 64 // survivors measured in depth (the fastest to answer in stage 1)

func (o *DiscoverOpts) defaults() {
	if o.Timeout <= 0 {
		o.Timeout = 1500 * time.Millisecond
	}
	if len(o.RRTypes) == 0 {
		o.RRTypes = []uint16{dns.TypeTXT, dns.TypeMX, dns.TypeAAAA, dns.TypeA}
	}
	if o.Conc <= 0 {
		o.Conc = 64
	}
	if o.QPS <= 0 {
		o.QPS = 150
	}
	if o.Samples <= 0 {
		o.Samples = 3
	}
	if o.Keep <= 0 {
		o.Keep = 8
	}
	if o.CacheTTL <= 0 {
		o.CacheTTL = 30 * time.Minute
	}
	if o.Logf == nil {
		o.Logf = func(string, ...any) {}
	}
}

// limiter enforces both limits on every individual DNS probe: at most Conc in
// flight and at most QPS started per second.
type limiter struct {
	sem  chan struct{}
	tick *time.Ticker
}

func newLimiter(conc, qps int) *limiter {
	return &limiter{sem: make(chan struct{}, conc), tick: time.NewTicker(time.Second / time.Duration(qps))}
}

func (l *limiter) acquire(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-l.tick.C:
	}
	select {
	case <-ctx.Done():
		return false
	case l.sem <- struct{}{}:
		return true
	}
}

func (l *limiter) release() { <-l.sem }

// probe sends one throwaway diagnostic query (1 byte of data: the responder
// answers it without touching any session) through resolver over transport.
func (o DiscoverOpts) probe(ctx context.Context, resolver string, rrType uint16, transport string) (time.Duration, bool) {
	if !o.lim.acquire(ctx) {
		return 0, false
	}
	defer o.lim.release()
	ctx, cancel := context.WithTimeout(ctx, o.Timeout)
	defer cancel()
	t0 := time.Now()
	_, _, _, _, stage, _ := Exchange(ctx, o.Domain, []byte(o.Key), resolver, rrType, transport, true, 100,
		func(uint64) []byte { return []byte{0} }, o.Timeout)
	return time.Since(t0), stage == StageOK
}

// firstAnswer probes every allowed record type over UDP and TCP in parallel (each
// probe under the shared limiter) and returns how fast the first success came.
func (o DiscoverOpts) firstAnswer(ctx context.Context, resolver string) (time.Duration, bool) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		d  time.Duration
		ok bool
	}
	res := make(chan result, len(o.RRTypes)*2) // buffered: probes cancelled after the first success must not block
	n := 0
	for _, t := range o.RRTypes {
		for _, tr := range []string{"udp", "tcp"} {
			n++
			go func(t uint16, tr string) {
				d, ok := o.probe(ctx, resolver, t, tr)
				res <- result{d, ok}
			}(t, tr)
		}
	}
	for i := 0; i < n; i++ {
		if r := <-res; r.ok {
			return r.d, true
		}
	}
	return 0, false
}

// pairScore measures one (record type, transport) pair with Samples probes. A
// probe that ran into its timeout counts at full length, so stalls lower the score.
func (o DiscoverOpts) pairScore(ctx context.Context, resolver string, rrType uint16, tr string) ResolverScore {
	s := ResolverScore{Resolver: resolver, Best: dns.TypeToString[rrType] + "/" + tr}
	var sumOK, sumAll time.Duration
	for k := 0; k < o.Samples && ctx.Err() == nil; k++ {
		d, ok := o.probe(ctx, resolver, rrType, tr)
		s.N++
		sumAll += d
		if ok {
			s.OK++
			sumOK += d
		}
	}
	if s.OK > 0 {
		s.Mean = sumOK / time.Duration(s.OK)
		rate := float64(s.OK) / float64(s.N)
		s.Score = rate * rate / (sumAll.Seconds()/float64(s.N) + 0.05)
	}
	return s
}

// DiscoverResolvers finds the good resolvers among candidates: a rate-limited
// reachability sweep over everything (every allowed record type, UDP and TCP),
// then every (record type, transport) pair of the fastest survivors is sampled
// and each resolver is ranked by its best pair - a resolver that blocks some
// types but carries one well is still good. The best Keep are returned, best
// first. Nothing about the candidates is assumed: it works for any list
// (built-in, user CIDRs, anything else).
func DiscoverResolvers(ctx context.Context, candidates []string, o DiscoverOpts) []ResolverScore {
	o.defaults()
	inputs := discoveryInputs(o, candidates)
	if o.CachePath != "" && !o.Refresh {
		if cached := loadCache(o.CachePath, o.CacheTTL, inputs); len(cached) > 0 {
			o.Logf("dns discovery: using %d cached resolvers from %s", len(cached), o.CachePath)
			return cached
		}
	}
	o.lim = newLimiter(o.Conc, o.QPS)
	defer o.lim.tick.Stop()

	seen := map[string]bool{}
	var cands []string
	for _, c := range candidates {
		c = withPort(c)
		if !seen[c] { // "1.2.3.4" and "1.2.3.4:53" are one resolver
			seen[c] = true
			cands = append(cands, c)
		}
	}

	// Stage 1: who answers at all, and how fast.
	type alive struct {
		r string
		d time.Duration
	}
	var mu sync.Mutex
	var up []alive
	var wg sync.WaitGroup
	outer := make(chan struct{}, o.Conc) // bounds goroutines; the limiter bounds the probes themselves
	for _, c := range cands {
		if ctx.Err() != nil {
			break
		}
		outer <- struct{}{}
		wg.Add(1)
		go func(c string) {
			defer wg.Done()
			defer func() { <-outer }()
			if d, ok := o.firstAnswer(ctx, c); ok {
				mu.Lock()
				up = append(up, alive{c, d})
				mu.Unlock()
			}
		}(c)
	}
	wg.Wait()
	o.Logf("dns discovery: %d/%d candidates answered", len(up), len(cands))
	sort.Slice(up, func(i, j int) bool { return up[i].d < up[j].d })
	if len(up) > maxStage2 {
		up = up[:maxStage2]
	}

	// Stage 2: sample every pair of each survivor; a resolver's score is its best pair.
	scores := make([]ResolverScore, len(up))
	for i, a := range up {
		wg.Add(1)
		go func(i int, r string) {
			defer wg.Done()
			var pairs sync.WaitGroup
			var pm sync.Mutex
			best := ResolverScore{Resolver: r}
			for _, t := range o.RRTypes {
				for _, tr := range []string{"udp", "tcp"} {
					pairs.Add(1)
					go func(t uint16, tr string) {
						defer pairs.Done()
						ps := o.pairScore(ctx, r, t, tr)
						pm.Lock()
						if ps.Score > best.Score {
							best = ps
						}
						pm.Unlock()
					}(t, tr)
				}
			}
			pairs.Wait()
			scores[i] = best
		}(i, a.r)
	}
	wg.Wait()

	sort.Slice(scores, func(i, j int) bool { return scores[i].Score > scores[j].Score })
	out := scores[:0]
	for _, s := range scores {
		if s.Score > 0 {
			out = append(out, s)
		}
	}
	if len(out) > o.Keep {
		out = out[:o.Keep]
	}
	for _, s := range out {
		o.Logf("dns discovery: %-22s best %-8s ok %d/%d mean %v score %.1f", s.Resolver, s.Best, s.OK, s.N, s.Mean.Round(time.Millisecond), s.Score)
	}
	if o.CachePath != "" && len(out) > 0 {
		saveCache(o.CachePath, inputs, out)
	}
	return out
}

// discoveryInputs identifies what a cache was computed for, so a cache written
// for other settings (domain, key, candidate set, record types) is never reused.
func discoveryInputs(o DiscoverOpts, candidates []string) string {
	cs := make([]string, 0, len(candidates))
	for _, c := range candidates {
		cs = append(cs, withPort(c))
	}
	sort.Strings(cs)
	h := sha256.New()
	fmt.Fprintf(h, "%s|%s|%v|%s", o.Domain, o.Key, o.RRTypes, strings.Join(cs, ","))
	return hex.EncodeToString(h.Sum(nil))
}

func withPort(r string) string {
	r = strings.TrimSpace(r)
	if _, _, err := net.SplitHostPort(r); err != nil {
		return net.JoinHostPort(r, "53")
	}
	return r
}

type cacheFile struct {
	At      time.Time       `json:"at"`
	Inputs  string          `json:"inputs"` // hash of domain, key, candidates, record types
	Results []ResolverScore `json:"results"`
}

func loadCache(path string, ttl time.Duration, inputs string) []ResolverScore {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var c cacheFile
	if json.Unmarshal(b, &c) != nil || time.Since(c.At) > ttl || c.Inputs != inputs {
		return nil
	}
	return c.Results
}

func saveCache(path, inputs string, r []ResolverScore) {
	if b, err := json.MarshalIndent(cacheFile{At: time.Now(), Inputs: inputs, Results: r}, "", " "); err == nil {
		_ = os.WriteFile(path, b, 0o600)
	}
}

// ExpandCIDRs lists the host addresses of the given CIDRs (or plain IPs), at most
// max of them, as candidate resolvers. It refuses a larger range instead of
// truncating it, so nobody scans more than they meant to.
func ExpandCIDRs(cidrs []string, max int) ([]string, error) {
	var out []string
	for _, c := range cidrs {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		if ip := net.ParseIP(c); ip != nil {
			out = append(out, ip.String())
			continue
		}
		ip, n, err := net.ParseCIDR(c)
		if err != nil {
			return nil, fmt.Errorf("bad resolver range %q: %w", c, err)
		}
		ones, bits := n.Mask.Size()
		if bits-ones > 20 || len(out)+(1<<(bits-ones)) > max {
			return nil, fmt.Errorf("resolver range %q is too large (limit %d addresses in total)", c, max)
		}
		for a := ip.Mask(n.Mask); n.Contains(a); a = nextIP(a) {
			out = append(out, a.String())
		}
	}
	if len(out) > max {
		return nil, fmt.Errorf("%d candidate addresses, limit %d", len(out), max)
	}
	return out, nil
}

func nextIP(ip net.IP) net.IP {
	n := append(net.IP(nil), ip...)
	for i := len(n) - 1; i >= 0; i-- {
		n[i]++
		if n[i] != 0 {
			break
		}
	}
	return n
}

// IsAuto reports whether a configured resolver list asks for discovery: empty, or
// containing "auto".
func IsAuto(configured []string) bool {
	if len(configured) == 0 {
		return true
	}
	for _, r := range configured {
		if strings.EqualFold(strings.TrimSpace(r), "auto") {
			return true
		}
	}
	return false
}
