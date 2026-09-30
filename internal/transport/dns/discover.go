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
	Mean     time.Duration `json:"mean_ns"` // mean RTT of answered probes; stalls count, so flaky resolvers rank lower
	Score    float64       `json:"score"`
}

// DiscoverOpts tunes DiscoverResolvers. Zero values take the defaults noted.
type DiscoverOpts struct {
	Domain, Key string
	RRTypes     []uint16      // record types a resolver may carry (default TXT, MX, AAAA, A): a candidate is kept if ANY works
	Timeout     time.Duration // per probe (1.5s)
	Conc        int           // probes in flight (64)
	QPS         int           // stage-1 send rate, so a big candidate list is not a flood (150)
	Samples     int           // stage-2 probes per survivor (6)
	Keep        int           // best resolvers returned (8)
	CachePath   string        // if set: reuse a fresh cache, and write the result
	CacheTTL    time.Duration // (30m)
	Logf        func(format string, args ...any)
}

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
		o.Samples = 6
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

// probe sends one throwaway diagnostic query (1 byte of data: the responder
// answers it without touching any session) through resolver over transport.
func (o DiscoverOpts) probe(ctx context.Context, resolver string, rrType uint16, transport string) (time.Duration, bool) {
	ctx, cancel := context.WithTimeout(ctx, o.Timeout)
	defer cancel()
	t0 := time.Now()
	_, _, _, _, stage, _ := Exchange(ctx, o.Domain, []byte(o.Key), resolver, rrType, transport, true, 100,
		func(uint64) []byte { return []byte{0} }, o.Timeout)
	return time.Since(t0), stage == StageOK
}

// probeAny reports whether resolver carries any allowed record type over UDP or
// TCP. The combinations run in parallel, so a dead resolver costs one timeout.
func (o DiscoverOpts) probeAny(ctx context.Context, resolver string) bool {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	res := make(chan bool, len(o.RRTypes)*2) // buffered: probes cancelled after the first success must not block
	n := 0
	for _, t := range o.RRTypes {
		for _, tr := range []string{"udp", "tcp"} {
			n++
			go func(t uint16, tr string) {
				_, ok := o.probe(ctx, resolver, t, tr)
				res <- ok
			}(t, tr)
		}
	}
	for i := 0; i < n; i++ {
		if <-res {
			return true
		}
	}
	return false
}

// DiscoverResolvers finds the good resolvers among candidates: a rate-limited
// reachability sweep (UDP, then TCP) over everything, then a few repeated probes
// of each survivor to measure success and round-trip time including stalls. The
// best Keep resolvers are returned, best first. Nothing about the candidates is
// assumed: it works for any list (built-in, user CIDRs, anything else).
func DiscoverResolvers(ctx context.Context, candidates []string, o DiscoverOpts) []ResolverScore {
	o.defaults()
	inputs := discoveryInputs(o, candidates)
	if o.CachePath != "" {
		if cached := loadCache(o.CachePath, o.CacheTTL, inputs); len(cached) > 0 {
			o.Logf("dns discovery: using %d cached resolvers from %s", len(cached), o.CachePath)
			return cached
		}
	}

	cands := make([]string, 0, len(candidates))
	for _, c := range candidates {
		cands = append(cands, withPort(c))
	}

	// Stage 1: who answers at all.
	var mu sync.Mutex
	var alive []string
	sem := make(chan struct{}, o.Conc)
	tick := time.NewTicker(time.Second / time.Duration(o.QPS))
	defer tick.Stop()
	var wg sync.WaitGroup
stage1:
	for _, c := range cands {
		select {
		case <-ctx.Done():
			break stage1
		case <-tick.C:
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(c string) {
			defer wg.Done()
			defer func() { <-sem }()
			if o.probeAny(ctx, c) {
				mu.Lock()
				alive = append(alive, c)
				mu.Unlock()
			}
		}(c)
	}
	wg.Wait()
	o.Logf("dns discovery: %d/%d candidates answered", len(alive), len(cands))

	// Stage 2: how good are the ones that answered.
	scores := make([]ResolverScore, len(alive))
	for i, r := range alive {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, r string) {
			defer wg.Done()
			defer func() { <-sem }()
			s := ResolverScore{Resolver: r}
			var sumOK, sumAll time.Duration
			for k := 0; k < o.Samples && ctx.Err() == nil; k++ {
				tr := "udp"
				if k%2 == 1 {
					tr = "tcp"
				}
				d, ok := o.probe(ctx, r, o.RRTypes[(k/2)%len(o.RRTypes)], tr)
				s.N++
				sumAll += d // a probe that ran into its timeout counts at full length
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
			scores[i] = s
		}(i, r)
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
		o.Logf("dns discovery: %-22s ok %d/%d mean %v score %.1f", s.Resolver, s.OK, s.N, s.Mean.Round(time.Millisecond), s.Score)
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
