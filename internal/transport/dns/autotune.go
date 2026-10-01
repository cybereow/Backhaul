package dnsx

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"time"

	"github.com/musix/backhaul/internal/transport/dns/rel"
	"github.com/musix/backhaul/internal/transport/dns/sel"
)

// The client is never reconfigured by hand: nothing about a resolver path is
// assumed, everything is measured. After the quick liveness warm-up, the best
// candidate profiles are probed for what they REALLY carry in each direction
// (how long a QNAME survives, how big a reply survives), and from then on every
// exchange is sized to that profile: upstream segments are cut to its measured
// query size and the reply is requested at its measured reply size. A path that
// drops long names or big replies therefore costs a few probes at startup
// instead of stalling the stream forever.

const (
	autotuneTop        = 16 // profiles measured in depth
	autotunePerResolve = 4  // at most this many of them per resolver
	minUsableUp        = sessionFrame + rel.HeaderLen + minClientMSS
)

// profKey identifies a profile's path independent of its nominal capacity.
type profKey struct {
	resolver  string
	rrType    uint16
	transport string
}

func keyOf(p sel.Profile) profKey { return profKey{p.Resolver, p.RRType, p.Transport} }

// profCaps is what a profile measured as carrying.
type profCaps struct {
	up   int           // query data bytes (sid+flags+packet) that survive
	down int           // reply payload bytes that survive
	rtt  time.Duration // typical probe round trip
}

func (pc profCaps) upMSS() int   { return pc.up - sessionFrame - rel.HeaderLen }
func (pc profCaps) downMSS() int { return pc.down - 1 - rel.HeaderLen }

// probeUp sends diagnostic queries carrying n data bytes and reports whether the
// responder received all n (the QNAME survived the path).
func (c *clientConn) probeUp(ctx context.Context, p sel.Profile, n int) (time.Duration, bool) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	data := make([]byte, n)
	data[4] = FlagProbe // sid 0, probe flag: answered in diagnostic mode, never a session
	for i := sessionFrame; i < n; i++ {
		data[i] = byte(i * 7)
	}
	t0 := time.Now()
	_, qSeen, _, _, stage, _ := exchangeVia(c.pool, ctx, c.domain, c.key, p.Resolver, p.RRType, p.Transport, true, 64,
		func(uint64) []byte { return data }, c.timeout)
	return time.Since(t0), stage == StageOK && int(qSeen) == n
}

// probeDown requests a reply of size bytes and returns how many arrived intact.
func (c *clientConn) probeDown(ctx context.Context, p sel.Profile, size int) (time.Duration, int) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	var nonce uint64
	t0 := time.Now()
	resp, _, _, _, stage, _ := exchangeVia(c.pool, ctx, c.domain, c.key, p.Resolver, p.RRType, p.Transport, true, size,
		func(n uint64) []byte { nonce = n; return []byte{0} }, c.timeout)
	if stage != StageOK || len(resp) == 0 {
		return time.Since(t0), 0
	}
	if !bytes.HasPrefix(patternBytes(nonce, len(resp)), resp) {
		return time.Since(t0), 0 // corrupted
	}
	return time.Since(t0), len(resp)
}

// measureCaps finds how much one profile carries each way. ok is false when the
// profile carries too little to be useful.
func (c *clientConn) measureCaps(ctx context.Context, p sel.Profile, maxQ int) (profCaps, bool) {
	var pc profCaps
	var rtts []time.Duration

	// Upstream: the longest query data that still gets through, trying the full
	// size first and backing off (each size twice, a lost probe is not a limit).
	sizes := []int{maxQ, maxQ * 80 / 100, maxQ * 60 / 100, maxQ * 40 / 100, minUsableUp} // smallest last: the least that is still worth running
	for _, n := range sizes {
		if n < minUsableUp || n > maxQ {
			continue
		}
		for try := 0; try < 2 && ctx.Err() == nil; try++ {
			if d, ok := c.probeUp(ctx, p, n); ok {
				pc.up = n
				rtts = append(rtts, d)
				break
			}
		}
		if pc.up > 0 {
			break
		}
	}
	if pc.up == 0 {
		return pc, false
	}

	// Downstream: grow the requested reply until it stops arriving intact. A
	// shorter reply than requested means the responder's own limit: stop there.
	for _, size := range []int{64, 128, 256, 384, 512, 640, 768, 896, 1024, 1152, 1280} {
		var got int
		for try := 0; try < 2 && ctx.Err() == nil; try++ {
			d, g := c.probeDown(ctx, p, size)
			if g > 0 {
				got = g
				rtts = append(rtts, d)
				break
			}
		}
		if got == 0 {
			break
		}
		pc.down = got
		if got < size {
			break
		}
	}
	if pc.down-1-rel.HeaderLen < minClientMSS {
		return pc, false
	}
	sort.Slice(rtts, func(i, j int) bool { return rtts[i] < rtts[j] })
	pc.rtt = rtts[len(rtts)/2]
	return pc, true
}

// balancedCap scores a profile by the harmonic mean of what one exchange can
// carry each way: traffic goes both ways and the heavy direction is not known in
// advance, so a profile that carries 750 bytes down but 16 up must not outrank
// one that carries a solid 100 and 650.
func balancedCap(up, down int) int {
	if up <= 0 || down <= 0 {
		return 0
	}
	return 2 * up * down / (up + down)
}

// autotune measures the best warm-up survivors and rebuilds the selector over
// them with their real capacities. On any failure to measure anything it leaves
// the selector as it is (the nominal capacities keep working, just unadapted).
func (c *clientConn) autotune(ctx context.Context, selCfg sel.Config, maxQ int) {
	now := time.Now()
	c.mu.Lock()
	type scored struct {
		p sel.Profile
		s float64
	}
	var ranked []scored
	for _, p := range c.allProfiles {
		if s := c.mgr.Score(p, now); s > 0 {
			ranked = append(ranked, scored{p, s})
		}
	}
	c.mu.Unlock()
	sort.Slice(ranked, func(i, j int) bool { return ranked[i].s > ranked[j].s })

	// Measure at least twice as many profiles as the selector shares load over, so
	// a many-worker tunnel spreads over many resolvers.
	top := max(autotuneTop, 2*selCfg.TopK)

	// Cover every resolver first (its best profile), then fill by rank, so the
	// measured set keeps the diversity discovery found.
	perRes := map[string]int{}
	chosen := map[profKey]bool{}
	var cands []sel.Profile
	take := func(p sel.Profile) {
		perRes[p.Resolver]++
		chosen[keyOf(p)] = true
		cands = append(cands, p)
	}
	for _, r := range ranked {
		if perRes[r.p.Resolver] == 0 && len(cands) < top {
			take(r.p)
		}
	}
	for _, r := range ranked {
		if len(cands) >= top {
			break
		}
		if !chosen[keyOf(r.p)] && perRes[r.p.Resolver] < autotunePerResolve {
			take(r.p)
		}
	}
	if len(cands) == 0 {
		return
	}

	type result struct {
		p  sel.Profile
		pc profCaps
		ok bool
	}
	results := make([]result, len(cands))
	var wg sync.WaitGroup
	for i, p := range cands {
		wg.Add(1)
		go func(i int, p sel.Profile) {
			defer wg.Done()
			pc, ok := c.measureCaps(ctx, p, maxQ)
			results[i] = result{p, pc, ok}
		}(i, p)
	}
	wg.Wait()

	caps := map[profKey]profCaps{}
	var tuned []sel.Profile
	for _, r := range results {
		if !r.ok {
			continue
		}
		caps[keyOf(r.p)] = r.pc
		np := r.p
		np.Cap = balancedCap(r.pc.upMSS(), r.pc.downMSS())
		tuned = append(tuned, np)
		if c.logf != nil {
			c.logf("dns autotune: %-22s rr=%-3d %-3s carries up %3dB down %4dB (rtt %v)", r.p.Resolver, r.p.RRType, r.p.Transport, r.pc.upMSS(), r.pc.downMSS(), r.pc.rtt.Round(time.Millisecond))
		}
	}
	if len(tuned) == 0 {
		return
	}
	// Warm survivors that were not measured are NOT put in the selector: with
	// nominal capacities they would outrank the measured profiles, and sel would
	// keep sampling them (dead ones cost a 2s timeout each, which showed up as
	// latency spikes and made the shaper cut workers). They wait as standby: if
	// every measured profile fails, the carrier measures the standby set instead
	// (see failover) rather than reaching its outage limit.
	var standby []sel.Profile
	for _, r := range ranked {
		if !chosen[keyOf(r.p)] {
			standby = append(standby, r.p)
		}
	}
	c.mu.Lock()
	// A control block may have denied a type while this ran: apply the CURRENT deny
	// list to the result (denied profiles wait in denyHeld, never dropped).
	var allowed []sel.Profile
	for _, np := range tuned {
		if typeIn(c.denied, np.RRType) {
			if !containsProfile(c.denyHeld, np) {
				c.denyHeld = append(c.denyHeld, np)
			}
		} else {
			allowed = append(allowed, np)
		}
	}
	if len(allowed) == 0 {
		c.mu.Unlock()
		return // everything measured is denied now: keep the current (allowed) selector
	}
	tuned = allowed
	mgr := sel.New(selCfg, tuned)
	for _, np := range tuned {
		for i := 0; i < warmSamples; i++ { // measured, so proven: seed the selector with what we saw
			mgr.Observe(time.Now(), np, true, caps[keyOf(np)].rtt)
		}
	}
	c.standby = standby
	c.mgr = mgr
	c.caps = caps
	c.profiles = tuned // later denies rebuild the selector from the tuned set
	c.mu.Unlock()
}

// failover runs when every measured profile has failed: it measures the standby
// survivors (and re-tests the old set, which may have recovered) and swaps in
// whatever works. At most one runs at a time, and not more often than every 20s.
func (c *clientConn) failover(maxQ int) {
	c.mu.Lock()
	if c.failingOver || time.Since(c.lastFailover) < 20*time.Second || len(c.standby) == 0 && len(c.profiles) == 0 {
		c.mu.Unlock()
		return
	}
	c.failingOver = true
	var pool []sel.Profile
	for _, p := range append(append([]sel.Profile(nil), c.standby...), c.profiles...) {
		if !typeIn(c.denied, p.RRType) { // the server's deny list outlives a failover
			pool = append(pool, p)
		} else {
			if !containsProfile(c.denyHeld, p) {
				c.denyHeld = append(c.denyHeld, p) // kept so a later lifted deny can bring it back
			}
		}
	}
	c.mu.Unlock()
	go func() {
		defer func() {
			c.mu.Lock()
			c.failingOver, c.lastFailover = false, time.Now()
			c.mu.Unlock()
		}()
		c.mu.Lock()
		c.allProfiles = pool
		c.mgr = sel.New(c.selCfg, pool) // a selector that knows the whole pool, so the warm-up can score it
		c.mu.Unlock()
		c.warmup(pool)
		if c.ctx.Err() == nil {
			c.autotune(c.ctx, c.selCfg, maxQ)
		}
	}()
}

func containsProfile(l []sel.Profile, p sel.Profile) bool {
	for _, x := range l {
		if keyOf(x) == keyOf(p) {
			return true
		}
	}
	return false
}
