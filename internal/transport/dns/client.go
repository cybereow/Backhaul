package dnsx

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/miekg/dns"
	"github.com/musix/backhaul/internal/transport/dns/rel"
	"github.com/musix/backhaul/internal/transport/dns/sel"
)

const (
	outageLimit  = 90 * time.Second // sustained total failure after which the carrier is torn down
	dialAttempts = 3                // startup exchanges before Dial gives up
	closeGrace   = 3 * time.Second  // max wait for the FIN exchange in Close
	rxWindow     = time.Second      // how long receive activity keeps helper workers polling
	minClientMSS = 16               // below this the tunnel is not worth running
	pollFast     = 20 * time.Millisecond
	pollIdle     = 500 * time.Millisecond
)

// DialParams configures the client dialer.
type DialParams struct {
	Domain   string
	Key      string
	Profiles []sel.Profile
	Timeout  time.Duration
	Rel      rel.Config
	// Workers is how many exchanges may be in flight at once (default 1). More
	// workers raise throughput roughly linearly until the resolvers push back;
	// extra workers only run while there is data to move.
	Workers int
	// NoAutotune skips the capacity measurement of the best profiles (tests).
	NoAutotune bool
	// NoHedge turns off hedging: an exchange that has not answered after a few
	// typical round trips otherwise starts one extra exchange (up to Workers
	// extra in flight) so a resolver that occasionally stalls ~2s does not stall
	// the stream.
	NoHedge bool
	// OutageLimit is how long every exchange may fail after the tunnel was up
	// before the carrier gives up and reports an error (default 90s).
	OutageLimit time.Duration
	// Sel tunes profile selection (zero values take the sel defaults). Raising
	// TopK spreads the workers over more resolver/record-type profiles.
	Sel sel.Config
	// Logf, if set, receives a periodic per-profile summary (debug aid).
	Logf func(format string, args ...any)
}

// Dial returns a reliable net.Conn running over DNS.
func Dial(ctx context.Context, p DialParams) (net.Conn, error) {
	if len(p.Profiles) == 0 {
		return nil, errors.New("dnsx: no profiles provided")
	}

	if p.Timeout <= 0 {
		p.Timeout = 2 * time.Second
	}
	domain := dns.Fqdn(p.Domain)
	isn := uint32(0)

	maxQ := maxQueryData(domain)
	if maxQ <= 0 {
		return nil, fmt.Errorf("dnsx: domain %q is too long to carry payload", domain)
	}
	// maxQueryData already excludes the query header and MAC.
	mss := maxQ - sessionFrame - rel.HeaderLen
	if mss < minClientMSS {
		return nil, fmt.Errorf("dnsx: domain %q leaves only %d bytes per query, need at least %d", domain, mss, minClientMSS)
	}

	rcfg := p.Rel
	rcfg.MSS = mss
	rcfg.ISN = isn
	ep := rel.New(rcfg)
	mgr := sel.New(p.Sel, p.Profiles)
	sid := randNonce() // Use 32-bit of it
	sid32 := uint32(sid)

	c := &clientConn{
		ep:      ep,
		mgr:     mgr,
		domain:  domain,
		key:     []byte(p.Key),
		timeout: p.Timeout,
		sid:     sid32,
		closed:  make(chan struct{}),
		finDone: make(chan struct{}),
	}
	c.cond = sync.NewCond(&c.mu)
	c.logf = p.Logf
	c.pool = newConnPool()
	c.noHedgeCfg = p.NoHedge
	c.hedge.Store(!p.NoHedge)
	c.selCfg = p.Sel
	c.profiles = p.Profiles
	c.outage = p.OutageLimit
	if c.outage <= 0 {
		c.outage = outageLimit
	}
	c.maxHedge = int32(max(p.Workers, 1))
	c.stats = map[sel.Profile]*profStat{}

	c.ctx, c.cancel = context.WithCancel(ctx)
	// Wake any Read/Write parked on the cond when the context ends (parent
	// cancellation included), or they would sleep until their deadline.
	go func() {
		<-c.ctx.Done()
		c.mu.Lock()
		c.cond.Broadcast()
		c.mu.Unlock()
	}()

	c.allProfiles = p.Profiles
	c.maxQ = maxQ
	c.baseMSS = mss
	c.warmup(p.Profiles)
	if !p.NoAutotune {
		c.autotune(c.ctx, p.Sel, maxQ)
	}
	// The session must really come up: a few attempts (one lost query is normal),
	// then give up instead of returning a connection no server ever answered.
	for try := 0; try < dialAttempts && c.ctx.Err() == nil; try++ {
		c.exchange(false)
		c.mu.Lock()
		up, err := c.established, c.err
		c.mu.Unlock()
		if err != nil {
			c.cancel()
			return nil, err
		}
		if up {
			break
		}
	}
	c.mu.Lock()
	up := c.established
	c.mu.Unlock()
	if !up {
		c.cancel()
		return nil, errors.New("dnsx: no server answered through any resolver")
	}

	n := p.Workers
	if n < 1 {
		n = 1
	}
	c.target.Store(int32(n))
	for i := 0; i < n; i++ {
		c.wg.Add(1)
		go c.loop(i)
	}
	if n > minWorkers {
		c.wg.Add(1)
		go c.shape(n)
	}

	return c, nil
}

const (
	warmSamples     = 4   // samples per profile before first use (>= sel MinWeight, == FailStreak)
	warmConcurrency = 48  // profiles probed in parallel (spread over resolvers, see interleave)
	livenessReply   = 100 // reply bytes requested by warm-up probes
	warmFailStop    = 2   // consecutive failures after which the rest count as failed without waiting
)

// warmup measures every profile in parallel with throwaway probe queries (1 byte
// of data: the responder answers those in diagnostic mode without touching any
// session) so sel starts with real numbers instead of exploring one slow,
// possibly dead profile per exchange. A profile that fails warmFailStop times in
// a row is marked failed at once.
func (c *clientConn) warmup(profiles []sel.Profile) {
	profiles = interleave(profiles)
	sem := make(chan struct{}, warmConcurrency)
	var wg sync.WaitGroup
	for _, p := range profiles {
		wg.Add(1)
		sem <- struct{}{}
		go func(p sel.Profile) {
			defer wg.Done()
			defer func() { <-sem }()
			// Liveness only: a small reply. What the profile really carries is measured
			// afterwards by autotune; asking for the nominal maximum here would declare
			// a path dead that simply cannot carry big replies.
			respSize := livenessReply
			fails := 0
			for i := 0; i < warmSamples; i++ {
				if c.ctx.Err() != nil {
					return
				}
				ok := false
				if fails < warmFailStop {
					ctx, cancel := context.WithTimeout(c.ctx, c.timeout)
					if !c.acquireProbe(ctx) {
						cancel()
						return
					}
					t0 := time.Now()
					_, _, _, _, stage, _ := Exchange(ctx, c.domain, c.key, p.Resolver, p.RRType, p.Transport, true, respSize, func(uint64) []byte { return []byte{0} }, c.timeout)
					cancel()
					c.slots.Add(-1)
					ok = stage == StageOK
					c.mu.Lock()
					c.mgr.Observe(time.Now(), p, ok, time.Since(t0))
					c.record(time.Now(), p, ok, time.Since(t0))
					c.mu.Unlock()
				} else {
					c.mu.Lock()
					c.mgr.Observe(time.Now(), p, false, 0)
					c.mu.Unlock()
				}
				if ok {
					fails = 0
				} else {
					fails++
				}
			}
		}(p)
	}
	wg.Wait()
}

// carrierRespSize is the reply payload a carrier exchange asks for on a profile
// with codec capacity cap: the largest reply the carrier will actually send
// (flags + rel header + one capped segment), never the codec's nominal capacity,
// which can exceed what a 1232-byte EDNS message holds once the DNS framing is
// added.
func carrierRespSize(cap int) int {
	n := cap - envelopeOverhead
	if max := 1 + rel.HeaderLen + maxSegment; n > max {
		n = max
	}
	if n < 0 {
		n = 0
	}
	return n
}

// interleave reorders profiles round-robin by resolver so the parallel warm-up
// hits many resolvers at once instead of hammering one (rate limits would read
// as failures).
func interleave(profiles []sel.Profile) []sel.Profile {
	var order []string
	by := map[string][]sel.Profile{}
	for _, p := range profiles {
		if _, ok := by[p.Resolver]; !ok {
			order = append(order, p.Resolver)
		}
		by[p.Resolver] = append(by[p.Resolver], p)
	}
	out := make([]sel.Profile, 0, len(profiles))
	for i := 0; len(out) < len(profiles); i++ {
		for _, r := range order {
			if i < len(by[r]) {
				out = append(out, by[r][i])
			}
		}
	}
	return out
}

// profStat counts exchanges per profile for the Logf summary.
type profStat struct {
	ok, fail int
	rtt      time.Duration
}

type clientConn struct {
	ep      *rel.Endpoint
	mgr     *sel.Manager
	domain  string
	key     []byte
	timeout time.Duration
	sid     uint32

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu   sync.Mutex // protects reads/writes to ep
	cond *sync.Cond

	closed  chan struct{}
	finDone chan struct{} // closed once a FIN has been delivered
	finOnce sync.Once
	err     error

	readDeadline  time.Time
	writeDeadline time.Time

	localAddr  net.Addr
	remoteAddr net.Addr

	lastRX       time.Time // when data (or an advancing ACK) last arrived
	progressAt   time.Time // last time the stream moved (or had nothing to move): outage detection
	rxProgress   time.Time // last time downstream bytes arrived or the server had none outstanding
	rxBehind     bool      // the server's last packet pointed past what we have received
	logf         func(string, ...any)
	stats        map[sel.Profile]*profStat
	pool         *connPool
	allProfiles  []sel.Profile        // every candidate profile (autotune ranks these)
	caps         map[profKey]profCaps // measured per-profile capacities (nil: use nominal)
	baseMSS      int                  // client segment size for profiles without measured caps
	hedge        atomic.Bool          // hedging enabled (the server's control block can turn it off)
	noHedgeCfg   bool                 // hedging disabled by the caller
	maxHedge     int32                // cap on extra in-flight exchanges
	hedges       atomic.Int32         // extra exchanges currently in flight
	slots        atomic.Int32         // exchanges + hedges in flight; bounded by the server's cap (takeSlot)
	target       atomic.Int32         // workers currently allowed to run (shaping)
	ctrlCap      atomic.Int32         // server's MaxWorkers (0: none)
	idlePoll     atomic.Int64         // server's idle polling interval in ns (0: default)
	selCfg       sel.Config
	profiles     []sel.Profile // current candidate set (a deny rebuilds the selector from it)
	denied       []uint16      // record types the server told us not to use
	denyHeld     []sel.Profile // profiles of denied types, set aside (not dropped) until the deny is lifted
	lastCtrlReq  time.Time     // last control block actually received
	lastCtrlTry  time.Time     // last time we asked for one
	standby      []sel.Profile // warm survivors autotune did not measure (failover pool)
	failingOver  bool
	lastFailover time.Time
	maxQ         int
	winOK        atomic.Int32 // exchange outcomes in the current shaping window
	winFail      atomic.Int32
	psrtt        map[sel.Profile]time.Duration // smoothed RTT of answered exchanges per profile (capped): sets its hedge delay
	nextLog      time.Time
	lastOK       time.Time // last successful exchange (outage detection)
	outage       time.Duration
	established  bool // first reply seen: stop sending SYN
}

// loop is one exchange worker. The primary keeps polling while idle; helpers
// only run while there is data to move.
func (c *clientConn) loop(idx int) {
	defer c.wg.Done()
	primary := idx == 0

	timer := time.NewTimer(0) // Start immediately
	defer timer.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-timer.C:
			if !primary {
				if int32(idx) >= c.effTarget() {
					timer.Reset(pollFast) // shaped out: the resolvers are being asked to carry less
					continue
				}
				c.mu.Lock()
				busy := c.ep.Pending() > 0 || c.recentRX() || c.ep.PeerWnd() == 0
				c.mu.Unlock()
				if !busy {
					timer.Reset(pollFast)
					continue
				}
			}
			if !c.takeSlot() { // the server's cap is full
				timer.Reset(pollFast)
				continue
			}
			c.exchange(true)

			c.mu.Lock()
			pending := c.ep.Pending()
			recent := c.recentRX()
			peerWnd := c.ep.PeerWnd()
			c.mu.Unlock()

			if recent || pending > 0 || peerWnd == 0 {
				timer.Reset(1 * time.Microsecond)
			} else {
				timer.Reset(c.idleInterval())
			}
		}
	}
}

// exchange runs one poll. With slotHeld the caller reserved a slot (takeSlot) and
// the primary network attempt releases it when that attempt really ends, not when
// exchange returns: a hedge can answer first while the primary is still in flight.
func (c *clientConn) exchange(slotHeld bool) {
	now := time.Now()

	c.mu.Lock()
	prof := c.mgr.Pick(now)

	respSize := carrierRespSize(prof.Cap)
	segMSS := c.baseMSS
	if pc, ok := c.caps[keyOf(prof)]; ok {
		// What this path was measured to carry: cut the upstream segment to its
		// query size and ask for the reply size it carried intact.
		respSize, segMSS = pc.down, pc.upMSS()
	}
	c.ep.SetMSS(segMSS)

	pk := c.ep.Next(now)
	isFIN := false // we'll set this if the connection is closed and we have no more pending data
	select {
	case <-c.closed:
		if c.ep.Pending() == 0 {
			isFIN = true
		}
	default:
	}
	c.mu.Unlock()

	flags := byte(0)
	c.mu.Lock()
	if !c.established {
		flags |= FlagSYN
	}
	if now.Sub(c.lastCtrlReq) > controlPullEvery && now.Sub(c.lastCtrlTry) > 2*time.Second {
		flags |= FlagCtrlReq // asked again until a block arrives: a lost first exchange must not cost 30 s
		c.lastCtrlTry = now
	}
	c.mu.Unlock()
	if isFIN {
		flags |= FlagFIN
	}

	qDataFunc := func(nonce uint64) []byte {
		pkData := pk.Marshal()
		buf := make([]byte, sessionFrame+len(pkData))
		binary.BigEndian.PutUint32(buf[0:], c.sid)
		buf[4] = flags
		copy(buf[sessionFrame:], pkData)
		return buf
	}

	// A lost query/response is just a lost packet (rel retransmits), but a slow
	// one is not: cutting it off early throws away a reply that was on its way.
	to := c.timeout

	t0 := time.Now()
	type netResult struct {
		prof  sel.Profile
		data  []byte
		rttMs int64
		stage Stage
	}
	resCh := make(chan netResult, 2)
	// Every attempt has its own full timeout (a hedge started late must not inherit
	// the primary's nearly spent deadline) and outlives this call: cancelling on
	// return would abort the slower copy and score a healthy profile as failing.
	send := func(p sel.Profile, rs int) {
		ctx, cancel := context.WithTimeout(c.ctx, to)
		defer cancel()
		d, _, _, ms, st, _ := exchangeVia(c.pool, ctx, c.domain, c.key, p.Resolver, p.RRType, p.Transport, true, rs, qDataFunc, to)
		resCh <- netResult{p, d, ms, st}
	}
	go func() {
		send(prof, respSize)
		if slotHeld {
			c.slots.Add(-1)
		}
	}()

	dup := false
	var res netResult
	if c.hedge.Load() {
		select {
		case res = <-resCh:
		case <-time.After(c.hedgeDelay(prof)):
			// Still unanswered after a few typical round trips: a resolver is sitting
			// on the query. Send the SAME packet again through another profile (rel
			// ignores whichever copy arrives second), so a stall costs one hedge
			// delay instead of a full stall plus a retransmission timeout.
			if c.hedgeWorthIt() && c.acquireHedge() {
				dup = true
				p2, rs2 := c.pickOther(prof, segMSS)
				go func() {
					defer c.hedges.Add(-1)
					defer c.slots.Add(-1)
					send(p2, rs2)
				}()
			}
			res = <-resCh
		}
	} else {
		res = <-resCh
	}
	c.process(res.prof, res.stage, res.data, res.rttMs, isFIN, t0)
	if dup {
		// The slower copy still carries a reply (data, an ACK); fold it in too.
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			select {
			case r := <-resCh:
				c.process(r.prof, r.stage, r.data, r.rttMs, isFIN, t0) // a FIN delivered by the slower copy still counts (finOnce dedupes)
			case <-c.ctx.Done():
			}
		}()
	}
}

// process folds one exchange's outcome into the connection: selector stats, the
// reply's REL packet, control block, flags and the outage checks. Called once per
// reply, including the replies of hedged duplicates.
func (c *clientConn) process(prof sel.Profile, stage Stage, respData []byte, rttMs int64, isFIN bool, t0 time.Time) {
	now := time.Now()
	c.mu.Lock()
	if stage == StageOK {
		c.winOK.Add(1)
		c.mgr.Observe(now, prof, true, time.Duration(rttMs)*time.Millisecond)
		if !c.established {
			c.progressAt = now
			c.rxProgress = now
		}
		c.established = true
		c.lastOK = now
		// Track typical (not stalled) round trips: cap so a 2s stall cannot push
		// the hedge delay up to where hedging stops helping.
		rt := min(time.Duration(rttMs)*time.Millisecond, hedgeMaxDelay)
		if c.psrtt == nil {
			c.psrtt = map[sel.Profile]time.Duration{}
		}
		if old := c.psrtt[prof]; old == 0 {
			c.psrtt[prof] = rt
		} else {
			c.psrtt[prof] = (7*old + rt) / 8
		}
		if isFIN {
			c.finOnce.Do(func() { close(c.finDone) })
		}

		if len(respData) >= 1 {
			flags := respData[0]
			if flags&FlagRST != 0 {
				c.err = errors.New("connection reset by peer")
				c.cancel()
				c.notifyAll()
			} else {
				if flags&FlagFIN != 0 {
					if c.err == nil {
						c.err = io.EOF
					}
					c.notifyAll()
				}
				body := respData[1:]
				if flags&FlagCtrl != 0 && len(body) >= 1 && len(body) >= 1+int(body[0]) {
					n := int(body[0])
					if ctl, err := ParseControl(body[1 : 1+n]); err == nil {
						c.mu.Unlock() // applyControl takes mu itself
						c.applyControl(ctl)
						c.mu.Lock()
					}
					body = body[1+n:]
				}
				p, err := rel.Unmarshal(body)
				if err == nil {
					pendingBefore := c.ep.Pending()
					rn0 := c.ep.RcvNxt()
					c.ep.Recv(now, p)
					// An empty packet carries the server's send position: when it is
					// beyond what we received, bytes are outstanding downstream. They
					// must arrive within the outage limit, or the path drops data
					// replies while still answering empty polls.
					rn1 := c.ep.RcvNxt()
					c.rxBehind = rel.SeqAfter(p.Seq, rn1)
					if rn1 != rn0 || !c.rxBehind {
						c.rxProgress = now
					}
					// Only new data or an ACK that actually advanced counts as
					// activity; the cumulative ACK alone is nonzero forever.
					if len(p.Data) > 0 || c.ep.Pending() < pendingBefore {
						c.lastRX = now
						c.progressAt = now
					}
					if c.ep.Pending() == 0 {
						c.progressAt = now // nothing waiting: idle is healthy
					}
					c.notifyAll()
				}
			}
		}
	} else {
		c.winFail.Add(1)
		if len(c.mgr.Active()) == 0 {
			defer c.failover(c.maxQ) // everything measured is failing: look for another way through
		}
		// No reply at all counts as a failure too: otherwise a profile the path
		// silently drops would never be disabled and would eat every Nth exchange.
		c.mgr.Observe(now, prof, false, 0)
	}
	c.record(now, prof, stage == StageOK, now.Sub(t0))
	if c.logf != nil && (stage != StageOK || now.Sub(t0) > 1500*time.Millisecond) {
		c.logf("dns slow/failed exchange: %s rr=%d %s stage=%v took %v", prof.Resolver, prof.RRType, prof.Transport, stage, now.Sub(t0).Round(time.Millisecond))
	}

	// Every resolver/profile has been failing for outageLimit: the tunnel is dead
	// for practical purposes. End it so the owner can reconnect and re-discover
	// resolvers instead of queueing streams onto it.
	// A valid reply is not enough: a middlebox that answers short empty polls but
	// drops the longer data-carrying queries keeps every poll "OK" while the data
	// never gets through. So the stream must also make progress while data waits.
	if c.established && c.err == nil {
		dead := stage != StageOK && now.Sub(c.lastOK) > c.outage
		if c.ep.PeerWnd() == 0 {
			c.progressAt = now // the peer is applying backpressure: waiting is legitimate, not a stall
		}
		stuck := c.ep.Pending() > 0 && now.Sub(c.progressAt) > c.outage
		downStuck := c.rxBehind && now.Sub(c.rxProgress) > c.outage
		if dead || stuck || downStuck {
			why := "no resolver answered"
			if !dead {
				why = "no data got through"
			}
			c.err = fmt.Errorf("dnsx: %s for %v", why, c.outage)
			c.cancel()
			c.notifyAll()
		}
	}

	c.mu.Unlock()
}

const (
	hedgeMinDelay = 400 * time.Millisecond
	hedgeMaxDelay = 1500 * time.Millisecond
)

// hedgeDelay is how long an exchange may run before a helper is started: a few
// typical round trips, clamped.
// The delay follows the profile being used: a TCP profile that normally answers in
// 600ms must not be "stalled" at 300ms, or every exchange would be duplicated and
// the doubled load would throttle the very path hedging is meant to protect.
func (c *clientConn) hedgeDelay(p sel.Profile) time.Duration {
	c.mu.Lock()
	d := 3 * c.psrtt[p]
	c.mu.Unlock()
	if d == 0 {
		d = 3 * hedgeMinDelay // nothing measured yet: wait longer
	}
	return min(max(d, hedgeMinDelay), hedgeMaxDelay)
}

// hedgeWorthIt: duplicating a stalled poll that has nothing to carry or wait for
// only adds load; hedge when there is data to move or a reply being waited for.
func (c *clientConn) hedgeWorthIt() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ep.Pending() > 0 || c.recentRX() || !c.established
}

// acquireHedge takes one unit of the hedge budget (bounded by the current shaped
// worker target, so hedges never pile load on a throttled path) when hedging is
// allowed; the caller releases it with c.hedges.Add(-1).
func (c *clientConn) acquireHedge() bool {
	if c.ctx.Err() != nil {
		return false
	}
	limit := min(c.maxHedge, max(c.effTarget(), 1))
	if c.hedges.Add(1) > limit || !c.takeSlot() {
		c.hedges.Add(-1)
		return false
	}
	return true
}

// acquireProbe waits for a slot for a warm-up/measurement probe, so probing during
// a failover obeys the server's worker cap like any other exchange. The caller
// releases it with c.slots.Add(-1). False when ctx ended first.
func (c *clientConn) acquireProbe(ctx context.Context) bool {
	for !c.takeSlot() {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(20 * time.Millisecond):
		}
	}
	return true
}

// takeSlot reserves one in-flight exchange (worker or hedge) against the server's
// worker cap. One atomic counter serves both kinds, so they cannot race past it.
// The caller releases it with c.slots.Add(-1).
func (c *clientConn) takeSlot() bool {
	n := c.slots.Add(1)
	if cp := c.ctrlCap.Load(); cp > 0 && n > cp {
		c.slots.Add(-1)
		return false
	}
	return true
}

// pickOther returns a profile (and its reply size) for a duplicate, preferring a
// different resolver than the original's. The duplicate is the same packet, so the
// profile must carry segMSS upstream; if none does, it falls back to orig.
func (c *clientConn) pickOther(orig sel.Profile, segMSS int) (sel.Profile, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fits := func(p sel.Profile) bool {
		if pc, ok := c.caps[keyOf(p)]; ok {
			return pc.upMSS() >= segMSS
		}
		return c.baseMSS >= segMSS
	}
	p := orig
	for i := 0; i < 4; i++ {
		if q := c.mgr.Pick(time.Now()); fits(q) && (q.Resolver != orig.Resolver || i == 3) {
			p = q
			break
		}
	}
	rs := carrierRespSize(p.Cap)
	if pc, ok := c.caps[keyOf(p)]; ok {
		rs = pc.down
	}
	return p, rs
}

// recentRX reports whether data arrived within rxWindow. It is a time window, not
// a flag one worker consumes: helper workers sleep on their own timers and would
// otherwise never see receive activity, so downloads ran through one poller.
// mu must be held.
func (c *clientConn) recentRX() bool {
	return !c.lastRX.IsZero() && time.Since(c.lastRX) < rxWindow
}

// record updates the per-profile counters and, at most every 5s, logs them.
// mu is held.
func (c *clientConn) record(now time.Time, prof sel.Profile, ok bool, rtt time.Duration) {
	if c.logf == nil {
		return
	}
	st := c.stats[prof]
	if st == nil {
		st = &profStat{}
		c.stats[prof] = st
	}
	if ok {
		st.ok++
		st.rtt = rtt
	} else {
		st.fail++
	}
	if now.Before(c.nextLog) {
		return
	}
	c.nextLog = now.Add(5 * time.Second)
	for p, s := range c.stats {
		c.logf("dns profile %s rr=%d %s: ok=%d fail=%d lastrtt=%v score=%.0f", p.Resolver, p.RRType, p.Transport, s.ok, s.fail, s.rtt, c.mgr.Score(p, now))
	}
	c.logf("dns carrier: pending=%d sent=%d retx=%d peerWnd=%d active=%d", c.ep.Pending(), c.ep.Sent, c.ep.Retx, c.ep.PeerWnd(), len(c.mgr.Active()))
}

func (c *clientConn) notifyAll() {
	c.cond.Broadcast()
}

func (c *clientConn) Read(b []byte) (n int, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for {
		if n := c.ep.Read(b); n > 0 {
			c.cond.Broadcast()
			return n, nil
		}
		if c.err != nil {
			return 0, c.err
		}

		select {
		case <-c.closed:
			return 0, io.ErrClosedPipe
		default:
		}
		select {
		case <-c.ctx.Done():
			return 0, c.ctx.Err()
		default:
		}

		if !c.readDeadline.IsZero() && time.Now().After(c.readDeadline) {
			return 0, timeoutError{}
		}

		var timer *time.Timer
		if !c.readDeadline.IsZero() {
			timer = time.AfterFunc(time.Until(c.readDeadline), func() { c.cond.Broadcast() })
		}
		c.cond.Wait()
		if timer != nil {
			timer.Stop()
		}
	}
}

func (c *clientConn) Write(b []byte) (n int, err error) {
	written := 0

	c.mu.Lock()
	defer c.mu.Unlock()

	for len(b) > 0 {
		if c.err != nil {
			return written, c.err
		}
		// Never enqueue on a closed carrier: a FIN may already be on its way and
		// bytes accepted after it would be reported as written but never sent.
		select {
		case <-c.closed:
			return written, io.ErrClosedPipe
		default:
		}

		if c.ep.Pending() == 0 {
			c.progressAt = time.Now() // the stuck clock starts when data starts waiting
		}
		n := c.ep.Write(b)
		b = b[n:]
		written += n
		if n > 0 {
			c.cond.Broadcast()
		}

		if len(b) == 0 {
			break
		}

		select {
		case <-c.closed:
			return written, io.ErrClosedPipe
		default:
		}
		select {
		case <-c.ctx.Done():
			return written, c.ctx.Err()
		default:
		}

		if !c.writeDeadline.IsZero() && time.Now().After(c.writeDeadline) {
			return written, timeoutError{}
		}

		var timer *time.Timer
		if !c.writeDeadline.IsZero() {
			timer = time.AfterFunc(time.Until(c.writeDeadline), func() { c.cond.Broadcast() })
		}
		c.cond.Wait()
		if timer != nil {
			timer.Stop()
		}
	}
	return written, nil
}

func (c *clientConn) Close() error {
	c.mu.Lock()
	select {
	case <-c.closed:
		c.mu.Unlock()
		return nil
	default:
		close(c.closed)
		c.notifyAll()
	}
	c.mu.Unlock()

	// Give the FIN one round trip to get out, but never hold the caller for long
	// (a dead link would otherwise stall every reconnect).
	select {
	case <-c.finDone:
	case <-time.After(closeGrace):
	case <-c.ctx.Done():
	}
	c.cancel()
	c.wg.Wait()
	c.pool.closeAll()
	return nil
}

func (c *clientConn) LocalAddr() net.Addr {
	return &net.IPAddr{IP: net.IPv4zero}
}

func (c *clientConn) RemoteAddr() net.Addr {
	return &net.IPAddr{IP: net.IPv4zero}
}

func (c *clientConn) SetDeadline(t time.Time) error {
	c.SetReadDeadline(t)
	c.SetWriteDeadline(t)
	return nil
}

func (c *clientConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.readDeadline = t
	c.mu.Unlock()
	c.notifyAll()
	return nil
}

func (c *clientConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	c.writeDeadline = t
	c.mu.Unlock()
	c.notifyAll()
	return nil
}

type timeoutError struct{}

func (e timeoutError) Error() string   { return "i/o timeout" }
func (e timeoutError) Timeout() bool   { return true }
func (e timeoutError) Temporary() bool { return true }
