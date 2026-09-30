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
	closeGrace   = 3 * time.Second  // max wait for the FIN exchange in Close
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
	c.hedge = !p.NoHedge
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

	c.warmup(p.Profiles)
	c.exchange()
	if c.err != nil {
		c.cancel()
		return nil, c.err
	}

	n := p.Workers
	if n < 1 {
		n = 1
	}
	for i := 0; i < n; i++ {
		c.wg.Add(1)
		go c.loop(i == 0)
	}

	return c, nil
}

const (
	warmSamples     = 4  // samples per profile before first use (>= sel MinWeight, == FailStreak)
	warmConcurrency = 48 // profiles probed in parallel (spread over resolvers, see interleave)
	warmFailStop    = 2  // consecutive failures after which the rest count as failed without waiting
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
			respSize := carrierRespSize(p.Cap)
			fails := 0
			for i := 0; i < warmSamples; i++ {
				if c.ctx.Err() != nil {
					return
				}
				ok := false
				if fails < warmFailStop {
					ctx, cancel := context.WithTimeout(c.ctx, c.timeout)
					t0 := time.Now()
					_, _, _, _, stage, _ := Exchange(ctx, c.domain, c.key, p.Resolver, p.RRType, p.Transport, true, respSize, func(uint64) []byte { return []byte{0} }, c.timeout)
					cancel()
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

	recentRX    bool
	logf        func(string, ...any)
	stats       map[sel.Profile]*profStat
	pool        *connPool
	hedge       bool          // hedging enabled
	maxHedge    int32         // cap on extra in-flight exchanges
	hedges      atomic.Int32  // extra exchanges currently in flight
	srtt        time.Duration // smoothed RTT of answered exchanges (capped), sets the hedge delay
	nextLog     time.Time
	lastOK      time.Time // last successful exchange (outage detection)
	outage      time.Duration
	established bool // first reply seen: stop sending SYN
}

// loop is one exchange worker. The primary keeps polling while idle; helpers
// only run while there is data to move.
func (c *clientConn) loop(primary bool) {
	defer c.wg.Done()

	timer := time.NewTimer(0) // Start immediately
	defer timer.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-timer.C:
			if !primary {
				c.mu.Lock()
				busy := c.ep.Pending() > 0 || c.recentRX || c.ep.PeerWnd() == 0
				c.mu.Unlock()
				if !busy {
					timer.Reset(pollFast)
					continue
				}
			}
			c.exchange()

			c.mu.Lock()
			pending := c.ep.Pending()
			recent := c.recentRX
			if primary {
				c.recentRX = false
			}
			peerWnd := c.ep.PeerWnd()
			c.mu.Unlock()

			if recent || pending > 0 || peerWnd == 0 {
				timer.Reset(1 * time.Microsecond)
			} else {
				timer.Reset(pollIdle)
			}
		}
	}
}

func (c *clientConn) exchange() {
	now := time.Now()

	c.mu.Lock()
	prof := c.mgr.Pick(now)

	respSize := carrierRespSize(prof.Cap)

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

	ctx, cancel := context.WithTimeout(c.ctx, to)
	defer cancel()

	t0 := time.Now()
	type netResult struct {
		data  []byte
		rttMs int64
		stage Stage
	}
	resCh := make(chan netResult, 1)
	go func() {
		d, _, _, ms, st, _ := exchangeVia(c.pool, ctx, c.domain, c.key, prof.Resolver, prof.RRType, prof.Transport, true, respSize, qDataFunc, to)
		resCh <- netResult{d, ms, st}
	}()
	var res netResult
	if c.hedge {
		select {
		case res = <-resCh:
		case <-time.After(c.hedgeDelay()):
			c.startHedge()
			res = <-resCh
		}
	} else {
		res = <-resCh
	}
	respData, rttMs, stage := res.data, res.rttMs, res.stage

	now = time.Now()
	c.mu.Lock()
	if stage == StageOK {
		c.mgr.Observe(now, prof, true, time.Duration(rttMs)*time.Millisecond)
		c.established = true
		c.lastOK = now
		// Track typical (not stalled) round trips: cap so a 2s stall cannot push
		// the hedge delay up to where hedging stops helping.
		rt := min(time.Duration(rttMs)*time.Millisecond, hedgeMaxDelay)
		if c.srtt == 0 {
			c.srtt = rt
		} else {
			c.srtt = (7*c.srtt + rt) / 8
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
				p, err := rel.Unmarshal(respData[1:])
				if err == nil {
					pendingBefore := c.ep.Pending()
					c.ep.Recv(now, p)
					// Only new data or an ACK that actually advanced counts as
					// activity; the cumulative ACK alone is nonzero forever.
					if len(p.Data) > 0 || c.ep.Pending() < pendingBefore {
						c.recentRX = true
					}
					c.notifyAll()
				}
			}
		}
	} else {
		// No reply at all counts as a failure too: otherwise a profile the path
		// silently drops would never be disabled and would eat every Nth exchange.
		c.mgr.Observe(now, prof, false, 0)
	}
	c.record(now, prof, stage == StageOK, now.Sub(t0))

	// Every resolver/profile has been failing for outageLimit: the tunnel is dead
	// for practical purposes. End it so the owner can reconnect and re-discover
	// resolvers instead of queueing streams onto it.
	if stage != StageOK && c.established && now.Sub(c.lastOK) > c.outage && c.err == nil {
		c.err = fmt.Errorf("dnsx: no resolver answered for %v", c.outage)
		c.cancel()
		c.notifyAll()
	}

	c.mu.Unlock()
}

const (
	hedgeMinDelay = 250 * time.Millisecond
	hedgeMaxDelay = 1200 * time.Millisecond
)

// hedgeDelay is how long an exchange may run before a helper is started: a few
// typical round trips, clamped.
func (c *clientConn) hedgeDelay() time.Duration {
	c.mu.Lock()
	d := 3 * c.srtt
	c.mu.Unlock()
	if d == 0 {
		d = 400 * time.Millisecond
	}
	return min(max(d, hedgeMinDelay), hedgeMaxDelay)
}

// startHedge runs one extra exchange in the background when there is data to
// move and the extra-exchange budget allows. Each extra exchange carries its own
// packet and processes its own reply, so nothing is duplicated or wasted; it just
// keeps the pipe full while a stalled exchange is still waiting.
func (c *clientConn) startHedge() {
	c.mu.Lock()
	busy := c.ep.Pending() > 0 || c.recentRX
	c.mu.Unlock()
	if !busy || c.ctx.Err() != nil {
		return
	}
	if c.hedges.Add(1) > c.maxHedge {
		c.hedges.Add(-1)
		return
	}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer c.hedges.Add(-1)
		c.exchange()
	}()
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
