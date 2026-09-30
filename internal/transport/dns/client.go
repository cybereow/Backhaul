package dnsx

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/miekg/dns"
	"github.com/musix/backhaul/internal/transport/dns/rel"
	"github.com/musix/backhaul/internal/transport/dns/sel"
)

const (
	pollFast = 20 * time.Millisecond
	pollIdle = 500 * time.Millisecond

	minExchangeTimeout = 100 * time.Millisecond
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
	mss := maxQ - queryHdr - sessionFrame - rel.HeaderLen
	if mss < 0 {
		mss = 0
	}

	rcfg := p.Rel
	rcfg.MSS = mss
	rcfg.ISN = isn
	ep := rel.New(rcfg)
	mgr := sel.New(sel.Config{}, p.Profiles)
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
	}
	c.cond = sync.NewCond(&c.mu)
	c.logf = p.Logf
	c.stats = map[sel.Profile]*profStat{}

	c.ctx, c.cancel = context.WithCancel(ctx)

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
	warmConcurrency = 16 // profiles probed in parallel
	warmFailStop    = 2  // consecutive failures after which the rest count as failed without waiting
)

// warmup measures every profile in parallel with throwaway probe queries (1 byte
// of data: the responder answers those in diagnostic mode without touching any
// session) so sel starts with real numbers instead of exploring one slow,
// possibly dead profile per exchange. A profile that fails warmFailStop times in
// a row is marked failed at once.
func (c *clientConn) warmup(profiles []sel.Profile) {
	sem := make(chan struct{}, warmConcurrency)
	var wg sync.WaitGroup
	for _, p := range profiles {
		wg.Add(1)
		sem <- struct{}{}
		go func(p sel.Profile) {
			defer wg.Done()
			defer func() { <-sem }()
			respSize := p.Cap - envelopeOverhead
			if respSize < 0 {
				respSize = 0
			}
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

	closed chan struct{}
	err    error

	readDeadline  time.Time
	writeDeadline time.Time

	localAddr  net.Addr
	remoteAddr net.Addr

	recentRX    bool
	logf        func(string, ...any)
	stats       map[sel.Profile]*profStat
	nextLog     time.Time
	established bool          // first reply seen: stop sending SYN
	srtt        time.Duration // smoothed exchange RTT, drives the per-exchange timeout
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
			inflight := c.ep.Sent - c.ep.Retx
			recent := c.recentRX
			if primary {
				c.recentRX = false
			}
			peerWnd := c.ep.PeerWnd()
			c.mu.Unlock()

			if recent || pending > 0 || inflight > 0 || peerWnd == 0 {
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

	srvMss := prof.Cap - envelopeOverhead - sessionFrame - rel.HeaderLen
	if srvMss < 0 {
		srvMss = 0
	}
	respSize := prof.Cap - envelopeOverhead
	if respSize < 0 {
		respSize = 0
	}

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

	// A lost query/response is just a lost packet, so do not wait the full Timeout
	// for it: 8x the smoothed RTT, floored at minExchangeTimeout.
	to := c.timeout
	c.mu.Lock()
	if c.srtt > 0 {
		if t := 8 * c.srtt; t < to {
			to = t
		}
		if to < minExchangeTimeout {
			to = minExchangeTimeout
		}
	}
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(c.ctx, to)
	defer cancel()

	t0 := time.Now()
	respData, _, _, rttMs, stage, _ := Exchange(ctx, c.domain, c.key, prof.Resolver, prof.RRType, prof.Transport, true, respSize, qDataFunc, to)

	now = time.Now()
	c.mu.Lock()
	if stage == StageOK {
		c.mgr.Observe(now, prof, true, time.Duration(rttMs)*time.Millisecond)
		c.established = true
		if r := now.Sub(t0); c.srtt == 0 {
			c.srtt = r
		} else {
			c.srtt = (7*c.srtt + r) / 8
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
					c.ep.Recv(now, p)
					if len(p.Data) > 0 || p.Ack > 0 {
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

	active := c.mgr.Active()
	if len(active) == 0 {
	}

	c.mu.Unlock()
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

	go func() {
		time.Sleep(5 * time.Second)
		c.cancel()
	}()
	c.wg.Wait()
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
