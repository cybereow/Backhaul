package transport

import (
	"math"
	"net"
	"testing"
	"time"

	"github.com/musix/backhaul/internal/utils/network"
)

// capConn is a distinct net.Conn identity for the fake TCP_INFO source.
type capConn struct{ net.Conn }

// capFeed is a fake TCP_INFO source whose connections deliver at a set rate.
type capFeed struct {
	now   time.Time
	state map[net.Conn]*network.TCPDelivery
}

func newCapFeed() *capFeed {
	return &capFeed{now: time.Unix(1_700_000_000, 0), state: map[net.Conn]*network.TCPDelivery{}}
}

func (f *capFeed) info(c net.Conn) (network.TCPDelivery, bool) {
	d, ok := f.state[c]
	if !ok {
		return network.TCPDelivery{}, false
	}
	return *d, true
}

func (f *capFeed) get(c net.Conn) *network.TCPDelivery {
	d := f.state[c]
	if d == nil {
		d = &network.TCPDelivery{}
		f.state[c] = d
	}
	return d
}

// backlogged moves c forward one second during which it delivered at mbps with
// data queued throughout: the path was the limit.
func (f *capFeed) backlogged(c net.Conn, mbps float64) {
	d := f.get(c)
	d.BytesAcked += uint64(mbps * 1e6 / 8)
	d.Busy += time.Second
	d.NotSent = 1 << 20
	d.AppLimited = false
}

// unfilled moves c forward one second during which it delivered at mbps without
// a queue: the sender, not the path, set the pace.
func (f *capFeed) unfilled(c net.Conn, mbps float64) {
	d := f.get(c)
	d.BytesAcked += uint64(mbps * 1e6 / 8)
	d.Busy += 200 * time.Millisecond
	d.NotSent = 0
	d.AppLimited = true
}

// step takes one sample of every session, a second after the last.
func (f *capFeed) step(sessions []*pooledSession, hosts map[string]*hostCapacity) {
	f.now = f.now.Add(time.Second)
	sampleCapacity(sessions, hosts, f.now, f.info)
}

func capSession(f *capFeed, host string) *pooledSession {
	ps := &pooledSession{host: host, conn: &capConn{}}
	f.get(ps.conn)
	return ps
}

// A domain that is the limit at a fraction of what another was seen to carry is
// charged in proportion, on every one of its connections, measured or not. The
// fast domain need never have been the limit itself: the server may be unable
// to fill it.
func TestCapacityChargesTheSlowDomain(t *testing.T) {
	f := newCapFeed()
	fast, slow, slowIdle, unknown := capSession(f, "fast.example"), capSession(f, "slow.example"), capSession(f, "slow.example"), capSession(f, "new.example")
	sessions := []*pooledSession{fast, slow, slowIdle, unknown}
	hosts := map[string]*hostCapacity{}
	f.step(sessions, hosts) // the first look only sets the baseline
	for i := 0; i < 3; i++ {
		f.unfilled(fast.conn, 900)
		f.backlogged(slow.conn, 50)
		f.step(sessions, hosts)
	}

	if p := fast.slowness(); p != 1 {
		t.Fatalf("the best domain is charged %v", p)
	}
	if p := slow.slowness(); math.Abs(p-18) > 0.01 {
		t.Fatalf("a domain 18x slower is charged %v, want 18", p)
	}
	if p := slowIdle.slowness(); math.Abs(p-18) > 0.01 {
		t.Fatalf("an unmeasured connection of the slow domain is charged %v, want its domain's 18", p)
	}
	if p := unknown.slowness(); p != 1 {
		t.Fatalf("a domain nothing is known about is charged %v", p)
	}
	if got := slow.capEst.Load(); math.Abs(float64(got)-50e6/8) > 1 {
		t.Fatalf("the slow domain's estimate is %d bytes/s, want 50 Mbps", got)
	}
	// and the charge reaches the placement score
	if a, b := agedScore(100, fast), agedScore(100, slow); b != a*slow.slowness() {
		t.Fatalf("scores %v and %v do not reflect the charge", a, b)
	}
}

// Carrying little is not being slow. Only a domain seen to be the limit is
// charged: light traffic, a connection that moved nothing and a connection
// without a socket are not held against theirs, however far below the best they
// run. A domain that was the limit at a trickle is charged in full.
func TestCapacityChargesOnlyWhatWasSeenLimited(t *testing.T) {
	f := newCapFeed()
	fast, light, dead, trickle := capSession(f, "fast.example"), capSession(f, "light.example"), capSession(f, "dead.example"), capSession(f, "trickle.example")
	noSocket := &pooledSession{host: "nosocket.example"}
	sessions := []*pooledSession{fast, light, dead, trickle, noSocket}
	hosts := map[string]*hostCapacity{}
	f.step(sessions, hosts)
	for i := 0; i < 3; i++ {
		f.backlogged(fast.conn, 900)
		f.unfilled(light.conn, 3)         // a chat session
		f.backlogged(dead.conn, 0)        // queued, and nothing gets through
		f.backlogged(trickle.conn, 8*0.1) // queued, and 100 KB in the second
		f.step(sessions, hosts)
	}
	for _, ps := range []*pooledSession{fast, light, dead, noSocket} {
		if p := ps.slowness(); p != 1 {
			t.Fatalf("%s is charged %v", ps.host, p)
		}
	}
	if hosts["dead.example"] != nil || hosts["nosocket.example"] != nil {
		t.Fatal("a connection that moved nothing produced a sample")
	}
	if p := trickle.slowness(); p != capMaxPenalty {
		t.Fatalf("a domain that was the limit at 0.8 Mbps is charged %v, want %v", p, capMaxPenalty)
	}

	// A backlogged second in the middle of light traffic does not make the
	// connection the limit: the queue has to be there at both ends of the
	// interval.
	f.unfilled(light.conn, 3)
	f.step(sessions, hosts)
	d := f.get(light.conn)
	d.BytesAcked += 1 << 20
	d.Busy += time.Second
	d.NotSent = 1 << 20
	d.AppLimited = false
	f.step(sessions, hosts)
	if p := light.slowness(); p != 1 {
		t.Fatalf("a queue that had only just formed got the domain charged %v", p)
	}
}

// A domain is taken to carry the most it was seen to, not the least it was
// limited to: a connection limited while sharing the uplink with many others
// says less about the path than one that had it alone.
func TestCapacityUsesTheBestSeen(t *testing.T) {
	f := newCapFeed()
	a, b := capSession(f, "cdn.example"), capSession(f, "cdn.example")
	sessions := []*pooledSession{a, b}
	hosts := map[string]*hostCapacity{}
	f.step(sessions, hosts)
	for i := 0; i < 2; i++ {
		f.unfilled(a.conn, 900)   // alone: the server could not fill it
		f.backlogged(b.conn, 200) // later, limited while the uplink was shared
		f.step(sessions, hosts)
	}
	if p := b.slowness(); p != 1 {
		t.Fatalf("a domain is charged %v against its own best", p)
	}
	if got := float64(b.capEst.Load()) * 8 / 1e6; math.Abs(got-900) > 0.01 {
		t.Fatalf("the estimate is %.1f Mbps, want the best seen (900)", got)
	}
}

// Paths within a factor of two of the best are treated as equal, the charge is
// bounded, and an estimate expires so a path that recovered is tried again.
func TestCapacityPenaltyBoundsAndExpiry(t *testing.T) {
	if p := capacityPenalty(500, 900); p != 1 {
		t.Fatalf("a path at 55%% of the best is charged %v", p)
	}
	if p := capacityPenalty(1, 900); p != capMaxPenalty {
		t.Fatalf("the charge is %v, want it capped at %v", p, capMaxPenalty)
	}
	if p := capacityPenalty(0, 900); p != 1 {
		t.Fatalf("an unknown path is charged %v", p)
	}

	f := newCapFeed()
	fast, slow := capSession(f, "fast.example"), capSession(f, "slow.example")
	sessions := []*pooledSession{fast, slow}
	hosts := map[string]*hostCapacity{}
	f.step(sessions, hosts)
	for i := 0; i < 2; i++ { // the queue must be there at both ends of an interval
		f.unfilled(fast.conn, 900)
		f.backlogged(slow.conn, 50)
		f.step(sessions, hosts)
	}
	if slow.slowness() <= 1 {
		t.Fatal("the slow domain is not charged")
	}
	// nothing more is sent on either; the estimates age out
	f.now = f.now.Add(2 * capBucket)
	f.step(sessions, hosts)
	if p := slow.slowness(); p != 1 {
		t.Fatalf("an expired estimate still charges %v", p)
	}
	if len(hosts) != 0 {
		t.Fatalf("%d expired host estimates are kept", len(hosts))
	}
}

func TestHostKey(t *testing.T) {
	for in, want := range map[string]string{
		"N2.Example.com:443": "n2.example.com",
		"n2.example.com":     "n2.example.com",
		"[2001:db8::1]:8443": "2001:db8::1",
		"":                   "",
	} {
		if got := hostKey(in); got != want {
			t.Errorf("hostKey(%q) = %q, want %q", in, got, want)
		}
	}
}
