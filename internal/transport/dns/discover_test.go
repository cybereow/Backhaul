package dnsx

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/miekg/dns"
)

func startResponder(t *testing.T, domain, key string, delay time.Duration) string {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var conn net.PacketConn = pc
	if delay > 0 {
		conn = &stallConn{PacketConn: pc, delay: delay}
	}
	r := NewResponder(domain, key, newTestLogger())
	srv := &dns.Server{PacketConn: conn, Handler: dns.HandlerFunc(r.handle)}
	go func() { _ = srv.ActivateAndServe() }()
	t.Cleanup(func() { _ = srv.Shutdown() })
	return pc.LocalAddr().String()
}

func TestDiscoverRanksAndDropsDead(t *testing.T) {
	const domain, key = "t.example.com", "k"
	fast := startResponder(t, domain, key, 0)
	slow := startResponder(t, domain, key, 400*time.Millisecond)
	blackhole, err := net.ListenPacket("udp", "127.0.0.1:0") // bound, never answers
	if err != nil {
		t.Fatal(err)
	}
	defer blackhole.Close()
	closed, _ := net.ListenPacket("udp", "127.0.0.1:0")
	closedAddr := closed.LocalAddr().String()
	closed.Close() // nothing listens here any more

	cache := filepath.Join(t.TempDir(), "resolvers.json")
	got := DiscoverResolvers(context.Background(), []string{slow, blackhole.LocalAddr().String(), closedAddr, fast}, DiscoverOpts{
		Domain: domain, Key: key, Timeout: time.Second, Samples: 4, CachePath: cache,
	})
	if len(got) != 2 || got[0].Resolver != fast || got[1].Resolver != slow {
		t.Fatalf("want [fast slow], got %+v", got)
	}
	if got[0].OK == 0 || got[0].Score <= got[1].Score {
		t.Errorf("fast resolver should outscore the slow one: %+v", got)
	}

	// same inputs again: served from the cache (a 1ns timeout would fail every live probe)
	again := DiscoverResolvers(context.Background(), []string{slow, blackhole.LocalAddr().String(), closedAddr, fast}, DiscoverOpts{
		Domain: domain, Key: key, Timeout: time.Nanosecond, CachePath: cache,
	})
	if len(again) != 2 || again[0].Resolver != fast {
		t.Fatalf("cache not used: %+v", again)
	}
}

func TestExpandCIDRs(t *testing.T) {
	got, err := ExpandCIDRs([]string{"10.1.2.0/30", "9.9.9.9"}, 100)
	if err != nil || len(got) != 5 || got[0] != "10.1.2.0" || got[3] != "10.1.2.3" || got[4] != "9.9.9.9" {
		t.Fatalf("got %v err %v", got, err)
	}
	for _, bad := range [][]string{{"10.0.0.0/8"}, {"10.0.0.0/24", "10.0.1.0/24"}, {"nonsense"}} {
		if _, err := ExpandCIDRs(bad, 300); err == nil && len(bad) == 1 && bad[0] != "10.0.0.0/24" {
			t.Errorf("ExpandCIDRs(%v) accepted", bad)
		}
	}
	if _, err := ExpandCIDRs([]string{"10.0.0.0/24", "10.0.1.0/24"}, 300); err == nil {
		t.Error("total over the limit accepted")
	}
	if !IsAuto(nil) || !IsAuto([]string{"1.2.3.4", "Auto"}) || IsAuto([]string{"1.2.3.4"}) {
		t.Error("IsAuto wrong")
	}
}

func TestDiscoverCacheBoundToInputs(t *testing.T) {
	const domain, key = "t.example.com", "k"
	fast := startResponder(t, domain, key, 0)
	cache := filepath.Join(t.TempDir(), "r.json")
	opts := DiscoverOpts{Domain: domain, Key: key, Timeout: time.Second, Samples: 2, CachePath: cache}
	if got := DiscoverResolvers(context.Background(), []string{fast}, opts); len(got) != 1 {
		t.Fatalf("first run: %+v", got)
	}
	// different candidate set: the cached entry must not be reused
	if got := DiscoverResolvers(context.Background(), []string{"127.0.0.1:1"}, opts); len(got) != 0 {
		t.Errorf("cache reused for a different candidate set: %+v", got)
	}
	// different domain: same
	o2 := opts
	o2.Domain = "other.example.com"
	if got := DiscoverResolvers(context.Background(), []string{fast}, o2); len(got) != 0 {
		t.Errorf("cache reused for a different domain: %+v", got)
	}
}

func TestDiscoverUsesConfiguredRecordTypes(t *testing.T) {
	const domain, key = "t.example.com", "k"
	addr := startResponder(t, domain, key, 0)
	// the carrier may use any type; discovery must keep a resolver that works for the allowed ones
	got := DiscoverResolvers(context.Background(), []string{addr}, DiscoverOpts{
		Domain: domain, Key: key, Timeout: time.Second, Samples: 2, RRTypes: []uint16{dns.TypeAAAA},
	})
	if len(got) != 1 {
		t.Fatalf("resolver carrying AAAA dropped: %+v", got)
	}
}

func TestResponderAnswersApexSOA(t *testing.T) {
	const domain, key = "t.example.com", "k"
	addr := startResponder(t, domain, key, 0)
	c := new(dns.Client)
	m := new(dns.Msg)
	m.SetQuestion(dns.Fqdn(domain), dns.TypeSOA)
	r, _, err := c.Exchange(m, addr)
	if err != nil || r.Rcode != dns.RcodeSuccess || len(r.Answer) != 1 {
		t.Fatalf("apex SOA: err=%v reply=%v", err, r)
	}
	m.SetQuestion(dns.Fqdn(domain), dns.TypeTXT)
	r, _, err = c.Exchange(m, addr)
	if err != nil || r.Rcode != dns.RcodeSuccess || len(r.Answer) != 0 || len(r.Ns) != 1 {
		t.Fatalf("apex TXT should be NODATA with SOA authority: err=%v reply=%v", err, r)
	}
}

func TestDefaultProfilesDedupesNormalizedResolvers(t *testing.T) {
	got := DefaultProfiles("t.example.com", []string{"2.189.44.44", "2.189.44.44:53", " 2.189.44.44 "}, []string{"TXT"})
	if len(got) != 2 { // one resolver x TXT x {udp,tcp}
		t.Fatalf("got %d profiles, want 2: %v", len(got), got)
	}
}

func TestDiscoverKeepsResolverThatCarriesOnlyOneType(t *testing.T) {
	const domain, key = "t.example.com", "k"
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r := NewResponder(domain, key, newTestLogger())
	srv := &dns.Server{PacketConn: pc, Handler: dns.HandlerFunc(func(w dns.ResponseWriter, req *dns.Msg) {
		if len(req.Question) == 1 && req.Question[0].Qtype == dns.TypeA { // everything else is filtered
			r.handle(w, req)
		}
	})}
	go func() { _ = srv.ActivateAndServe() }()
	defer srv.Shutdown()

	got := DiscoverResolvers(context.Background(), []string{pc.LocalAddr().String()}, DiscoverOpts{
		Domain: domain, Key: key, Timeout: 400 * time.Millisecond,
	})
	if len(got) != 1 || got[0].Best != "A/udp" {
		t.Fatalf("resolver whose only carrier is A dropped or mis-scored: %+v", got)
	}
}

func TestDiscoverDedupesAndRefreshes(t *testing.T) {
	const domain, key = "t.example.com", "k"
	addr := startResponder(t, domain, key, 0)
	cache := filepath.Join(t.TempDir(), "r.json")
	opts := DiscoverOpts{Domain: domain, Key: key, Timeout: time.Second, CachePath: cache}
	got := DiscoverResolvers(context.Background(), []string{addr, " " + addr + " "}, opts)
	if len(got) != 1 {
		t.Fatalf("the same endpoint listed twice must be one resolver: %+v", got)
	}
	// Refresh must bypass the cache: with no time to answer, a cache hit would still return the entry.
	opts.Refresh, opts.Timeout = true, time.Nanosecond
	if got := DiscoverResolvers(context.Background(), []string{addr, " " + addr + " "}, opts); len(got) != 0 {
		t.Fatalf("Refresh still served the cache: %+v", got)
	}
}

func TestDiscoverRespectsQPSPerProbe(t *testing.T) {
	var dead []string
	for i := 0; i < 5; i++ {
		c, _ := net.ListenPacket("udp", "127.0.0.1:0")
		dead = append(dead, c.LocalAddr().String())
		c.Close()
	}
	// 5 candidates x 4 record types x 2 transports = 40 probes; at 20/s that takes >= ~2s
	start := time.Now()
	DiscoverResolvers(context.Background(), dead, DiscoverOpts{Domain: "t.example.com", Key: "k", Timeout: 300 * time.Millisecond, QPS: 20})
	if el := time.Since(start); el < 1800*time.Millisecond {
		t.Errorf("40 probes at 20 QPS finished in %v: the limit is applied per candidate, not per probe", el)
	}
}

func TestPercentilesNearestRank(t *testing.T) {
	if _, p50, _ := percentiles([]int64{10, 20}); p50 != 10 {
		t.Errorf("p50 of two samples = %d, want the lower (10)", p50)
	}
	ten := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	if _, p50, p90 := percentiles(ten); p50 != 5 || p90 != 9 {
		t.Errorf("p50/p90 of 1..10 = %d/%d, want 5/9", p50, p90)
	}
}
