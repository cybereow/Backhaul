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
