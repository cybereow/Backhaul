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

	// second call is served from the cache, without any network
	again := DiscoverResolvers(context.Background(), []string{"192.0.2.1"}, DiscoverOpts{Domain: domain, Key: key, CachePath: cache})
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
