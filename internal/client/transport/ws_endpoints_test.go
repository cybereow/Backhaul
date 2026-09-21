package transport

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/musix/backhaul/config"
	"github.com/sirupsen/logrus"
)

const wsEndpointTestToken = "endpoint-test-token"

// upgradeServer is a loopback WebSocket upgrade server that records which
// paths and Host headers reached it. Tunnel connections are closed right after
// the upgrade (so tunnelDialer returns); control connections are held open
// until stop is closed.
type upgradeServer struct {
	srv  *httptest.Server
	stop chan struct{}

	mu    sync.Mutex
	paths []string
	hosts []string
}

func newUpgradeServer(t *testing.T) *upgradeServer {
	t.Helper()
	u := &upgradeServer{stop: make(chan struct{})}
	u.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+wsEndpointTestToken {
			http.Error(w, "bad token", http.StatusUnauthorized)
			return
		}
		u.mu.Lock()
		u.paths = append(u.paths, r.URL.Path)
		u.hosts = append(u.hosts, r.Host)
		u.mu.Unlock()

		conn, _, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			return
		}
		defer conn.Close()
		if strings.HasSuffix(r.URL.Path, "/channel") {
			// drain so the client's close frame does not back up
			go io.Copy(io.Discard, conn)
			<-u.stop
		}
	}))
	t.Cleanup(func() {
		close(u.stop)
		u.srv.Close()
	})
	return u
}

func (u *upgradeServer) addr() string { return u.srv.Listener.Addr().String() }

func (u *upgradeServer) port() string {
	_, p, _ := net.SplitHostPort(u.addr())
	return p
}

// count returns how many recorded requests have a path with the given prefix.
func (u *upgradeServer) count(prefix string) int {
	u.mu.Lock()
	defer u.mu.Unlock()
	n := 0
	for _, p := range u.paths {
		if strings.HasPrefix(p, prefix) {
			n++
		}
	}
	return n
}

func (u *upgradeServer) hostsSeen() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]string(nil), u.hosts...)
}

// waitCount polls until the server has seen want requests with the prefix.
func (u *upgradeServer) waitCount(t *testing.T, prefix string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if u.count(prefix) >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server %s: want %d %q requests, got %d", u.addr(), want, prefix, u.count(prefix))
}

// newEndpointTestClient builds a WsTransport the way NewWSClient does, minus
// the web UI. cancel stays nil on purpose: channelHandler only calls Restart
// when it is set, and a Restart racing test teardown is not what is under test.
func newEndpointTestClient(ctx context.Context, cfg *WsConfig) *WsTransport {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	cfg.Token = wsEndpointTestToken
	cfg.Mode = config.WS
	cfg.DialTimeOut = 2 * time.Second
	return &WsTransport{
		config:      cfg,
		ctx:         ctx,
		logger:      logger,
		controlFlow: make(chan struct{}, 100),
		userAgent:   "endpoint-test",
		endpoints:   buildEndpoints(cfg.RemoteAddrs, cfg.EdgeIPs, cfg.RemoteAddr, cfg.EdgeIP),
	}
}

// dialBoth drives n control dials and then n tunnel dials through the real
// channelDialer/tunnelDialer of c. The two phases are kept separate on
// purpose: control and tunnel dials share one round-robin counter, so
// interleaving them with an even endpoint count would pin every control dial
// to one endpoint and every tunnel dial to the other.
func dialBoth(c *WsTransport, n int) {
	for i := 0; i < n; i++ {
		c.channelDialer()
	}
	for i := 0; i < n; i++ {
		c.tunnelDialer()
	}
}

func TestWSEndpointDialPaths(t *testing.T) {
	t.Run("list_spreads_control_and_tunnel_over_both_endpoints", func(t *testing.T) {
		a, b := newUpgradeServer(t), newUpgradeServer(t)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// RemoteAddr is a dead address: a non-empty list must win over it.
		c := newEndpointTestClient(ctx, &WsConfig{
			RemoteAddr:  "127.0.0.1:1",
			RemoteAddrs: []string{a.addr(), b.addr()},
		})
		dialBoth(c, 4)

		for name, s := range map[string]*upgradeServer{"a": a, "b": b} {
			s.waitCount(t, "/channel", 2)
			s.waitCount(t, "/tunnel/", 2)
			if got := s.count("/channel"); got != 2 {
				t.Errorf("server %s: %d control dials, want 2", name, got)
			}
			if got := s.count("/tunnel/"); got != 2 {
				t.Errorf("server %s: %d tunnel dials, want 2", name, got)
			}
		}
	})

	t.Run("list_only_config_without_remote_addr", func(t *testing.T) {
		a, b := newUpgradeServer(t), newUpgradeServer(t)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		c := newEndpointTestClient(ctx, &WsConfig{RemoteAddrs: []string{a.addr(), b.addr()}})
		for i := 0; i < 4; i++ {
			c.tunnelDialer()
		}
		if a.count("/tunnel/") != 2 || b.count("/tunnel/") != 2 {
			t.Fatalf("tunnel dials a=%d b=%d, want 2 and 2", a.count("/tunnel/"), b.count("/tunnel/"))
		}
	})

	t.Run("edge_ip_alignment_keeps_hostname_and_missing_entry_dials_directly", func(t *testing.T) {
		a, b := newUpgradeServer(t), newUpgradeServer(t)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Entry 0 names a hostname that does not resolve but carries an edge
		// IP, so the dial can only succeed by connecting to the edge address.
		// Entry 1 has no edge entry and is dialed at its own address.
		hostA := "front.invalid:" + a.port()
		c := newEndpointTestClient(ctx, &WsConfig{
			RemoteAddrs: []string{hostA, b.addr()},
			EdgeIPs:     []string{"127.0.0.1"},
		})
		for i := 0; i < 4; i++ {
			c.tunnelDialer()
		}
		if a.count("/tunnel/") != 2 || b.count("/tunnel/") != 2 {
			t.Fatalf("tunnel dials a=%d b=%d, want 2 and 2", a.count("/tunnel/"), b.count("/tunnel/"))
		}
		// Host header stays the selected hostname, never the edge address.
		for _, h := range a.hostsSeen() {
			if h != hostA {
				t.Errorf("edge endpoint Host = %q, want %q", h, hostA)
			}
		}
		for _, h := range b.hostsSeen() {
			if h != b.addr() {
				t.Errorf("plain endpoint Host = %q, want %q", h, b.addr())
			}
		}
	})

	t.Run("single_config_unchanged", func(t *testing.T) {
		a, b := newUpgradeServer(t), newUpgradeServer(t)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		c := newEndpointTestClient(ctx, &WsConfig{RemoteAddr: a.addr()})
		if len(c.endpoints) != 1 {
			t.Fatalf("want 1 endpoint, got %d", len(c.endpoints))
		}
		dialBoth(c, 3)

		if got := a.count("/channel"); got != 3 {
			t.Errorf("control dials = %d, want 3", got)
		}
		if got := a.count("/tunnel/"); got != 3 {
			t.Errorf("tunnel dials = %d, want 3", got)
		}
		if b.count("/") != 0 {
			t.Errorf("unrelated server saw %d requests", b.count("/"))
		}
	})

	t.Run("custom_path_applies_to_every_endpoint", func(t *testing.T) {
		a, b := newUpgradeServer(t), newUpgradeServer(t)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		c := newEndpointTestClient(ctx, &WsConfig{
			RemoteAddrs: []string{a.addr(), b.addr()},
			Path:        "base/",
		})
		for i := 0; i < 4; i++ {
			c.tunnelDialer()
		}
		if a.count("/base/tunnel/") != 2 || b.count("/base/tunnel/") != 2 {
			t.Fatalf("path not preserved: a=%v b=%v", a.paths, b.paths)
		}
	})

	t.Run("unusable_entry_fails_that_dial_and_rotation_continues", func(t *testing.T) {
		live := newUpgradeServer(t)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// endpoints[1] is dialed first (dialSeq starts at 0 and is incremented
		// before use), so the dead entry takes the first dial.
		c := newEndpointTestClient(ctx, &WsConfig{RemoteAddrs: []string{live.addr(), "127.0.0.1:1"}})

		// Short context so the dial's retry backoff is abandoned quickly; there
		// is no health-aware failover, the dial simply fails and returns.
		short, shortCancel := context.WithTimeout(ctx, 300*time.Millisecond)
		c.ctx = short
		start := time.Now()
		c.tunnelDialer()
		shortCancel()
		if time.Since(start) > 5*time.Second {
			t.Fatalf("dial to unusable entry took %v", time.Since(start))
		}
		if live.count("/") != 0 {
			t.Fatalf("live server saw a request from the dead entry's dial")
		}

		c.ctx = ctx
		c.tunnelDialer()
		if live.count("/tunnel/") != 1 {
			t.Fatalf("live tunnel dials = %d, want 1", live.count("/tunnel/"))
		}
	})
}

func TestWSEndpointConcurrentSelection(t *testing.T) {
	addrs := []string{"a:443", "b:443", "c:443"}
	c := &WsTransport{endpoints: buildEndpoints(addrs, nil, "", "")}

	const workers, per = 8, 300 // 2400 picks, divisible by 3
	var mu sync.Mutex
	counts := map[string]int{}
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := map[string]int{}
			for i := 0; i < per; i++ {
				local[c.nextEndpoint().addr]++
			}
			mu.Lock()
			for k, v := range local {
				counts[k] += v
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	for _, a := range addrs {
		if want := workers * per / len(addrs); counts[a] != want {
			t.Errorf("endpoint %q picked %d times, want %d", a, counts[a], want)
		}
	}
}

// The plain and mux transports must select identically for the same config.
func TestWSEndpointSelectionMatchesMux(t *testing.T) {
	addrs := []string{"a:443", "b:443", "c:443"}
	edges := []string{"10.0.0.1"}
	plain := &WsTransport{endpoints: buildEndpoints(addrs, edges, "single:443", "9.9.9.9")}
	mux := &WsMuxTransport{endpoints: buildEndpoints(addrs, edges, "single:443", "9.9.9.9")}

	for i := 0; i < 10; i++ {
		p, m := plain.nextEndpoint(), mux.nextEndpoint()
		if p != m {
			t.Fatalf("pick %d: plain %+v != mux %+v", i, p, m)
		}
	}

	// A single address with an edge IP stays a single, stable endpoint.
	single := &WsTransport{endpoints: buildEndpoints(nil, nil, "single:443", "9.9.9.9")}
	for i := 0; i < 3; i++ {
		if got := single.nextEndpoint(); got.addr != "single:443" || got.edgeIP != "9.9.9.9" {
			t.Fatalf("single endpoint changed: %+v", got)
		}
	}
}
