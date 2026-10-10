package client

import (
	"strings"
	"testing"

	"github.com/musix/backhaul/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func multiBase() config.ClientConfig {
	return config.ClientConfig{
		Transport:      config.WSSMUX,
		Token:          "shared",
		ConnectionPool: 8,
		Path:           "/ws",
		TLSVerify:      true,
		WebPort:        2060,
		SnifferLog:     "backhaul.json",
		MuxVersion:     2,
	}
}

func TestResolveServersNoneIsSingleServer(t *testing.T) {
	cfg := multiBase()
	cfg.RemoteAddr = "a:443"
	tunnels, err := ResolveServers(&cfg)
	require.NoError(t, err)
	assert.Nil(t, tunnels)
}

func TestResolveServersInheritsAndOverrides(t *testing.T) {
	no := false
	cfg := multiBase()
	cfg.Servers = []config.ClientServer{
		{RemoteAddr: "a.example:443"},
		{Name: "b", RemoteAddrs: []string{"b1:443", "b2:443"}, EdgeIPs: []string{"1.2.3.4"}, Token: "tb", Path: "/other", TLSVerify: &no, ConnectionPool: 3, WebPort: 2061, SnifferLog: "b.json"},
		{RemoteAddr: "c.example:443"},
	}
	tunnels, err := ResolveServers(&cfg)
	require.NoError(t, err)
	require.Len(t, tunnels, 3)

	a := tunnels[0]
	assert.Equal(t, "a.example:443", a.Name, "name defaults to the address")
	assert.Equal(t, "a.example:443", a.Config.RemoteAddr)
	assert.Equal(t, "shared", a.Config.Token)
	assert.Equal(t, "/ws", a.Config.Path)
	assert.True(t, a.Config.TLSVerify)
	assert.Equal(t, 8, a.Config.ConnectionPool)
	assert.Equal(t, 2060, a.Config.WebPort, "the first tunnel keeps [client].web_port")
	assert.Equal(t, 2, a.Config.MuxVersion, "everything else is inherited")
	assert.Nil(t, a.Config.Servers, "a resolved tunnel is a single-server config")

	b := tunnels[1]
	assert.Equal(t, "b", b.Name)
	assert.Equal(t, []string{"b1:443", "b2:443"}, b.Config.RemoteAddrs)
	assert.Equal(t, []string{"1.2.3.4"}, b.Config.EdgeIPs)
	assert.Equal(t, "tb", b.Config.Token)
	assert.Equal(t, "/other", b.Config.Path)
	assert.False(t, b.Config.TLSVerify)
	assert.Equal(t, 3, b.Config.ConnectionPool)
	assert.Equal(t, 2061, b.Config.WebPort)
	assert.Equal(t, "b.json", b.Config.SnifferLog)

	c := tunnels[2]
	assert.Equal(t, 0, c.Config.WebPort, "later tunnels never inherit [client].web_port")
	assert.Equal(t, "shared", c.Config.Token)

	// The resolved configs must not alias the original's slices.
	b.Config.RemoteAddrs[0] = "changed"
	assert.Equal(t, "b1:443", cfg.Servers[1].RemoteAddrs[0])
}

func TestResolveServersRejects(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*config.ClientConfig)
		want   string
	}{
		{"non-mux transport", func(c *config.ClientConfig) {
			c.Transport = config.WSS
			c.Servers = []config.ClientServer{{RemoteAddr: "a:443"}}
		}, "only supported on the wsmux/wssmux"},
		{"top-level remote_addr alongside servers", func(c *config.ClientConfig) {
			c.RemoteAddr = "x:443"
			c.Servers = []config.ClientServer{{RemoteAddr: "a:443"}}
		}, "move remote_addr"},
		{"top-level edge_ips alongside servers", func(c *config.ClientConfig) {
			c.EdgeIPs = []string{"1.1.1.1"}
			c.Servers = []config.ClientServer{{RemoteAddr: "a:443"}}
		}, "move remote_addr"},
		{"entry without an address", func(c *config.ClientConfig) {
			c.Servers = []config.ClientServer{{RemoteAddr: "a:443"}, {Name: "empty"}}
		}, "entry 2: remote_addr or remote_addrs is required"},
		{"no token anywhere", func(c *config.ClientConfig) {
			c.Token = ""
			c.Servers = []config.ClientServer{{RemoteAddr: "a:443", Token: "ta"}, {RemoteAddr: "b:443"}}
		}, "entry 2 (b:443): token is required"},
		{"same server twice", func(c *config.ClientConfig) {
			c.Servers = []config.ClientServer{{RemoteAddr: "a:443"}, {Name: "again", RemoteAddrs: []string{"z:443", "A:443"}}}
		}, "A:443/ws is already used by entry 1"},
		{"same server twice with an equivalent path", func(c *config.ClientConfig) {
			c.Servers = []config.ClientServer{{RemoteAddr: "a:443", Path: "/p/"}, {Name: "again", RemoteAddr: "a:443", Path: "p"}}
		}, "already used by entry 1"},
		{"duplicate names", func(c *config.ClientConfig) {
			c.Servers = []config.ClientServer{{Name: "x", RemoteAddr: "a:443"}, {Name: "x", RemoteAddr: "b:443"}}
		}, `name "x" is already used`},
		{"web_port collision", func(c *config.ClientConfig) {
			c.Servers = []config.ClientServer{{RemoteAddr: "a:443"}, {RemoteAddr: "b:443", WebPort: 2060}}
		}, "web_port 2060 is already used by entry 1"},
		{"sniffer_log collision", func(c *config.ClientConfig) {
			c.Sniffer = true
			c.Servers = []config.ClientServer{{RemoteAddr: "a:443"}, {RemoteAddr: "b:443", WebPort: 2061}}
		}, `sniffer_log "backhaul.json" is already used by entry 1`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := multiBase()
			tc.mutate(&cfg)
			_, err := ResolveServers(&cfg)
			require.Error(t, err)
			assert.True(t, strings.Contains(err.Error(), tc.want), "error %q should contain %q", err, tc.want)
		})
	}
}

func TestResolveServersAllowsRepeatsWithinOneEntry(t *testing.T) {
	// Listing an address twice in one entry only weights its round-robin.
	cfg := multiBase()
	cfg.Servers = []config.ClientServer{{RemoteAddrs: []string{"a:443", "a:443", "a2:443"}}, {RemoteAddr: "b:443"}}
	_, err := ResolveServers(&cfg)
	require.NoError(t, err)
}

func TestResolveServersSnifferWithDistinctLogs(t *testing.T) {
	cfg := multiBase()
	cfg.Sniffer = true
	cfg.Servers = []config.ClientServer{{RemoteAddr: "a:443"}, {RemoteAddr: "b:443", WebPort: 2061, SnifferLog: "b.json"}, {RemoteAddr: "c:443"}}
	_, err := ResolveServers(&cfg)
	require.NoError(t, err, "a tunnel without a web panel never writes its usage file")
}
