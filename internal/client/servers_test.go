package client

import (
	"strings"
	"testing"

	"github.com/musix/backhaul/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tunnel is one already-expanded [[client.servers]] entry (the loader's job is
// covered in cmd); ResolveServers only validates and names them.
func tunnel(addr string) config.ClientConfig {
	return config.ClientConfig{
		Transport:  config.WSSMUX,
		Token:      "shared",
		RemoteAddr: addr,
		Path:       "/ws",
		SnifferLog: "backhaul.json",
	}
}

func TestResolveServersNoneIsSingleServer(t *testing.T) {
	cfg := tunnel("a:443")
	tunnels, err := ResolveServers(&cfg)
	require.NoError(t, err)
	assert.Nil(t, tunnels)
}

func TestResolveServersNames(t *testing.T) {
	b := tunnel("")
	b.RemoteAddrs = []string{"b1:443", "b2:443"}
	c := tunnel("c:443")
	c.Name = "custom"
	cfg := config.ClientConfig{Tunnels: []config.ClientConfig{tunnel("a:443"), b, c}}

	tunnels, err := ResolveServers(&cfg)
	require.NoError(t, err)
	require.Len(t, tunnels, 3)
	assert.Equal(t, "a:443", tunnels[0].Name, "name defaults to the address")
	assert.Equal(t, "b1:443", tunnels[1].Name, "or to the first of remote_addrs")
	assert.Equal(t, "custom", tunnels[2].Name)
	assert.Equal(t, cfg.Tunnels[1], tunnels[1].Config)
}

func TestResolveServersRejects(t *testing.T) {
	cases := []struct {
		name    string
		tunnels func() []config.ClientConfig
		top     func(*config.ClientConfig)
		want    string
	}{
		{name: "non-mux transport", tunnels: func() []config.ClientConfig {
			b := tunnel("b:443")
			b.Transport = config.WSS
			return []config.ClientConfig{tunnel("a:443"), b}
		}, want: "entry 2 (b:443): client 'servers' is only supported on the wsmux/wssmux"},
		{name: "top-level remote_addr alongside servers", top: func(c *config.ClientConfig) { c.RemoteAddr = "x:443" }, want: "move remote_addr"},
		{name: "top-level edge_ips alongside servers", top: func(c *config.ClientConfig) { c.EdgeIPs = []string{"1.1.1.1"} }, want: "move remote_addr"},
		{name: "entry without an address", tunnels: func() []config.ClientConfig {
			return []config.ClientConfig{tunnel("a:443"), tunnel("")}
		}, want: "entry 2: remote_addr or remote_addrs is required"},
		{name: "no token", tunnels: func() []config.ClientConfig {
			b := tunnel("b:443")
			b.Token = ""
			return []config.ClientConfig{tunnel("a:443"), b}
		}, want: "entry 2 (b:443): token is required"},
		{name: "same server twice", tunnels: func() []config.ClientConfig {
			b := tunnel("")
			b.Name = "again"
			b.RemoteAddrs = []string{"z:443", "A:443"}
			return []config.ClientConfig{tunnel("a:443"), b}
		}, want: "A:443/ws is already used by entry 1"},
		{name: "same server twice with an equivalent path", tunnels: func() []config.ClientConfig {
			a, b := tunnel("a:443"), tunnel("a:443")
			a.Path, b.Path, b.Name = "/p/", "p", "again"
			return []config.ClientConfig{a, b}
		}, want: "already used by entry 1"},
		{name: "duplicate names", tunnels: func() []config.ClientConfig {
			a, b := tunnel("a:443"), tunnel("b:443")
			a.Name, b.Name = "x", "x"
			return []config.ClientConfig{a, b}
		}, want: `name "x" is already used`},
		{name: "web_port collision", tunnels: func() []config.ClientConfig {
			a, b := tunnel("a:443"), tunnel("b:443")
			a.WebPort, b.WebPort = 2060, 2060
			return []config.ClientConfig{a, b}
		}, want: "web_port 2060 is already used by entry 1"},
		{name: "sniffer_log collision", tunnels: func() []config.ClientConfig {
			a, b := tunnel("a:443"), tunnel("b:443")
			a.WebPort, b.WebPort = 2060, 2061
			a.Sniffer, b.Sniffer = true, true
			return []config.ClientConfig{a, b}
		}, want: `sniffer_log "backhaul.json" is already used by entry 1`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.ClientConfig{Tunnels: []config.ClientConfig{tunnel("a:443"), tunnel("b:443")}}
			if tc.tunnels != nil {
				cfg.Tunnels = tc.tunnels()
			}
			if tc.top != nil {
				tc.top(&cfg)
			}
			_, err := ResolveServers(&cfg)
			require.Error(t, err)
			assert.True(t, strings.Contains(err.Error(), tc.want), "error %q should contain %q", err, tc.want)
		})
	}
}

func TestResolveServersAllowsRepeatsWithinOneEntry(t *testing.T) {
	// Listing an address twice in one entry only weights its round-robin.
	a := tunnel("")
	a.RemoteAddrs = []string{"a:443", "a:443", "a2:443"}
	cfg := config.ClientConfig{Tunnels: []config.ClientConfig{a, tunnel("b:443")}}
	_, err := ResolveServers(&cfg)
	require.NoError(t, err)
}

func TestResolveServersSnifferWithoutPanels(t *testing.T) {
	// A tunnel without a web panel never writes its usage file, so sharing the
	// name is harmless.
	a, b := tunnel("a:443"), tunnel("b:443")
	a.Sniffer, b.Sniffer = true, true
	a.WebPort = 2060
	cfg := config.ClientConfig{Tunnels: []config.ClientConfig{a, b}}
	_, err := ResolveServers(&cfg)
	require.NoError(t, err)
}
