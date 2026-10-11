package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/musix/backhaul/config"
)

func TestDetectConfigType(t *testing.T) {
	cases := []struct {
		name string
		cfg  config.Config
		want string
	}{
		{
			name: "server by bind_addr",
			cfg:  config.Config{Server: config.ServerConfig{BindAddr: "0.0.0.0:443"}},
			want: "server",
		},
		{
			name: "client by single remote_addr",
			cfg:  config.Config{Client: config.ClientConfig{RemoteAddr: "ir.aosky.ir:443"}},
			want: "client",
		},
		{
			name: "client by remote_addrs only",
			cfg:  config.Config{Client: config.ClientConfig{RemoteAddrs: []string{"a:443", "b:443"}}},
			want: "client",
		},
		{
			name: "client by servers only",
			cfg:  config.Config{Client: config.ClientConfig{Tunnels: []config.ClientConfig{{RemoteAddr: "a:443"}}}},
			want: "client",
		},
		{
			name: "neither set",
			cfg:  config.Config{},
			want: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := detectConfigType(&tc.cfg); got != tc.want {
				t.Errorf("detectConfigType = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestLoadConfigTLSVerify verifies the secure-by-default behaviour of the
// TOML loader: omitting tls_verify must produce true (secure default),
// explicit true stays true, explicit false stays false. The metadata key is
// the contract; we must not infer omission from the bool zero value.
func TestLoadConfigTLSVerify(t *testing.T) {
	cases := []struct {
		name    string
		toml    string
		want    bool
		wantErr bool
	}{
		{
			name: "omitted defaults to true",
			toml: "[client]\nremote_addr = \"127.0.0.1:443\"\n",
			want: true,
		},
		{
			name: "explicit true stays true",
			toml: "[client]\nremote_addr = \"127.0.0.1:443\"\ntls_verify = true\n",
			want: true,
		},
		{
			name: "explicit false stays false",
			toml: "[client]\nremote_addr = \"127.0.0.1:443\"\ntls_verify = false\n",
			want: false,
		},
		{
			name:    "malformed TOML returns error",
			toml:    "[client\nbroken",
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "config.toml")
			if err := os.WriteFile(path, []byte(tc.toml), 0o600); err != nil {
				t.Fatalf("write temp config: %v", err)
			}
			cfg, err := loadConfig(path)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error from malformed TOML, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg.Client.TLSVerify != tc.want {
				t.Errorf("TLSVerify = %v, want %v", cfg.Client.TLSVerify, tc.want)
			}
		})
	}
}

// TestValidateStripeConfig covers the startup bounds on mux_stripe and
// mux_stripe_parity. The wire carries index/total/parity as single bytes, so
// the ceiling is 255 total legs even though Reed-Solomon itself allows 256
// shards; a wider configuration would be truncated by the sender's uint8 cast
// and rejected by the receiver, so it must not start. The helper is called
// directly (never through Run) so no case reaches logger.Fatalf/os.Exit.
func TestValidateStripeConfig(t *testing.T) {
	cases := []struct {
		name    string
		factor  int
		parity  int
		wantErr bool
	}{
		// Ordinary, post-applyDefaults values.
		{name: "defaults: width 1, no parity", factor: 1, parity: 0},
		{name: "plain striping", factor: 4, parity: 0},
		{name: "striping with parity", factor: 4, parity: 2},

		// Non-FEC width is bounded too - this is the case the old check missed.
		{name: "width at the wire ceiling", factor: 255, parity: 0},
		{name: "width one past the ceiling", factor: 256, parity: 0, wantErr: true},
		{name: "huge width", factor: 1 << 30, parity: 0, wantErr: true},
		{name: "zero width", factor: 0, parity: 0, wantErr: true},
		{name: "negative width", factor: -1, parity: 0, wantErr: true},

		// Parity needs at least two data legs.
		{name: "parity with width 1", factor: 1, parity: 1, wantErr: true},
		{name: "negative parity", factor: 4, parity: -1, wantErr: true},

		// Total is capped at 255, not the library's 256.
		{name: "total exactly at the ceiling", factor: 253, parity: 2},
		{name: "total 256 is one too many", factor: 254, parity: 2, wantErr: true},
		{name: "huge parity does not overflow the sum", factor: 2, parity: 1 << 30, wantErr: true},
	}
	for _, role := range []string{"server", "client"} {
		for _, tc := range cases {
			t.Run(role+"/"+tc.name, func(t *testing.T) {
				err := validateStripeConfig(role, tc.factor, tc.parity)
				if tc.wantErr && err == nil {
					t.Fatalf("validateStripeConfig(%q, %d, %d) = nil, want an error", role, tc.factor, tc.parity)
				}
				if !tc.wantErr && err != nil {
					t.Fatalf("validateStripeConfig(%q, %d, %d) = %v, want nil", role, tc.factor, tc.parity, err)
				}
				if err != nil && !strings.HasPrefix(err.Error(), role+" ") {
					t.Errorf("error should name the role it came from, got %q", err)
				}
			})
		}
	}
}

// TestValidateStripeConfigAcceptsAppliedDefaults pins the contract between
// applyDefaults and validateStripeConfig: whatever normalization does to an
// omitted or stray negative legacy value, the result must still validate.
func TestValidateStripeConfigAcceptsAppliedDefaults(t *testing.T) {
	cfg := &config.Config{}
	cfg.Server.StripeFactor, cfg.Server.StripeParity = 0, -3
	cfg.Client.StripeFactor, cfg.Client.StripeParity = -1, -1
	applyDefaults(cfg)

	if err := validateStripeConfig("server", cfg.Server.StripeFactor, cfg.Server.StripeParity); err != nil {
		t.Errorf("normalized server stripe config rejected: %v", err)
	}
	if err := validateStripeConfig("client", cfg.Client.StripeFactor, cfg.Client.StripeParity); err != nil {
		t.Errorf("normalized client stripe config rejected: %v", err)
	}
}

func TestValidateDNSMux(t *testing.T) {
	srv := &config.Config{Server: config.ServerConfig{Transport: config.DNSMUX}}
	if validateDNSMux(srv, "server") == nil {
		t.Error("server without dns_domain accepted")
	}
	srv.Server.DNSDomain = "t.example.com"
	if err := validateDNSMux(srv, "server"); err != nil {
		t.Errorf("valid server rejected: %v", err)
	}

	cli := &config.Config{Client: config.ClientConfig{Transport: config.DNSMUX, DNSDomain: "t.example.com"}}
	// dns_resolvers is optional: empty means auto-discovery.
	if err := validateDNSMux(cli, "client"); err != nil {
		t.Errorf("client without dns_resolvers rejected: %v", err)
	}
	cli.Client.DNSDomain = ""
	if validateDNSMux(cli, "client") == nil {
		t.Error("client without dns_domain accepted")
	}

	other := &config.Config{Server: config.ServerConfig{Transport: config.TCP}}
	if err := validateDNSMux(other, "server"); err != nil {
		t.Errorf("non-dnsmux transport affected: %v", err)
	}
}

func TestDetectConfigTypeDNSMux(t *testing.T) {
	srv := &config.Config{Server: config.ServerConfig{Transport: config.DNSMUX, DNSDomain: "t.example.com"}}
	if got := detectConfigType(srv); got != "server" {
		t.Errorf("dnsmux server detected as %q", got)
	}
	cli := &config.Config{Client: config.ClientConfig{Transport: config.DNSMUX, DNSDomain: "t.example.com"}}
	if got := detectConfigType(cli); got != "client" {
		t.Errorf("dnsmux client detected as %q", got)
	}
}

func TestValidateDNSMuxRejectsUnknownRecordTypes(t *testing.T) {
	cli := &config.Config{Client: config.ClientConfig{Transport: config.DNSMUX, DNSDomain: "t.example.com", DNSRecordTypes: []string{"TXT", "TXTT"}}}
	if validateDNSMux(cli, "client") == nil {
		t.Error("a misspelled record type was accepted")
	}
	cli.Client.DNSRecordTypes = []string{"txt", "MX"}
	if err := validateDNSMux(cli, "client"); err != nil {
		t.Errorf("valid (case-insensitive) record types rejected: %v", err)
	}
}

// loadClientServers runs a config through the same steps as Run: load (which
// expands [[client.servers]]), defaults, validation.
func loadClientServers(t *testing.T, doc string) (*config.Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(path)
	if err != nil {
		return cfg, err
	}
	applyDefaults(cfg)
	if got := detectConfigType(cfg); got != "client" {
		t.Fatalf("detectConfigType = %q, want client", got)
	}
	return cfg, validateClientServers(cfg, "client")
}

// TestLoadConfigClientServers: an entry may override any [client] key, and
// everything it leaves out is inherited. Defaults are worked out per tunnel.
func TestLoadConfigClientServers(t *testing.T) {
	cfg, err := loadClientServers(t, `
[client]
transport = "wssmux"
token = "shared"
connection_pool = 4
mux_version = 2
keepalive_period = 30
mux_recievebuffer = 8388608
edge_ips = []
web_port = 2060

[[client.servers]]
name = "de"
remote_addr = "de.example:443"

[[client.servers]]
name = "nl"
remote_addrs = ["nl1.example:443", "nl2.example:443"]
edge_ips = ["1.1.1.1", "2.2.2.2"]
token = "nl-token"
tls_verify = false
transport = "wsmux"
mux_version = 1
keepalive_period = 90
mux_recievebuffer = 2097152
mux_ws_framing = false
log_level = "debug"

[[client.servers]]
name = "fr"
remote_addr = "fr.example:443"
web_port = 2062
`)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Client.Tunnels) != 3 {
		t.Fatalf("got %d tunnels, want 3", len(cfg.Client.Tunnels))
	}
	de, nl, fr := cfg.Client.Tunnels[0], cfg.Client.Tunnels[1], cfg.Client.Tunnels[2]

	// de sets nothing but its address: everything else comes from [client].
	if de.Token != "shared" || !de.TLSVerify || de.Transport != "wssmux" || de.MuxVersion != 2 || de.Keepalive != 30 || de.ConnectionPool != 4 {
		t.Errorf("de did not inherit: %+v", de)
	}
	if de.MaxStreamBuffer != deriveStreamBuffer(8388608, defaultMuxCon) {
		t.Errorf("de mux_streambuffer = %d, want it derived from the inherited receive buffer", de.MaxStreamBuffer)
	}
	if !de.MuxWSFraming || !de.MuxStealthHandshake {
		t.Error("loader defaults (framing, stealth handshake) must reach every tunnel")
	}
	if de.WebPort != 2060 {
		t.Errorf("first tunnel web_port = %d, want [client]'s 2060", de.WebPort)
	}

	// nl overrides keys of every kind.
	if nl.Token != "nl-token" || nl.TLSVerify || nl.Transport != "wsmux" || nl.MuxVersion != 1 || nl.Keepalive != 90 || nl.MuxWSFraming || nl.LogLevel != "debug" {
		t.Errorf("nl overrides lost: %+v", nl)
	}
	if nl.MaxStreamBuffer != deriveStreamBuffer(2097152, defaultMuxCon) {
		t.Errorf("nl mux_streambuffer = %d, want it derived from nl's own receive buffer", nl.MaxStreamBuffer)
	}
	if nl.ConnectionPool != 4 || len(nl.RemoteAddrs) != 2 || len(nl.EdgeIPs) != 2 {
		t.Errorf("nl inherited/list values wrong: pool=%d addrs=%v edges=%v", nl.ConnectionPool, nl.RemoteAddrs, nl.EdgeIPs)
	}
	if nl.WebPort != 0 {
		t.Errorf("later tunnels never inherit [client].web_port, nl has %d", nl.WebPort)
	}
	if fr.WebPort != 2062 {
		t.Errorf("fr web_port = %d, want its own 2062", fr.WebPort)
	}
	for _, tun := range cfg.Client.Tunnels {
		if tun.Name == "" || tun.Servers != nil || tun.Tunnels != nil {
			t.Errorf("tunnel %q must be a single-server config", tun.Name)
		}
	}
}

// TestLoadConfigClientServersListsAreIndependent: the TOML decoder writes a
// decoded array into the existing backing array, so an entry's list must never
// overwrite [client]'s (and so every other tunnel's inherited) list.
func TestLoadConfigClientServersListsAreIndependent(t *testing.T) {
	cfg, err := loadClientServers(t, `
[client]
transport = "wsmux"
token = "t"
stripe_ports = ["80", "443", "8080"]

[[client.servers]]
remote_addr = "a:443"
stripe_ports = ["22"]

[[client.servers]]
remote_addr = "b:443"
`)
	if err != nil {
		t.Fatal(err)
	}
	a, b := cfg.Client.Tunnels[0], cfg.Client.Tunnels[1]
	if strings.Join(a.StripePorts, ",") != "22" {
		t.Errorf("a stripe_ports = %v, want [22]", a.StripePorts)
	}
	if strings.Join(b.StripePorts, ",") != "80,443,8080" || strings.Join(cfg.Client.StripePorts, ",") != "80,443,8080" {
		t.Errorf("an entry's list leaked: b=%v [client]=%v", b.StripePorts, cfg.Client.StripePorts)
	}
}

func TestLoadConfigClientServersTokenOnlyPerEntry(t *testing.T) {
	cfg, err := loadClientServers(t, `
[client]
transport = "wssmux"

[[client.servers]]
remote_addr = "a:443"
token = "ta"

[[client.servers]]
remote_addr = "b:443"
token = "tb"
`)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Client.Tunnels[0].Token != "ta" || cfg.Client.Tunnels[1].Token != "tb" {
		t.Errorf("tokens = %q, %q", cfg.Client.Tunnels[0].Token, cfg.Client.Tunnels[1].Token)
	}
}

func TestLoadConfigClientServersRejects(t *testing.T) {
	cases := []struct {
		name, doc, want string
	}{
		{"unknown key in an entry", `
[client]
transport = "wsmux"
token = "t"
[[client.servers]]
remote_addr = "a:443"
mux_verison = 2
`, "unknown key(s) mux_verison"},
		{"process-wide key in an entry", `
[client]
transport = "wsmux"
token = "t"
[[client.servers]]
remote_addr = "a:443"
pprof = true
`, `"pprof" applies to the whole process`},
		{"wrong type in an entry", `
[client]
transport = "wsmux"
token = "t"
[[client.servers]]
remote_addr = "a:443"
connection_pool = "lots"
`, "entry 1"},
		{"bad stripe settings in an entry", `
[client]
transport = "wsmux"
token = "t"
[[client.servers]]
remote_addr = "a:443"
mux_stripe = 1
mux_stripe_parity = 2
`, "entry 1"},
		{"token missing everywhere", `
[client]
transport = "wsmux"
[[client.servers]]
remote_addr = "a:443"
token = "ta"
[[client.servers]]
remote_addr = "b:443"
`, "token is required"},
		{"name in [client] is not inherited into duplicates", `
[client]
transport = "wsmux"
token = "t"
name = "x"
[[client.servers]]
remote_addr = "a:443"
[[client.servers]]
remote_addr = "a:443"
`, `name "a:443" is already used by entry 1`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadClientServers(t, tc.doc)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

func TestValidateClientServersRejectsMixedTopLevelAddr(t *testing.T) {
	cfg := &config.Config{Client: config.ClientConfig{
		Transport:  config.WSMUX,
		Token:      "t",
		RemoteAddr: "x:443",
		Tunnels:    []config.ClientConfig{{RemoteAddr: "a:443", Transport: config.WSMUX, Token: "t", StripeFactor: 1}},
	}}
	if err := validateClientServers(cfg, "client"); err == nil {
		t.Fatal("expected an error for remote_addr set alongside servers")
	}
	if err := validateClientServers(cfg, "server"); err != nil {
		t.Fatalf("a server config is never checked: %v", err)
	}
}
