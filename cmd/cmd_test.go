package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/musix/backhaul/config"
	"github.com/musix/backhaul/internal/client"
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
			cfg:  config.Config{Client: config.ClientConfig{Servers: []config.ClientServer{{RemoteAddr: "a:443"}}}},
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

// TestLoadConfigClientServers parses a multi-server client the way Run does
// (load, defaults, validation) and checks each tunnel's resolved settings,
// including the per-entry tls_verify: omitted inherits [client], explicit wins.
func TestLoadConfigClientServers(t *testing.T) {
	const doc = `
[client]
transport = "wssmux"
token = "shared"
connection_pool = 4

[[client.servers]]
name = "de"
remote_addr = "de.example:443"

[[client.servers]]
name = "nl"
remote_addrs = ["nl1.example:443", "nl2.example:443"]
token = "nl-token"
tls_verify = false
`
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	applyDefaults(cfg)
	if got := detectConfigType(cfg); got != "client" {
		t.Fatalf("detectConfigType = %q, want client", got)
	}
	if err := validateClientServers(cfg, "client"); err != nil {
		t.Fatalf("validateClientServers: %v", err)
	}
	tunnels, err := client.ResolveServers(&cfg.Client)
	if err != nil {
		t.Fatal(err)
	}
	if len(tunnels) != 2 {
		t.Fatalf("got %d tunnels, want 2", len(tunnels))
	}
	de, nl := tunnels[0].Config, tunnels[1].Config
	if de.Token != "shared" || !de.TLSVerify || de.ConnectionPool != 4 || de.RemoteAddr != "de.example:443" {
		t.Errorf("de resolved wrong: token=%q tls_verify=%v pool=%d addr=%q", de.Token, de.TLSVerify, de.ConnectionPool, de.RemoteAddr)
	}
	if nl.Token != "nl-token" || nl.TLSVerify || len(nl.RemoteAddrs) != 2 {
		t.Errorf("nl resolved wrong: token=%q tls_verify=%v addrs=%v", nl.Token, nl.TLSVerify, nl.RemoteAddrs)
	}
	if !nl.MuxWSFraming || !nl.MuxStealthHandshake {
		t.Error("loader defaults (framing, stealth handshake) must reach every tunnel")
	}
}

func TestValidateClientServersRejectsMixedTopLevelAddr(t *testing.T) {
	cfg := &config.Config{Client: config.ClientConfig{
		Transport:  config.WSMUX,
		Token:      "t",
		RemoteAddr: "x:443",
		Servers:    []config.ClientServer{{RemoteAddr: "a:443"}},
	}}
	if err := validateClientServers(cfg, "client"); err == nil {
		t.Fatal("expected an error for remote_addr set alongside servers")
	}
	if err := validateClientServers(cfg, "server"); err != nil {
		t.Fatalf("a server config is never checked: %v", err)
	}
}
