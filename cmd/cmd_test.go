package cmd

import (
	"os"
	"path/filepath"
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
