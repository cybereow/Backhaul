package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/musix/backhaul/config"
)

func TestDeriveStreamBuffer(t *testing.T) {
	tests := []struct {
		name             string
		maxReceiveBuffer int
		muxCon           int
		want             int
	}{
		{
			// The shipped defaults: 4MB shared by 8 streams is 512KB each, so
			// the streams on a session can collectively fill the session window.
			name:             "defaults give each stream its share of the session budget",
			maxReceiveBuffer: defaultMaxReceiveBuffer, muxCon: defaultMuxCon,
			want: 512 * 1024,
		},
		{
			// A high concurrency setting divides the budget further, but never
			// below the window earlier releases ran with.
			name:             "floor at the historical 64KB window",
			maxReceiveBuffer: defaultMaxReceiveBuffer, muxCon: 256,
			want: minMaxStreamBuffer,
		},
		{
			// smux's VerifyConfig rejects a stream buffer larger than the
			// session buffer, so the clamp has to hold even for a tiny budget.
			name:             "never exceeds the session receive buffer",
			maxReceiveBuffer: 32 * 1024, muxCon: 1,
			want: 32 * 1024,
		},
		{
			// applyDefaults fills MuxCon in after MaxStreamBuffer, so the
			// derivation has to cope with it still being unset.
			name:             "unset mux_con falls back to the default concurrency",
			maxReceiveBuffer: defaultMaxReceiveBuffer, muxCon: 0,
			want: 512 * 1024,
		},
		{
			name:             "unset receive buffer falls back to the default budget",
			maxReceiveBuffer: 0, muxCon: defaultMuxCon,
			want: 512 * 1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := deriveStreamBuffer(tt.maxReceiveBuffer, tt.muxCon); got != tt.want {
				t.Fatalf("deriveStreamBuffer(%d, %d) = %d, want %d",
					tt.maxReceiveBuffer, tt.muxCon, got, tt.want)
			}
		})
	}
}

func TestApplyDefaultsStreamBuffer(t *testing.T) {
	t.Run("derived when unset", func(t *testing.T) {
		cfg := &config.Config{}
		applyDefaults(cfg)

		if cfg.Server.MaxStreamBuffer != 512*1024 {
			t.Errorf("server mux_streambuffer = %d, want %d", cfg.Server.MaxStreamBuffer, 512*1024)
		}
		if cfg.Client.MaxStreamBuffer != 512*1024 {
			t.Errorf("client mux_streambuffer = %d, want %d", cfg.Client.MaxStreamBuffer, 512*1024)
		}
		// smux rejects a per-stream window larger than the session budget.
		if cfg.Server.MaxStreamBuffer > cfg.Server.MaxReceiveBuffer {
			t.Errorf("server mux_streambuffer %d exceeds mux_recievebuffer %d",
				cfg.Server.MaxStreamBuffer, cfg.Server.MaxReceiveBuffer)
		}
		if cfg.Client.MaxStreamBuffer > cfg.Client.MaxReceiveBuffer {
			t.Errorf("client mux_streambuffer %d exceeds mux_recievebuffer %d",
				cfg.Client.MaxStreamBuffer, cfg.Client.MaxReceiveBuffer)
		}
	})

	t.Run("explicit config wins", func(t *testing.T) {
		cfg := &config.Config{}
		cfg.Server.MaxStreamBuffer = 128 * 1024
		cfg.Client.MaxStreamBuffer = 96 * 1024
		applyDefaults(cfg)

		if cfg.Server.MaxStreamBuffer != 128*1024 {
			t.Errorf("server mux_streambuffer = %d, want it left alone", cfg.Server.MaxStreamBuffer)
		}
		if cfg.Client.MaxStreamBuffer != 96*1024 {
			t.Errorf("client mux_streambuffer = %d, want it left alone", cfg.Client.MaxStreamBuffer)
		}
	})

	t.Run("follows a configured mux_con", func(t *testing.T) {
		cfg := &config.Config{}
		cfg.Server.MuxCon = 4
		applyDefaults(cfg)

		if cfg.Server.MaxStreamBuffer != 1024*1024 {
			t.Errorf("server mux_streambuffer = %d, want %d", cfg.Server.MaxStreamBuffer, 1024*1024)
		}
	})
}

// TestLoadConfigMuxWSFraming: mux_ws_framing is on when the key is omitted and an
// explicit false is honored, in both roles, through the real config loader (the
// omitted-versus-false distinction only exists at TOML decode time).
func TestLoadConfigMuxWSFraming(t *testing.T) {
	load := func(t *testing.T, toml string) *config.Config {
		t.Helper()
		path := filepath.Join(t.TempDir(), "config.toml")
		if err := os.WriteFile(path, []byte(toml), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := loadConfig(path)
		if err != nil {
			t.Fatalf("loadConfig: %v", err)
		}
		applyDefaults(cfg) // defaulting must not undo it
		return cfg
	}

	for _, tc := range []struct {
		name                 string
		toml                 string
		wantServer, wantClnt bool
	}{
		{"omitted in both", "[server]\ntransport = \"wsmux\"\n[client]\ntransport = \"wsmux\"\n", true, true},
		{"explicit false in both", "[server]\nmux_ws_framing = false\n[client]\nmux_ws_framing = false\n", false, false},
		{"explicit true in both", "[server]\nmux_ws_framing = true\n[client]\nmux_ws_framing = true\n", true, true},
		{"false only on the server", "[server]\nmux_ws_framing = false\n[client]\ntransport = \"wsmux\"\n", false, true},
		{"false only on the client", "[server]\ntransport = \"wsmux\"\n[client]\nmux_ws_framing = false\n", true, false},
		{"empty config", "", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := load(t, tc.toml)
			if cfg.Server.MuxWSFraming != tc.wantServer {
				t.Errorf("server mux_ws_framing = %v, want %v", cfg.Server.MuxWSFraming, tc.wantServer)
			}
			if cfg.Client.MuxWSFraming != tc.wantClnt {
				t.Errorf("client mux_ws_framing = %v, want %v", cfg.Client.MuxWSFraming, tc.wantClnt)
			}
		})
	}

	// Other transports ignore the key: it loads without complaint and only the
	// wsmux/wssmux wiring reads it.
	cfg := load(t, "[server]\ntransport = \"tcp\"\nbind_addr = \"0.0.0.0:1\"\nmux_ws_framing = false\n")
	if cfg.Server.Transport != config.TCP || cfg.Server.MuxWSFraming {
		t.Fatalf("transport = %q, mux_ws_framing = %v", cfg.Server.Transport, cfg.Server.MuxWSFraming)
	}
}

// TestLoadConfigMuxHalfClose: mux_half_close is server-only and off unless the
// operator turns it on (opt-in: it makes the server reject old clients), through
// the real loader and applyDefaults.
func TestLoadConfigMuxHalfClose(t *testing.T) {
	load := func(t *testing.T, toml string) *config.Config {
		t.Helper()
		path := filepath.Join(t.TempDir(), "config.toml")
		if err := os.WriteFile(path, []byte(toml), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := loadConfig(path)
		if err != nil {
			t.Fatalf("loadConfig: %v", err)
		}
		applyDefaults(cfg)
		return cfg
	}
	if cfg := load(t, ""); cfg.Server.MuxHalfClose {
		t.Fatal("mux_half_close defaulted to true")
	}
	if cfg := load(t, "[server]\ntransport = \"wsmux\"\n"); cfg.Server.MuxHalfClose {
		t.Fatal("an omitted mux_half_close defaulted to true")
	}
	if cfg := load(t, "[server]\nmux_half_close = true\n"); !cfg.Server.MuxHalfClose {
		t.Fatal("an explicit mux_half_close = true was lost")
	}
	if cfg := load(t, "[server]\nmux_half_close = false\n"); cfg.Server.MuxHalfClose {
		t.Fatal("an explicit mux_half_close = false was lost")
	}
}

// TestValidateHalfClose: the startup check behind the Fatalf in Run.
func TestValidateHalfClose(t *testing.T) {
	server := func(tr config.TransportType, ver int, on bool) *config.Config {
		cfg := &config.Config{}
		cfg.Server.Transport = tr
		cfg.Server.MuxVersion = ver
		cfg.Server.MuxHalfClose = on
		return cfg
	}
	for _, tc := range []struct {
		name    string
		cfg     *config.Config
		kind    string
		wantErr string // substring, "" = ok
	}{
		{"off is always fine", server(config.TCP, 1, false), "server", ""},
		{"wsmux v2", server(config.WSMUX, 2, true), "server", ""},
		{"wssmux v2", server(config.WSSMUX, 2, true), "server", ""},
		{"wsmux v1", server(config.WSMUX, 1, true), "server", "mux_version"},
		{"tcpmux", server(config.TCPMUX, 2, true), "server", "wsmux/wssmux"},
		{"plain ws", server(config.WS, 2, true), "server", "wsmux/wssmux"},
		{"a client config never triggers it", server(config.TCP, 1, true), "client", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateHalfClose(tc.cfg, tc.kind)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("error = %v, want one containing %q", err, tc.wantErr)
			}
		})
	}
}
