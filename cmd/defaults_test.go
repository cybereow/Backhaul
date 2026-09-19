package cmd

import (
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
