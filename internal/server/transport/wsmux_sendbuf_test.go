package transport

import "testing"

// TestTunnelLegSendBufDefaultsToReceiveWindow guards the upload-asymmetry fix:
// with no explicit so_sndbuf the server's accepted tunnel legs must be forced to
// the smux session receive window (MaxReceiveBuffer) rather than left on
// tcp_wmem autotuning, which pins the server->client (upload) direction at
// ~tcp_wmem[2]/RTT while download runs unthrottled. The derived default is
// force-only so it can never pin a clamped buffer when unprivileged.
func TestTunnelLegSendBufDefaultsToReceiveWindow(t *testing.T) {
	s := &WsMuxTransport{config: &WsMuxConfig{MaxReceiveBuffer: 16 * 1024 * 1024}}
	size, force := s.tunnelLegSendBuf()
	if size != 16*1024*1024 {
		t.Fatalf("tunnelLegSendBuf() size = %d, want the receive window %d", size, 16*1024*1024)
	}
	if !force {
		t.Fatal("a derived default must be force-only so it can't pin a clamped buffer when unprivileged")
	}
}

// TestTunnelLegSendBufHonorsExplicitConfig keeps an operator-set so_sndbuf
// authoritative: the derived floor only fills in when the config leaves it
// unset, and an explicit size keeps the ordinary (clamped-fallback) behavior.
func TestTunnelLegSendBufHonorsExplicitConfig(t *testing.T) {
	s := &WsMuxTransport{config: &WsMuxConfig{MaxReceiveBuffer: 16 * 1024 * 1024, SO_SNDBUF: 4 * 1024 * 1024}}
	size, force := s.tunnelLegSendBuf()
	if size != 4*1024*1024 {
		t.Fatalf("tunnelLegSendBuf() size = %d, want the explicit so_sndbuf %d", size, 4*1024*1024)
	}
	if force {
		t.Fatal("an explicit so_sndbuf must not be force-only; the operator asked for a fixed size")
	}
}
