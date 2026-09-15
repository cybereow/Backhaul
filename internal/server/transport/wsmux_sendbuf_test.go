package transport

import "testing"

// TestTunnelLegSendBufDefaultsToReceiveWindow guards the upload-asymmetry fix:
// with no explicit so_sndbuf the server's accepted tunnel legs must be forced to
// the smux session receive window (MaxReceiveBuffer) rather than left on
// tcp_wmem autotuning, which pins the server->client (upload) direction at
// ~tcp_wmem[2]/RTT while download runs unthrottled.
func TestTunnelLegSendBufDefaultsToReceiveWindow(t *testing.T) {
	s := &WsMuxTransport{config: &WsMuxConfig{MaxReceiveBuffer: 16 * 1024 * 1024}}
	if got, want := s.tunnelLegSendBuf(), 16*1024*1024; got != want {
		t.Fatalf("tunnelLegSendBuf() = %d, want the receive window %d", got, want)
	}
}

// TestTunnelLegSendBufHonorsExplicitConfig keeps an operator-set so_sndbuf
// authoritative: the derived floor only fills in when the config leaves it unset.
func TestTunnelLegSendBufHonorsExplicitConfig(t *testing.T) {
	s := &WsMuxTransport{config: &WsMuxConfig{MaxReceiveBuffer: 16 * 1024 * 1024, SO_SNDBUF: 4 * 1024 * 1024}}
	if got, want := s.tunnelLegSendBuf(), 4*1024*1024; got != want {
		t.Fatalf("tunnelLegSendBuf() = %d, want the explicit so_sndbuf %d", got, want)
	}
}
