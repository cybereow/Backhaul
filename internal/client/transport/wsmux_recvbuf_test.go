package transport

import "testing"

// TestTunnelLegRecvBufDefaultsToReceiveWindow mirrors the server's send-buffer
// fix on the receiving end of the upload path: with no explicit so_rcvbuf the
// client's dialed tunnel legs must hold the same in-flight window the server is
// now allowed to send, instead of relying on tcp_rmem autotuning that may not
// have been raised.
func TestTunnelLegRecvBufDefaultsToReceiveWindow(t *testing.T) {
	c := &WsMuxTransport{config: &WsMuxConfig{MaxReceiveBuffer: 16 * 1024 * 1024}}
	if got, want := c.tunnelLegRecvBuf(), 16*1024*1024; got != want {
		t.Fatalf("tunnelLegRecvBuf() = %d, want the receive window %d", got, want)
	}
}

// TestTunnelLegRecvBufHonorsExplicitConfig keeps an operator-set so_rcvbuf
// authoritative over the derived floor.
func TestTunnelLegRecvBufHonorsExplicitConfig(t *testing.T) {
	c := &WsMuxTransport{config: &WsMuxConfig{MaxReceiveBuffer: 16 * 1024 * 1024, SO_RCVBUF: 8 * 1024 * 1024}}
	if got, want := c.tunnelLegRecvBuf(), 8*1024*1024; got != want {
		t.Fatalf("tunnelLegRecvBuf() = %d, want the explicit so_rcvbuf %d", got, want)
	}
}
