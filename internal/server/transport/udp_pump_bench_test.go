package transport

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// discardConn is a net.Conn whose Write always succeeds instantly and keeps no
// data, so the benchmark measures udpToTCP's own per-packet cost rather than
// the socket's.
type discardConn struct{ net.Conn }

func (discardConn) Write(p []byte) (int, error) { return len(p), nil }
func (discardConn) Close() error                { return nil }

func benchLogger() *logrus.Logger {
	l := logrus.New()
	l.SetOutput(io.Discard)
	l.SetLevel(logrus.InfoLevel) // the production default: Trace/Debug are suppressed
	return l
}

// BenchmarkUDPToTCP measures the per-packet cost of the UDP->TCP forwarding
// pump, the hot loop every tunneled UDP packet passes through.
func BenchmarkUDPToTCP(b *testing.B) {
	payload := make([]byte, 512)

	udp := &LocalAcceptUDPConn{
		payload:    make(chan []byte, 1024),
		clientAddr: &net.UDPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 5000},
	}

	go func() {
		for i := 0; i < b.N; i++ {
			udp.payload <- payload
		}
		close(udp.payload)
	}()

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()

	udpToTCP(discardConn{}, udp, benchLogger(), nil, 8080, false)

	b.StopTimer()
}

// TestUDPToTCPIdleTimeoutRearms verifies the reused idle timer behaves like the
// per-iteration time.After it replaced: the pump must NOT time out while
// packets keep arriving (the timer is re-armed per packet), and must return
// once the flow goes quiet for the timeout.
func TestUDPToTCPIdleTimeoutRearms(t *testing.T) {
	udp := &LocalAcceptUDPConn{
		payload:    make(chan []byte),
		clientAddr: &net.UDPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 5000},
	}

	var written [][]byte
	done := make(chan struct{})
	go func() {
		udpToTCPWithTimeout(recordConn{&written}, udp, benchLogger(), nil, 0, false, 150*time.Millisecond)
		close(done)
	}()

	// Keep feeding packets at an interval shorter than the timeout for well
	// longer than the timeout itself. A timer that is not re-armed would fire.
	for i := 0; i < 8; i++ {
		select {
		case udp.payload <- []byte{byte(i)}:
		case <-done:
			t.Fatalf("pump exited early after %d packets; idle timer was not re-armed", i)
		}
		time.Sleep(40 * time.Millisecond)
	}

	// Now go quiet: the pump must exit on the idle timeout.
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pump did not exit after going idle")
	}

	if len(written) != 8 {
		t.Fatalf("got %d frames, want 8", len(written))
	}
	for i, f := range written {
		// Each frame is the 2-byte big-endian length header plus the payload.
		if len(f) != 3 || f[0] != 0 || f[1] != 1 || f[2] != byte(i) {
			t.Fatalf("frame %d = %v, want [0 1 %d]", i, f, i)
		}
	}
}

// recordConn captures a copy of every Write, so the test can assert the
// reusable staging buffer still produces correct, independent frames.
type recordConn struct{ out *[][]byte }

func (c recordConn) Write(p []byte) (int, error) {
	*c.out = append(*c.out, append([]byte(nil), p...))
	return len(p), nil
}
func (recordConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (recordConn) Close() error                     { return nil }
func (recordConn) LocalAddr() net.Addr              { return nil }
func (recordConn) RemoteAddr() net.Addr             { return nil }
func (recordConn) SetDeadline(time.Time) error      { return nil }
func (recordConn) SetReadDeadline(time.Time) error  { return nil }
func (recordConn) SetWriteDeadline(time.Time) error { return nil }
