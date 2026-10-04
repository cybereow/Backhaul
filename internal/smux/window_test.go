package smux

import (
	"io"
	"net"
	"testing"
	"time"
)

// A stream's receive window is what the peer may have outstanding on it: the
// peer learns of a change at once, sends no more than the window while nothing
// is read, and goes on as it is read or raised.
func TestSetReceiveWindow(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Version = 2
	cfg.MaxReceiveBuffer = 8 << 20
	cfg.MaxStreamBuffer = 4 << 20
	a, b := net.Pipe()
	cli, err := Client(a, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	srv, err := Server(b, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	w, err := cli.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	r, err := srv.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}

	// sent is how much of 1 MiB the writer gets out before it has to wait.
	sent := func() int {
		t.Helper()
		_ = w.SetWriteDeadline(time.Now().Add(300 * time.Millisecond))
		n, err := w.Write(make([]byte, 1<<20))
		if err != ErrTimeout {
			t.Fatalf("Write = %d, %v; want it to wait on the window", n, err)
		}
		return n
	}

	const small, large = 64 << 10, 256 << 10
	if err := r.SetReceiveWindow(small); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // let the update reach the writer
	if n := sent(); n != small {
		t.Fatalf("writer sent %d bytes into a %d window", n, small)
	}
	if n := sent(); n != 0 {
		t.Fatalf("writer sent %d more bytes with the window full", n)
	}
	if _, err := io.ReadFull(r, make([]byte, small)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if n := sent(); n != small {
		t.Fatalf("writer sent %d bytes after the window was read, want %d", n, small)
	}

	// raising it lets the writer go on without anything being read
	if err := r.SetReceiveWindow(large); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if n := sent(); n != large-small {
		t.Fatalf("writer sent %d bytes after the window grew from %d to %d", n, small, large)
	}

	// and it is bounded by the session's stream buffer
	if err := r.SetReceiveWindow(1 << 30); err != nil {
		t.Fatal(err)
	}
	if got := r.receiveWindow(); got != uint32(cfg.MaxStreamBuffer) {
		t.Fatalf("window %d, want it capped at %d", got, cfg.MaxStreamBuffer)
	}
}
