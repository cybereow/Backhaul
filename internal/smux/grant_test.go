package smux

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func grantPair(t *testing.T, session, stream int) (cli, srv *Session) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Version = 2
	cfg.MaxReceiveBuffer = session
	cfg.MaxStreamBuffer = stream
	a, b := net.Pipe()
	cli, err := Client(a, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cli.Close() })
	srv, err = Server(b, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	return cli, srv
}

// Streams whose readers stop must not use up the session's receive budget: the
// connection has to stay readable, so that a new stream can still be opened and
// used while they are stuck. (Before the budget was shared among the streams,
// four full streams halted the whole session.)
func TestStalledStreamsDoNotHaltTheSession(t *testing.T) {
	const session, stream = 4 << 20, 1 << 20
	cli, srv := grantPair(t, session, stream)

	// Eight streams, each read once (which announces its window) and then never
	// again, while their writers send all they are allowed to.
	for i := 0; i < 8; i++ {
		w, err := cli.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte{1}); err != nil {
			t.Fatal(err)
		}
		r, err := srv.AcceptStream()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(r, make([]byte, 1)); err != nil {
			t.Fatal(err)
		}
		go func() {
			buf := make([]byte, 64<<10)
			for {
				if _, err := w.Write(buf); err != nil {
					return
				}
			}
		}()
	}
	time.Sleep(time.Second) // every writer has run into its window by now

	// The session is still being read: a new stream gets through at once.
	done := make(chan error, 1)
	go func() {
		w, err := cli.OpenStream()
		if err != nil {
			done <- err
			return
		}
		if _, err := w.Write([]byte("ping")); err != nil {
			done <- err
			return
		}
		r, err := srv.AcceptStream()
		if err != nil {
			done <- err
			return
		}
		_, err = io.ReadFull(r, make([]byte, 4))
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a new stream did not get through: the session stopped being read")
	}

	if n, d := srv.BudgetHalts(); n != 0 {
		t.Fatalf("the read loop halted on the budget %d time(s), %v in all", n, d)
	}
	srv.grantMu.Lock()
	sum := srv.grantSum
	srv.grantMu.Unlock()
	if sum > session {
		t.Fatalf("%d bytes granted out of a %d byte budget", sum, session)
	}
}

// A stream that has the session to itself gets its whole configured window, as
// before: sharing only starts when the streams carrying data would not all fit.
func TestLoneStreamKeepsItsFullWindow(t *testing.T) {
	const session, stream = 8 << 20, 2 << 20
	cli, srv := grantPair(t, session, stream)
	w, err := cli.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(make([]byte, 64<<10)); err != nil {
		t.Fatal(err)
	}
	r, err := srv.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(r, make([]byte, 64<<10)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadUint32(&w.peerWindow) != stream {
		if time.Now().After(deadline) {
			t.Fatalf("the writer was told a window of %d, want the configured %d", atomic.LoadUint32(&w.peerWindow), stream)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// When more streams carry data than fit, they share the budget; and when the
// streams end, everything granted is given back.
func TestBusyStreamsShareTheBudgetAndGiveItBack(t *testing.T) {
	const session, stream, n = 4 << 20, 2 << 20, 8
	cli, srv := grantPair(t, session, stream)
	var readers, writers []*Stream
	for i := 0; i < n; i++ {
		w, err := cli.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte{1}); err != nil {
			t.Fatal(err)
		}
		r, err := srv.AcceptStream()
		if err != nil {
			t.Fatal(err)
		}
		readers, writers = append(readers, r), append(writers, w)
		go io.Copy(io.Discard, r)
		go func() {
			buf := make([]byte, 32<<10)
			for {
				if _, err := w.Write(buf); err != nil {
					return
				}
			}
		}()
	}
	time.Sleep(3 * grantTick) // the streams are counted and their windows settle

	budget := int64(session) * grantShareNum / grantShareDen
	srv.grantMu.Lock()
	sum := srv.grantSum
	srv.grantMu.Unlock()
	if sum > budget+n*grantFloor {
		t.Fatalf("%d bytes granted to %d busy streams out of a %d byte budget", sum, n, budget)
	}
	for i, w := range writers {
		if win := atomic.LoadUint32(&w.peerWindow); win >= stream {
			t.Fatalf("stream %d still has the full window (%d) with %d streams sharing %d", i, win, n, budget)
		}
	}
	if halts, _ := srv.BudgetHalts(); halts != 0 {
		t.Fatalf("the read loop halted on the budget %d time(s)", halts)
	}

	for i := range readers {
		writers[i].Close()
		readers[i].Close()
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		srv.grantMu.Lock()
		sum = srv.grantSum
		srv.grantMu.Unlock()
		if sum == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d bytes are still granted after every stream was closed", sum)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A stream that has gone quiet gives its window back, and gets it again when it
// carries data once more.
func TestIdleStreamGivesItsWindowBack(t *testing.T) {
	const session, stream = 8 << 20, 2 << 20
	cli, srv := grantPair(t, session, stream)
	w, err := cli.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	send := func() {
		t.Helper()
		if _, err := w.Write(make([]byte, 512<<10)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	r, err := srv.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	go io.Copy(io.Discard, r)
	send()
	window := func() uint32 { return atomic.LoadUint32(&w.peerWindow) }
	waitFor := func(what string, d time.Duration, ok func() bool) {
		t.Helper()
		end := time.Now().Add(d)
		for !ok() {
			if time.Now().After(end) {
				t.Fatalf("%s (window %d)", what, window())
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	waitFor("the busy stream never got its full window", 2*time.Second, func() bool { return window() == stream })
	waitFor("the idle stream kept its window", idleReclaim+4*grantTick, func() bool { return window() == initialPeerWindow })
	srv.grantMu.Lock()
	sum := srv.grantSum
	srv.grantMu.Unlock()
	if sum != initialPeerWindow {
		t.Fatalf("an idle stream is still granted %d bytes", sum)
	}
	send()
	waitFor("the stream did not get its window again once it carried data", 2*time.Second, func() bool { return window() == stream })
}
