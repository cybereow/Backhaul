package poolhealth

import (
	"io"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/musix/backhaul/internal/smux"
)

// A pool that carries traffic gets a line, marked with its role and carrying the
// numbers taken from the kernel and from smux; an idle one gets none.
func TestReportDescribesABusyPool(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("TCP_INFO is only read on Linux")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err == nil {
			accepted <- c
		}
	}()
	cc, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	sc := <-accepted
	defer sc.Close()

	cfg := smux.DefaultConfig()
	cfg.Version = 2
	client, err := smux.Client(cc, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server, err := smux.Server(sc, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	go func() {
		st, err := server.AcceptStream()
		if err == nil {
			io.Copy(io.Discard, st)
		}
	}()
	st, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}

	r := New("client")
	members := []Member{{Conn: cc, Session: client, ProbeOut: 1500 * time.Millisecond}}
	now := time.Now()
	if line, ok := r.Report(now, members); ok {
		t.Fatalf("a first look has nothing to compare with, yet reported: %s", line)
	}
	if _, ok := r.Report(now.Add(time.Second), members); ok {
		t.Fatal("an idle pool was reported")
	}

	buf := make([]byte, 64<<10)
	for i := 0; i < 64; i++ { // 4 MiB
		if _, err := st.Write(buf); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(100 * time.Millisecond) // let the kernel see it acknowledged
	line, ok := r.Report(now.Add(2*time.Second), members)
	if !ok {
		t.Fatal("a pool that just moved 4 MiB was not reported")
	}
	for _, want := range []string{"pool health (client)", "1 conns", "1 streams", "out ", "held by peer's window", "session budget halts 0", "probes waiting 1 (oldest 1.5s)"} {
		if !strings.Contains(line, want) {
			t.Errorf("line lacks %q:\n%s", want, line)
		}
	}
}
