package transport

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/musix/backhaul/internal/utils/network"
	"github.com/xtaci/smux"
)

// countingConn counts real read syscalls on a real socket.
type countingConn struct {
	net.Conn
	reads int64
}

func (c *countingConn) Read(p []byte) (int, error) {
	atomic.AddInt64(&c.reads, 1)
	return c.Conn.Read(p)
}

// benchSmuxRead measures wsmux's server-side receive path. smux collects every
// frame header with io.ReadFull(conn, hdr[:8]), so on an unbuffered socket each
// frame costs a syscall for its 8-byte header before the payload read.
//
// wrapped=true is the production handoff after the pipelined-bytes fix:
// smux reads through the WebSocketConn's shared buffer. This checks that
// correctness fix does not cost throughput.
func benchSmuxRead(b *testing.B, wrapped bool) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	const chunk = 32 * 1024
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		accepted <- c
	}()

	dialed, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	defer dialed.Close()

	srvRaw := <-accepted
	defer srvRaw.Close()

	counted := &countingConn{Conn: srvRaw}
	var recvSide net.Conn = counted
	if wrapped {
		recvSide = network.NewWebSocketConn(counted, ws.StateServerSide, nil).NetConn()
	}

	cfg := smux.DefaultConfig()
	cfg.Version = 2
	cfg.MaxFrameSize = 32768
	cfg.KeepAliveDisabled = true

	recv, err := smux.Client(recvSide, cfg)
	if err != nil {
		b.Fatalf("smux client: %v", err)
	}
	send, err := smux.Server(dialed, cfg)
	if err != nil {
		b.Fatalf("smux server: %v", err)
	}

	payload := make([]byte, chunk)
	go func() {
		st, err := send.OpenStream()
		if err != nil {
			return
		}
		for {
			if _, err := st.Write(payload); err != nil {
				return
			}
		}
	}()

	stream, err := recv.AcceptStream()
	if err != nil {
		b.Fatalf("accept stream: %v", err)
	}
	_ = stream.SetReadDeadline(time.Now().Add(60 * time.Second))

	sink := make([]byte, chunk)

	b.SetBytes(chunk)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := io.ReadFull(stream, sink); err != nil {
			b.Fatalf("read: %v", err)
		}
	}

	b.StopTimer()
	b.ReportMetric(float64(atomic.LoadInt64(&counted.reads))/float64(b.N), "reads/chunk")
	recv.Close()
	send.Close()
}

func BenchmarkSmuxReadRawConn(b *testing.B)     { benchSmuxRead(b, false) }
func BenchmarkSmuxReadWrappedConn(b *testing.B) { benchSmuxRead(b, true) }
