package cmd

import (
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/musix/backhaul/internal/utils/handlers"
	"github.com/xtaci/smux"
)

// benchEnvelope is benchStreamWindow (streamwindow_bench_test.go) with the flow
// wrapped, or not, in the half-close envelope on both ends (plan 024). chunk is the
// application write size (64KB is what the handler's pooled copy uses on the
// server's upload hop).
// newLeg is the tunnel leg: the 30ms-RTT in-memory leg, or an undelayed pipe
// where CPU cost is all that shows.
func benchEnvelope(b *testing.B, streams, chunk int, envelope bool, newLeg func() (net.Conn, net.Conn, func())) {
	const perStream = 8 << 20

	cfg := smux.DefaultConfig()
	cfg.Version = 2
	cfg.MaxFrameSize = defaultMaxFrameSize
	cfg.MaxReceiveBuffer = defaultMaxReceiveBuffer
	cfg.MaxStreamBuffer = deriveStreamBuffer(defaultMaxReceiveBuffer, defaultMuxCon)
	if err := smux.VerifyConfig(cfg); err != nil {
		b.Fatalf("config: %v", err)
	}
	wrap := func(c net.Conn) net.Conn {
		if envelope {
			return handlers.NewHalfCloseConn(c)
		}
		return c
	}

	b.SetBytes(int64(perStream) * int64(streams))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		srvLeg, cliLeg, stopLeg := newLeg()
		srvSess, err := smux.Client(srvLeg, cfg)
		if err != nil {
			b.Fatal(err)
		}
		cliSess, err := smux.Server(cliLeg, cfg)
		if err != nil {
			b.Fatal(err)
		}

		srvStreams := make([]net.Conn, streams)
		for j := range srvStreams {
			st, err := srvSess.OpenStream()
			if err != nil {
				b.Fatal(err)
			}
			srvStreams[j] = wrap(st)
		}
		accepted := make(chan net.Conn, streams)
		go func() {
			for j := 0; j < streams; j++ {
				st, err := cliSess.AcceptStream()
				if err != nil {
					return
				}
				accepted <- wrap(st)
			}
		}()
		drained := make(chan struct{}, streams)
		for j := 0; j < streams; j++ {
			go func(st net.Conn) {
				_, _ = io.CopyBuffer(io.Discard, io.LimitReader(st, perStream), make([]byte, 64<<10))
				drained <- struct{}{}
			}(<-accepted)
		}

		payload := make([]byte, chunk)
		b.StartTimer()
		for j := 0; j < streams; j++ {
			go func(st net.Conn) {
				for sent := 0; sent < perStream; {
					n := min(len(payload), perStream-sent)
					w, err := st.Write(payload[:n])
					if err != nil {
						return
					}
					sent += w
				}
			}(srvStreams[j])
		}
		for j := 0; j < streams; j++ {
			<-drained
		}
		b.StopTimer()
		srvSess.Close()
		cliSess.Close()
		stopLeg()
		b.StartTimer()
	}
}

func pipeLeg() (net.Conn, net.Conn, func()) {
	a, b := net.Pipe()
	return a, b, func() { a.Close(); b.Close() }
}

// BenchmarkHalfCloseEnvelope compares a plain flow on the legacy smux stream
// against the same flow in the half-close envelope, on the 30ms-RTT leg of
// BenchmarkStreamWindow and on an undelayed pipe.
//
// Read the chunk dimension before comparing: with the 30ms leg and the default
// 512KB stream window, smux's half-window update rule makes throughput depend on
// whether the writes keep the byte stream aligned to the frame size. Aligned 64KB
// writes (a multiple of the 32KB frame) are a knife-edge best case for LEGACY
// (window/RTT); any other size drops legacy itself to about half of that, and the
// envelope's 3 header bytes per record always lands in the second regime. Real
// traffic is written in socket-read-sized chunks, so "unaligned" is the fair
// comparison; "aligned" mostly measures that phase effect.
func BenchmarkHalfCloseEnvelope(b *testing.B) {
	for _, leg := range []struct {
		name string
		mk   func() (net.Conn, net.Conn, func())
	}{{"rtt30ms", delayLeg}, {"nodelay", pipeLeg}} {
		for _, streams := range []int{1, 8} {
			for _, ch := range []struct {
				name string
				n    int
			}{{"aligned64k", 64 << 10}, {"unaligned50000", 50000}} {
				for _, envelope := range []bool{false, true} {
					name := "legacy"
					if envelope {
						name = "envelope"
					}
					b.Run(fmt.Sprintf("%s/streams=%d/%s/%s", leg.name, streams, ch.name, name), func(b *testing.B) {
						benchEnvelope(b, streams, ch.n, envelope, leg.mk)
					})
				}
			}
		}
	}
}
