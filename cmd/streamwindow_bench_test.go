package cmd

import (
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/xtaci/smux"
)

// --- a duplex net.Conn pair with a fixed one-way delay ----------------------
//
// A tunnel leg between two continents is a high-RTT path, and that is the whole
// point of deriveStreamBuffer: smux's per-stream window only bites when a window
// update costs a round trip. A loopback benchmark cannot show the effect at all,
// so the leg here delivers each write to its peer one legDelay later.

type delayedChunk struct {
	at   time.Time
	data []byte
}

// leg is the shared lifetime of one in-memory tunnel leg. Closing either end
// stops the delivery workers AND unblocks both ends' pending Read/Write, which
// a real socket does too: smux.Session.Close closes the underlying conn and
// then expects its recvLoop's blocked Read to return. Without that, every
// benchmark iteration would strand two recvLoop goroutines (and the large
// buffered channels they hold) on a channel receive that never completes -
// a growing live heap across iterations, and GC work charged to later ones.
type leg struct {
	done chan struct{}
	once sync.Once
}

func (l *leg) stop() { l.once.Do(func() { close(l.done) }) }

// delayConn is one end of an in-memory tunnel leg with a fixed one-way delay.
//
// Delivery MUST stay in order: smux is a framed protocol over a byte stream, so
// a single reordered write desynchronises the session and the whole thing
// stalls. A goroutine-per-write with a sleep looks like it delays each chunk
// equally but does not preserve order under load, so the delay runs through one
// FIFO scheduler goroutine per direction instead.
type delayConn struct {
	leg     *leg
	queue   chan delayedChunk
	in      chan []byte
	pending []byte
}

func (d *delayConn) Read(p []byte) (int, error) {
	for len(d.pending) == 0 {
		select {
		case <-d.leg.done:
			return 0, io.EOF
		case b, ok := <-d.in:
			if !ok {
				return 0, io.EOF
			}
			d.pending = b
		}
	}
	n := copy(p, d.pending)
	d.pending = d.pending[n:]
	return n, nil
}

func (d *delayConn) Write(p []byte) (int, error) {
	b := make([]byte, len(p))
	copy(b, p)
	select {
	case <-d.leg.done:
		return 0, net.ErrClosed
	case d.queue <- delayedChunk{at: time.Now().Add(legDelay), data: b}:
		return len(p), nil
	}
}

func (d *delayConn) Close() error                     { d.leg.stop(); return nil }
func (d *delayConn) LocalAddr() net.Addr              { return delayAddr{} }
func (d *delayConn) RemoteAddr() net.Addr             { return delayAddr{} }
func (d *delayConn) SetDeadline(time.Time) error      { return nil }
func (d *delayConn) SetReadDeadline(time.Time) error  { return nil }
func (d *delayConn) SetWriteDeadline(time.Time) error { return nil }

type delayAddr struct{}

func (delayAddr) Network() string { return "delay" }
func (delayAddr) String() string  { return "delay" }

// legDelay is the one-way delay of the in-memory leg: half of a 30ms tunnel
// round-trip, which is a realistic intercontinental CDN hop.
const legDelay = 15 * time.Millisecond

// deliver drains queue in FIFO order, releasing each chunk once its delay has
// elapsed. One goroutine per direction, so order is preserved by construction.
func deliver(queue <-chan delayedChunk, out chan<- []byte, done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		case c := <-queue:
			if wait := time.Until(c.at); wait > 0 {
				select {
				case <-done:
					return
				case <-time.After(wait):
				}
			}
			select {
			case <-done:
				return
			case out <- c.data:
			}
		}
	}
}

// delayLeg returns the two ends of one tunnel leg plus a stop func. Closing
// either end stops the leg too, so a session teardown is enough on its own;
// the returned func makes the benchmark's cleanup explicit and is idempotent.
func delayLeg() (net.Conn, net.Conn, func()) {
	a2b := make(chan []byte, 8192)
	b2a := make(chan []byte, 8192)
	qa := make(chan delayedChunk, 8192)
	qb := make(chan delayedChunk, 8192)
	l := &leg{done: make(chan struct{})}

	a := &delayConn{leg: l, queue: qa, in: b2a}
	b := &delayConn{leg: l, queue: qb, in: a2b}
	go deliver(qa, a2b, l.done)
	go deliver(qb, b2a, l.done)

	return a, b, l.stop
}

// benchStreamWindow moves data server -> client (the direction carrying the
// user's upload) over one tunnel leg at a realistic RTT, with the given
// per-stream window. Throughput is what MB/s reports.
func benchStreamWindow(b *testing.B, streamBuf int, streams int) {
	const perStream = 4 << 20

	cfg := smux.DefaultConfig()
	cfg.Version = 2
	cfg.MaxFrameSize = defaultMaxFrameSize
	cfg.MaxReceiveBuffer = defaultMaxReceiveBuffer
	cfg.MaxStreamBuffer = streamBuf
	if err := smux.VerifyConfig(cfg); err != nil {
		b.Fatalf("config: %v", err)
	}

	b.SetBytes(int64(perStream) * int64(streams))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		srvLeg, cliLeg, stopLeg := delayLeg()
		srvSess, err := smux.Client(srvLeg, cfg) // the backhaul server opens streams
		if err != nil {
			b.Fatal(err)
		}
		cliSess, err := smux.Server(cliLeg, cfg)
		if err != nil {
			b.Fatal(err)
		}

		srvStreams := make([]*smux.Stream, streams)
		for j := range srvStreams {
			if srvStreams[j], err = srvSess.OpenStream(); err != nil {
				b.Fatal(err)
			}
		}
		accepted := make(chan *smux.Stream, streams)
		go func() {
			for j := 0; j < streams; j++ {
				st, err := cliSess.AcceptStream()
				if err != nil {
					return
				}
				accepted <- st
			}
		}()
		drained := make(chan struct{}, streams)
		for j := 0; j < streams; j++ {
			go func(st *smux.Stream) {
				_, _ = io.CopyBuffer(io.Discard, io.LimitReader(st, perStream), make([]byte, 64<<10))
				drained <- struct{}{}
			}(<-accepted)
		}

		payload := make([]byte, 256<<10)
		b.StartTimer()
		for j := 0; j < streams; j++ {
			go func(st *smux.Stream) {
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

// BenchmarkStreamWindow is the evidence for deriveStreamBuffer: the same tunnel
// leg at 30ms RTT, once with the per-stream window this release used to pin
// (64KB) and once with the window now derived from the session budget
// (4MB/8 = 512KB).
func BenchmarkStreamWindow(b *testing.B) {
	derived := deriveStreamBuffer(defaultMaxReceiveBuffer, defaultMuxCon)
	for _, streams := range []int{1, 8} {
		for _, w := range []struct {
			name string
			size int
		}{
			{"old_fixed_64KB", minMaxStreamBuffer},
			{"derived", derived},
		} {
			b.Run(fmt.Sprintf("streams=%d/%s", streams, w.name), func(b *testing.B) {
				benchStreamWindow(b, w.size, streams)
			})
		}
	}
}
