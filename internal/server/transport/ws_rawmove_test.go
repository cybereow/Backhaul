package transport

import (
	"bufio"
	"crypto/sha256"
	"fmt"
	"io"
	mrand "math/rand"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ageProxy stands in for a CDN that cuts a connection once it has lived maxAge:
// a transparent pipe that closes both sides at that moment. The control channel
// is left alone, so that what is under test is the flows' connections.
type ageProxy struct {
	ln     net.Listener
	target string
	maxAge time.Duration
	cuts   atomic.Int64
	// jitter spreads the cuts, as real connections do not all start together.
}

func newAgeProxy(t *testing.T, target string, maxAge time.Duration) *ageProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &ageProxy{ln: ln, target: target, maxAge: maxAge}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go p.handle(c)
		}
	}()
	return p
}

func (p *ageProxy) handle(client net.Conn) {
	server, err := net.Dial("tcp", p.target)
	if err != nil {
		client.Close()
		return
	}
	cr := bufio.NewReader(client)
	head, err := readUpgradeHead(cr)
	if err != nil {
		client.Close()
		server.Close()
		return
	}
	if _, err := server.Write(head); err != nil {
		client.Close()
		server.Close()
		return
	}
	if !strings.Contains(string(head), "/channel") {
		timer := time.AfterFunc(p.maxAge, func() {
			p.cuts.Add(1)
			client.Close()
			server.Close()
		})
		defer timer.Stop()
	}
	// A half-close travels through, as through a real proxy: closing both sides
	// at the first end of file would throw away what is still in flight the other way.
	half := func(dst, src net.Conn, from io.Reader) {
		io.Copy(dst, from)
		if tc, ok := dst.(*net.TCPConn); ok {
			tc.CloseWrite()
		} else {
			dst.Close()
		}
	}
	back := make(chan struct{})
	go func() { half(client, server, server); close(back) }()
	half(server, client, cr)
	<-back
	client.Close()
	server.Close()
}

// pacedReader yields a deterministic stream at about rate bytes a second.
type pacedReader struct {
	r    io.Reader
	rate int
}

func (p *pacedReader) Read(b []byte) (int, error) {
	if len(b) > 32<<10 {
		b = b[:32<<10]
	}
	n, err := p.r.Read(b)
	time.Sleep(time.Duration(float64(n) / float64(p.rate) * float64(time.Second)))
	return n, err
}

// longFlowDest serves a flow that sends its own stream (seeded by the byte the
// user sends first) and hashes what it receives.
type longFlowDest struct {
	size, rate int64
	mu         sync.Mutex
	got        map[byte]string
	wg         sync.WaitGroup
}

func newLongFlowDest(size, rate int64) *longFlowDest {
	return &longFlowDest{size: size, rate: rate, got: map[byte]string{}}
}

func (d *longFlowDest) handle(c net.Conn) {
	defer c.Close()
	var seed [1]byte
	if _, err := io.ReadFull(c, seed[:]); err != nil {
		return
	}
	if seed[0] == 'p' { // the harness's readiness probe
		c.Write([]byte("p"))
		return
	}
	if seed[0] == 'f' { // a short echo flow (echoFlows), running beside the long ones
		c.Write(seed[:])
		io.Copy(c, c)
		return
	}
	d.wg.Add(1)
	defer d.wg.Done()
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		io.Copy(c, &pacedReader{r: stream(int64(seed[0])+1000, d.size), rate: int(d.rate)})
		c.(*net.TCPConn).CloseWrite()
	}()
	h := sha256.New()
	io.Copy(h, c)
	d.mu.Lock()
	d.got[seed[0]] = fmt.Sprintf("%x", h.Sum(nil))
	d.mu.Unlock()
	<-sent
}

// runLongFlow runs one flow of d.size bytes each way and reports whether both
// directions arrived intact.
func runLongFlow(addr string, id byte, d *longFlowDest) error {
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return err
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(120 * time.Second))
	if _, err := c.Write([]byte{id}); err != nil {
		return err
	}
	up := make(chan struct{})
	go func() {
		defer close(up)
		io.Copy(c, &pacedReader{r: stream(int64(id), d.size), rate: int(d.rate)})
		c.(*net.TCPConn).CloseWrite()
	}()
	h := sha256.New()
	n, err := io.Copy(h, c)
	<-up
	if err != nil {
		return fmt.Errorf("download failed after %d of %d bytes: %w", n, d.size, err)
	}
	if got, want := fmt.Sprintf("%x", h.Sum(nil)), sumOf(int64(id)+1000, d.size); got != want {
		return fmt.Errorf("download differs (%d of %d bytes)", n, d.size)
	}
	return nil
}

func (d *longFlowDest) checkUpload(id byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if got := d.got[id]; got != sumOf(int64(id), d.size) {
		return fmt.Errorf("upload differs or never finished (flow %d)", id)
	}
	return nil
}

func sumOf(seed, n int64) string {
	h := sha256.New()
	io.Copy(h, stream(seed, n))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func stream(seed, n int64) io.Reader { return io.LimitReader(mrand.New(mrand.NewSource(seed)), n) }

// Control: without moves, a flow through a path that cuts connections at an age
// dies at that age. This is what the moves are for.
func TestRawFlowDiesAtTheAgeLimitWithoutMoves(t *testing.T) {
	d := newLongFlowDest(8<<20, 1<<20) // ~8 s at 1 MB/s
	var proxy *ageProxy
	addr, srv, _ := startWsE2EOpts(t, d.handle, e2eOpts{raw: true, via: func(target string) string {
		proxy = newAgeProxy(t, target, 2*time.Second)
		return proxy.Addr()
	}, maxAge: 0})
	require.Greater(t, srv.rawFlows.Load()+1, int64(0))
	err := runLongFlow(addr, 1, d)
	require.Error(t, err, "the flow survived a path that cuts every connection at 2s, without moves")
	require.Greater(t, proxy.cuts.Load(), int64(0))
}

func (p *ageProxy) Addr() string { return p.ln.Addr().String() }

// With moves, the same flow outlives the age limit many times over and arrives
// intact in both directions.
func TestRawFlowSurvivesTheAgeLimitByMoving(t *testing.T) {
	d := newLongFlowDest(12<<20, 1<<20) // ~12 s, several times the age limit
	var proxy *ageProxy
	addr, srv, _ := startWsE2EOpts(t, d.handle, e2eOpts{raw: true, maxAge: 1200 * time.Millisecond, pool: 6, via: func(target string) string {
		proxy = newAgeProxy(t, target, 2*time.Second)
		return proxy.Addr()
	}})
	require.NoError(t, runLongFlow(addr, 2, d))
	d.wg.Wait()
	require.NoError(t, d.checkUpload(2))
	t.Logf("moves: %d, connections cut by the path: %d", srv.moves.Load(), proxy.cuts.Load())
	require.Greater(t, srv.moves.Load(), int64(3), "the flow was never moved")
}

// Stress: many flows, bursty and long, through a path that cuts connections by
// age, with short flows coming and going meanwhile. Everything arrives intact,
// nothing hangs, nothing leaks.
func TestRawFlowStressManyLongFlowsAcrossAgeLimits(t *testing.T) {
	if testing.Short() {
		t.Skip("stress")
	}
	before := runtime.NumGoroutine()
	d := newLongFlowDest(6<<20, 700<<10) // ~8.5 s each
	var proxy *ageProxy
	addr, srv, _ := startWsE2EOpts(t, d.handle, e2eOpts{raw: true, maxAge: 1000 * time.Millisecond, pool: 16, via: func(target string) string {
		proxy = newAgeProxy(t, target, 1800*time.Millisecond)
		return proxy.Addr()
	}})

	long := 24
	if v, _ := strconv.Atoi(os.Getenv("RAW_TEST_LONG")); v > 0 {
		long = v
	}
	var wg sync.WaitGroup
	errs := make(chan error, long+8)
	for i := 0; i < long; i++ {
		id := byte(10 + i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- runLongFlow(addr, id, d)
		}()
	}
	// Short flows alongside, so the pool is under pressure while flows move.
	stop := make(chan struct{})
	var shortFailed, shortRun atomic.Int64
	var sw sync.WaitGroup
	shorts := 4
	if v := os.Getenv("RAW_TEST_SHORT"); v != "" {
		shorts, _ = strconv.Atoi(v)
	}
	for w := 0; w < shorts; w++ {
		sw.Add(1)
		go func() {
			defer sw.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				shortRun.Add(1)
				if echoFlows(addr, 1) != 0 {
					shortFailed.Add(1)
				}
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(90 * time.Second):
		t.Fatal("long flows hung")
	}
	close(stop)
	sw.Wait()
	close(errs)
	failed := 0
	for err := range errs {
		if err != nil {
			failed++
			t.Errorf("a long flow failed: %v", err)
		}
	}
	d.wg.Wait()
	for i := 0; i < long; i++ {
		if err := d.checkUpload(byte(10 + i)); err != nil {
			t.Error(err)
		}
	}
	t.Logf("%d long flows, %d failed; %d moves, %d cuts by the path; short flows %d run, %d failed",
		long, failed, srv.moves.Load(), proxy.cuts.Load(), shortRun.Load(), shortFailed.Load())
	require.Greater(t, srv.moves.Load(), int64(long), "barely any moves happened")
	require.Less(t, float64(shortFailed.Load()), 0.02*float64(shortRun.Load()+1), "too many short flows failed")
	require.True(t, waitFor(15*time.Second, func() bool { return runtime.NumGoroutine() < before+120 }),
		"goroutines left behind: %d now, %d before", runtime.NumGoroutine(), before)
}
