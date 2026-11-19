package transport

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// wafProxy stands in for a CDN or WAF in front of the tunnel: it forwards the
// HTTP upgrade, and from then on - when strict - reads every byte as a WebSocket
// frame and kills the connection at the first one that is not valid, which is
// what happens to raw bytes on such a path. Not strict, it is a plain pipe. The
// mode is taken per connection, when the upgrade completes.
type wafProxy struct {
	ln     net.Listener
	target string
	strict atomic.Bool
	killed atomic.Int64 // connections killed for carrying invalid frames
}

func newWAFProxy(t *testing.T, target string, strict bool) *wafProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &wafProxy{ln: ln, target: target}
	p.strict.Store(strict)
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

func (p *wafProxy) Addr() string { return p.ln.Addr().String() }

func readUpgradeHead(r *bufio.Reader) ([]byte, error) {
	var head []byte
	for {
		line, err := r.ReadBytes('\n')
		head = append(head, line...)
		if err != nil {
			return head, err
		}
		if strings.TrimRight(string(line), "\r\n") == "" {
			return head, nil
		}
	}
}

func (p *wafProxy) handle(client net.Conn) {
	server, err := net.Dial("tcp", p.target)
	if err != nil {
		client.Close()
		return
	}
	closeBoth := func() { client.Close(); server.Close() }
	cr, sr := bufio.NewReader(client), bufio.NewReader(server)
	req, err := readUpgradeHead(cr)
	if err != nil {
		closeBoth()
		return
	}
	if _, err := server.Write(req); err != nil {
		closeBoth()
		return
	}
	resp, err := readUpgradeHead(sr)
	if err != nil {
		closeBoth()
		return
	}
	if _, err := client.Write(resp); err != nil {
		closeBoth()
		return
	}
	if !strings.Contains(string(resp), "101") || !p.strict.Load() {
		// A half-close travels through, as through a real proxy.
		half := func(dst net.Conn, src io.Reader) {
			io.Copy(dst, src)
			if tc, ok := dst.(*net.TCPConn); ok {
				tc.CloseWrite()
			} else {
				dst.Close()
			}
		}
		back := make(chan struct{})
		go func() { half(server, cr); close(back) }()
		half(client, sr)
		<-back
		closeBoth()
		return
	}
	go func() {
		if err := p.frames(cr, server, true); err != nil && err != io.EOF {
			p.killed.Add(1)
		}
		closeBoth()
	}()
	if err := p.frames(sr, client, false); err != nil && err != io.EOF {
		p.killed.Add(1)
	}
	closeBoth()
}

// frames forwards WebSocket frames from src to dst and fails on the first byte
// sequence that is not a valid frame header. A client's frames must be masked,
// a server's must not be.
func (p *wafProxy) frames(src *bufio.Reader, dst net.Conn, masked bool) error {
	for {
		b0, err := src.ReadByte()
		if err != nil {
			return err
		}
		b1, err := src.ReadByte()
		if err != nil {
			return err
		}
		switch b0 & 0x0f {
		case 0, 1, 2, 8, 9, 10:
		default:
			return fmt.Errorf("invalid opcode %#x", b0&0x0f)
		}
		if b0&0x70 != 0 {
			return fmt.Errorf("reserved bits set in %#x", b0)
		}
		if (b1&0x80 != 0) != masked {
			return fmt.Errorf("mask bit %v, want %v", b1&0x80 != 0, masked)
		}
		hdr := []byte{b0, b1}
		length := uint64(b1 & 0x7f)
		switch length {
		case 126:
			ext := make([]byte, 2)
			if _, err := io.ReadFull(src, ext); err != nil {
				return err
			}
			hdr = append(hdr, ext...)
			length = uint64(binary.BigEndian.Uint16(ext))
		case 127:
			ext := make([]byte, 8)
			if _, err := io.ReadFull(src, ext); err != nil {
				return err
			}
			hdr = append(hdr, ext...)
			length = binary.BigEndian.Uint64(ext)
		}
		if length > 1<<30 {
			return fmt.Errorf("absurd frame length %d", length)
		}
		if masked {
			key := make([]byte, 4)
			if _, err := io.ReadFull(src, key); err != nil {
				return err
			}
			hdr = append(hdr, key...)
		}
		if _, err := dst.Write(hdr); err != nil {
			return err
		}
		if _, err := io.CopyN(dst, src, int64(length)); err != nil {
			return err
		}
	}
}

// echoFlows runs n sequential echo flows and returns how many failed.
func echoFlows(addr string, n int) (failed int) {
	failed, _ = echoFlowsErr(addr, n)
	return
}

// echoFlowsErr is echoFlows that also returns the first error.
func echoFlowsErr(addr string, n int) (failed int, first error) {
	fail := func(err error) {
		failed++
		if first == nil {
			first = err
		}
	}
	for i := 0; i < n; i++ {
		c, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			fail(err)
			continue
		}
		c.SetDeadline(time.Now().Add(6 * time.Second))
		msg := []byte(fmt.Sprintf("flow-%d", i))
		if _, err := c.Write(msg); err != nil {
			fail(err)
			c.Close()
			continue
		}
		got := make([]byte, len(msg))
		if _, err := io.ReadFull(c, got); err != nil {
			fail(fmt.Errorf("flow %d: %w", i, err))
		} else if string(got) != string(msg) {
			fail(fmt.Errorf("flow %d: echoed %q", i, got))
		}
		c.Close()
	}
	return
}

func waitFor(d time.Duration, cond func() bool) bool {
	for deadline := time.Now().Add(d); time.Now().Before(deadline); time.Sleep(25 * time.Millisecond) {
		if cond() {
			return true
		}
	}
	return cond()
}

func viaProxy(p **wafProxy, strict bool, t *testing.T) func(string) string {
	return func(target string) string {
		*p = newWAFProxy(t, target, strict)
		return (*p).Addr()
	}
}

// Through a path that passes raw bytes the probe succeeds, and flows go raw.
func TestRawFlowsUsedWhenThePathCarriesThem(t *testing.T) {
	var proxy *wafProxy
	addr, srv, _ := startWsE2EOpts(t, echoDest, e2eOpts{raw: true, via: viaProxy(&proxy, false, t)})
	require.Zero(t, echoFlows(addr, 20))
	require.Greater(t, srv.rawFlows.Load(), int64(0), "no flow was raw although the path carries raw bytes")
}

// Through a path that reads the bytes after the upgrade as frames, raw flows
// would all break. The probe shows that, and every flow is framed instead.
func TestRawFallsBackThroughAStrictPath(t *testing.T) {
	var proxy *wafProxy
	addr, srv, _ := startWsE2EOpts(t, echoDest, e2eOpts{raw: true, skipRawVerified: true, via: viaProxy(&proxy, true, t)})
	// Give the client several probe rounds.
	require.True(t, waitFor(5*time.Second, func() bool { return proxy.killed.Load() >= 2 }), "the strict path never saw a probe")
	raw, _ := srv.rawMode()
	require.False(t, raw, "the server trusts raw flows through a path that refuses them")
	require.Zero(t, echoFlows(addr, 40), "flows must still be served, framed")
	require.Zero(t, srv.rawFlows.Load(), "a raw flow was sent through a strict path")
	require.Greater(t, srv.framedFlows.Load(), int64(0))
}

// A client that never probes (an older build) is never sent a raw flow.
func TestRawNotSentToAClientThatDoesNotProbe(t *testing.T) {
	addr, srv, _ := startWsE2EOpts(t, echoDest, e2eOpts{raw: true, probeEvery: -1})
	time.Sleep(500 * time.Millisecond)
	require.Zero(t, echoFlows(addr, 20))
	require.Zero(t, srv.rawFlows.Load())
}

// A path that starts refusing raw bytes is noticed by the next probe, and flows
// are framed from then on; one that starts carrying them is trusted again.
func TestRawFollowsThePathAsItChanges(t *testing.T) {
	var proxy *wafProxy
	addr, srv, _ := startWsE2EOpts(t, echoDest, e2eOpts{raw: true, via: viaProxy(&proxy, false, t), probeTTL: 2 * time.Second})
	require.Zero(t, echoFlows(addr, 10))
	require.Greater(t, srv.rawFlows.Load(), int64(0))

	proxy.strict.Store(true)
	require.True(t, waitFor(6*time.Second, func() bool { raw, _ := srv.rawMode(); return !raw }), "the server kept trusting a path that refuses raw bytes")
	rawBefore := srv.rawFlows.Load()
	// The connections made while it was transparent keep working, new ones are
	// framed: flows succeed either way once the verdict has flipped.
	failed, first := echoFlowsErr(addr, 30)
	require.Zero(t, failed, "first error: %v (raw flows %d, framed %d, killed by the strict path %d)", first, srv.rawFlows.Load(), srv.framedFlows.Load(), proxy.killed.Load())
	require.Equal(t, rawBefore, srv.rawFlows.Load(), "raw flows were sent after the verdict flipped")

	proxy.strict.Store(false)
	require.True(t, waitFor(8*time.Second, func() bool { raw, _ := srv.rawMode(); return raw }), "raw flows were never trusted again")
	failed, first = echoFlowsErr(addr, 20)
	require.Zero(t, failed, "after the path recovered, first error: %v (raw %d, framed %d, killed %d)", first, srv.rawFlows.Load(), srv.framedFlows.Load(), proxy.killed.Load())
	require.Greater(t, srv.rawFlows.Load(), rawBefore)
}

func TestRawModeTrustExpires(t *testing.T) {
	s := &WsTransport{config: &WsConfig{Raw: true, RawProbeTTL: 100 * time.Millisecond}}
	if raw, _ := s.rawMode(); raw {
		t.Fatal("trusted before any probe")
	}
	s.rawOKAt.Store(time.Now().UnixNano())
	s.rawCaps.Store(1)
	if raw, caps := s.rawMode(); !raw || caps != 1 {
		t.Fatalf("not trusted right after a good probe: %v %d", raw, caps)
	}
	s.rawBadAt.Store(time.Now().UnixNano() + 1)
	if raw, _ := s.rawMode(); raw {
		t.Fatal("trusted after a later bad probe")
	}
	s.rawBadAt.Store(0)
	time.Sleep(150 * time.Millisecond)
	if raw, _ := s.rawMode(); raw {
		t.Fatal("trusted after the probe expired")
	}
	off := &WsTransport{config: &WsConfig{Raw: false}}
	off.rawOKAt.Store(time.Now().UnixNano())
	if raw, _ := off.rawMode(); raw {
		t.Fatal("raw although the option is off")
	}
}

// Stress: the path flips between carrying and refusing raw bytes while many
// flows run. Flows that were raw when the path turned strict may break (nothing
// can save a raw flow whose connection a middlebox kills), but nothing may hang,
// leak or panic, and once the path settles every flow is served.
func TestRawFallbackStressWhilePathFlaps(t *testing.T) {
	if testing.Short() {
		t.Skip("stress")
	}
	var proxy *wafProxy
	goroutinesBefore := runtime.NumGoroutine()
	addr, srv, _ := startWsE2EOpts(t, echoDest, e2eOpts{raw: true, via: viaProxy(&proxy, false, t), probeTTL: time.Second})

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var attempted, failed atomic.Int64
	for w := 0; w < 12; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				attempted.Add(1)
				if echoFlows(addr, 1) != 0 {
					failed.Add(1)
				}
			}
		}()
	}
	for i := 0; i < 16; i++ {
		proxy.strict.Store(i%2 == 0)
		time.Sleep(700 * time.Millisecond)
	}
	proxy.strict.Store(false)
	close(stop)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("a flow hung while the path was flapping")
	}
	t.Logf("flows: %d attempted, %d failed while flapping; %d raw, %d framed",
		attempted.Load(), failed.Load(), srv.rawFlows.Load(), srv.framedFlows.Load())
	require.Greater(t, attempted.Load(), int64(100), "the load generator barely ran")

	// Settled on a path that carries raw bytes: everything is served again.
	require.True(t, waitFor(8*time.Second, func() bool { raw, _ := srv.rawMode(); return raw }))
	require.Zero(t, echoFlows(addr, 60), "flows fail after the path settled")

	// No goroutines left behind by the flapping (allow the pair's own steady ones).
	require.True(t, waitFor(10*time.Second, func() bool { return runtime.NumGoroutine() < goroutinesBefore+80 }),
		"goroutines kept growing: %d now, %d before", runtime.NumGoroutine(), goroutinesBefore)
}

// The first flows after start-up are raw: the pool is filled only once the
// first probe has an answer, so no connection in it is left unmarked at the
// front of the queue, where it would carry framed flows for as long as it sat.
func TestRawFirstFlowsAreRawRightAfterStart(t *testing.T) {
	addr, srv, _ := startWsE2EOpts(t, echoDest, e2eOpts{raw: true, pool: 12})
	require.Zero(t, srv.framedFlows.Load(), "flows before the test's own were framed")
	require.Zero(t, echoFlows(addr, 12))
	require.Zero(t, srv.framedFlows.Load(), "a flow right after start-up was framed")
	require.GreaterOrEqual(t, srv.rawFlows.Load(), int64(12))
}
