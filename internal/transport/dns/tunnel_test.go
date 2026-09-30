package dnsx

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/musix/backhaul/internal/transport/dns/rel"
	"github.com/musix/backhaul/internal/transport/dns/sel"
	"github.com/sirupsen/logrus"
)

func newTestLogger() *logrus.Logger {
	l := logrus.New()
	l.SetLevel(logrus.TraceLevel)
	return l
}

// tunnelPayload is the bytes moved per direction by the transfer tests. The
// carrier is slow by design (a ~100 B query MSS, one exchange in flight), and
// -race multiplies that, so the default is small; set DNS_SLOW_TESTS=1 for the
// 256 KiB soak.
func tunnelPayload() int {
	if os.Getenv("DNS_SLOW_TESTS") != "" {
		return 256 * 1024
	}
	return 32 * 1024
}

func payload(seed int64, n int) []byte {
	b := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(b)
	return b
}

type lossyConn struct {
	net.PacketConn
	loss float64
	dup  float64
	rng  *rand.Rand
	mu   sync.Mutex
}

func (c *lossyConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	c.mu.Lock()
	l := c.loss
	d := c.dup
	r := c.rng.Float64()
	d_r := c.rng.Float64()
	c.mu.Unlock()

	if r < l {
		return len(p), nil
	}
	n, err = c.PacketConn.WriteTo(p, addr)
	if d_r < d {
		_, _ = c.PacketConn.WriteTo(p, addr)
	}
	return n, err
}

func newLossyTestResolver(resp *Responder, loss, dup float64) (addr string, stop func(), err error) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}

	lp := &lossyConn{
		PacketConn: pc,
		loss:       loss,
		dup:        dup,
		rng:        rand.New(rand.NewSource(42)),
	}

	srv := &dns.Server{PacketConn: lp, Handler: dns.HandlerFunc(resp.handle)}
	go func() { _ = srv.ActivateAndServe() }()
	return pc.LocalAddr().String(), func() { _ = srv.Shutdown() }, nil
}

func TestTunnel256KiB(t *testing.T) {
	domain := "tunnel.example.com"
	key := "secret"
	logger := newTestLogger()

	srv := NewServer(domain, key, logger)
	srv.relCfg = rel.Config{MinRTO: 20 * time.Millisecond, MaxRTO: 1 * time.Second}
	addr, stop, err := newTestResolver(srv.responder)
	if err != nil {
		t.Fatalf("newTestResolver: %v", err)
	}
	defer stop()
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)

	testPayload := tunnelPayload()
	srvUp := payload(1, testPayload)

	go func() {
		defer wg.Done()
		conn, err := srv.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		_, err = io.Copy(conn, conn)
		if err != nil && err != io.EOF {
			t.Errorf("server echo: %v", err)
		}
	}()

	profiles := []sel.Profile{
		{Resolver: addr, RRType: 16, Transport: "udp", Cap: 700}, // TXT
	}

	cli, err := Dial(ctx, DialParams{
		Domain:   domain,
		Key:      key,
		Profiles: profiles,
		Timeout:  5 * time.Second,
		Rel:      rel.Config{MinRTO: 20 * time.Millisecond, MaxRTO: 1 * time.Second},
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	var cliWg sync.WaitGroup
	cliWg.Add(2)

	go func() {
		defer cliWg.Done()
		_, err := cli.Write(srvUp)
		if err != nil {
			t.Errorf("client write: %v", err)
		}
	}()

	gotDown := make([]byte, len(srvUp))
	go func() {
		defer cliWg.Done()
		_, err := io.ReadFull(cli, gotDown)
		if err != nil {
			t.Errorf("client read: %v", err)
		}
		if !bytes.Equal(gotDown, srvUp) {
			t.Errorf("client got wrong down data")
		}
	}()

	cliWg.Wait()
	cli.Close()
	cancel()
	wg.Wait()
}

func TestTunnelLossy(t *testing.T) {
	domain := "tunnel.example.com"
	key := "secret"
	logger := newTestLogger()

	srv := NewServer(domain, key, logger)
	srv.relCfg = rel.Config{MinRTO: 20 * time.Millisecond, MaxRTO: 1 * time.Second}
	addr, stop, err := newLossyTestResolver(srv.responder, 0.05, 0.05)
	if err != nil {
		t.Fatalf("newTestResolver: %v", err)
	}
	defer stop()
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)

	testPayload := tunnelPayload()
	srvUp := payload(3, testPayload)

	go func() {
		defer wg.Done()
		conn, err := srv.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		_, err = io.Copy(conn, conn)
		if err != nil && err != io.EOF {
			t.Errorf("server echo: %v", err)
		}
	}()

	profiles := []sel.Profile{
		{Resolver: addr, RRType: 16, Transport: "udp", Cap: 700}, // TXT
	}

	cli, err := Dial(ctx, DialParams{
		Domain:   domain,
		Key:      key,
		Profiles: profiles,
		Timeout:  5 * time.Second,
		Rel:      rel.Config{MinRTO: 20 * time.Millisecond, MaxRTO: 1 * time.Second},
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	var cliWg sync.WaitGroup
	cliWg.Add(2)

	go func() {
		defer cliWg.Done()
		if _, err := cli.Write(srvUp); err != nil {
			t.Errorf("client write: %v", err)
		}
	}()

	gotDown := make([]byte, len(srvUp))
	go func() {
		defer cliWg.Done()
		if _, err := io.ReadFull(cli, gotDown); err != nil {
			t.Errorf("client read: %v", err)
		}
		if !bytes.Equal(gotDown, srvUp) {
			t.Errorf("client got wrong down data")
		}
	}()

	cliWg.Wait()
	cli.Close()
	cancel()
	wg.Wait()
}

func TestTunnelMSSSwitch(t *testing.T) {
	domain := "tunnel.example.com"
	key := "secret"
	logger := newTestLogger()

	srv := NewServer(domain, key, logger)
	srv.relCfg = rel.Config{MinRTO: 20 * time.Millisecond, MaxRTO: 1 * time.Second}
	addr, stop, err := newTestResolver(srv.responder)
	if err != nil {
		t.Fatalf("newTestResolver: %v", err)
	}
	defer stop()
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)

	srvUp := payload(5, tunnelPayload())

	go func() {
		defer wg.Done()
		conn, err := srv.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		_, err = io.Copy(conn, conn)
		if err != nil && err != io.EOF {
			t.Errorf("server echo: %v", err)
		}
	}()

	profiles := []sel.Profile{
		{Resolver: addr, RRType: 16, Transport: "udp", Cap: 700}, // TXT
		{Resolver: addr, RRType: 15, Transport: "udp", Cap: 150}, // MX
	}

	cli, err := Dial(ctx, DialParams{
		Domain:   domain,
		Key:      key,
		Profiles: profiles,
		Timeout:  5 * time.Second,
		Rel:      rel.Config{MinRTO: 20 * time.Millisecond, MaxRTO: 1 * time.Second},
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	var cliWg sync.WaitGroup
	cliWg.Add(2)

	go func() {
		defer cliWg.Done()
		if _, err := cli.Write(srvUp); err != nil {
			t.Errorf("client write: %v", err)
		}
	}()

	gotDown := make([]byte, len(srvUp))
	go func() {
		defer cliWg.Done()
		if _, err := io.ReadFull(cli, gotDown); err != nil {
			t.Errorf("client read: %v", err)
		}
	}()

	cliWg.Wait()
	cli.Close()
	cancel()
	wg.Wait()
}

func TestTunnelCloseFINEOF(t *testing.T) {
	domain := "tunnel.example.com"
	key := "secret"
	logger := newTestLogger()

	srv := NewServer(domain, key, logger)
	srv.relCfg = rel.Config{MinRTO: 20 * time.Millisecond, MaxRTO: 1 * time.Second}
	addr, stop, err := newTestResolver(srv.responder)
	if err != nil {
		t.Fatalf("newTestResolver: %v", err)
	}
	defer stop()
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		conn, err := srv.Accept()
		if err != nil {
			return
		}

		b := make([]byte, 100)
		_, err = io.ReadFull(conn, b)
		if err != io.EOF {
			t.Errorf("server expected EOF, got %v", err)
		}
		conn.Close()
	}()

	profiles := []sel.Profile{
		{Resolver: addr, RRType: 16, Transport: "udp", Cap: 700}, // TXT
	}

	cli, err := Dial(ctx, DialParams{
		Domain:   domain,
		Key:      key,
		Profiles: profiles,
		Timeout:  5 * time.Second,
		Rel:      rel.Config{MinRTO: 20 * time.Millisecond, MaxRTO: 1 * time.Second},
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	// Send FIN
	cli.Close()

	b := make([]byte, 100)
	_, err = io.ReadFull(cli, b)
	if !errors.Is(err, io.ErrClosedPipe) && !errors.Is(err, io.EOF) {
		t.Errorf("client expected closed error, got %v", err)
	}

	cancel()
	wg.Wait()
}

func TestTunnelRST(t *testing.T) {
	domain := "tunnel.example.com"
	key := "secret"
	logger := newTestLogger()

	srv := NewServer(domain, key, logger)
	srv.relCfg = rel.Config{MinRTO: 20 * time.Millisecond, MaxRTO: 1 * time.Second}
	addr, stop, err := newTestResolver(srv.responder)
	if err != nil {
		t.Fatalf("newTestResolver: %v", err)
	}
	defer stop()
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	profiles := []sel.Profile{
		{Resolver: addr, RRType: 16, Transport: "udp", Cap: 700}, // TXT
	}

	cli, err := Dial(ctx, DialParams{
		Domain:   domain,
		Key:      key,
		Profiles: profiles,
		Timeout:  5 * time.Second,
		Rel:      rel.Config{MinRTO: 20 * time.Millisecond, MaxRTO: 1 * time.Second},
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer cli.Close()

	time.Sleep(100 * time.Millisecond)

	// Force clear sessions on server
	srv.mu.Lock()
	srv.sessions = make(map[uint32]*serverSession)
	srv.mu.Unlock()

	time.Sleep(1 * time.Second)

	_, err = cli.Write([]byte("hello"))
	if err == nil {
		t.Errorf("expected error due to RST, got nil")
	} else if !strings.Contains(err.Error(), "reset") && !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("expected reset error, got %v", err)
	}
}

func TestTunnelDeadlines(t *testing.T) {
	domain := "tunnel.example.com"
	key := "secret"
	logger := newTestLogger()

	srv := NewServer(domain, key, logger)
	srv.relCfg = rel.Config{MinRTO: 20 * time.Millisecond, MaxRTO: 1 * time.Second}
	addr, stop, err := newTestResolver(srv.responder)
	if err != nil {
		t.Fatalf("newTestResolver: %v", err)
	}
	defer stop()
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	profiles := []sel.Profile{
		{Resolver: addr, RRType: 16, Transport: "udp", Cap: 700}, // TXT
	}

	cli, err := Dial(ctx, DialParams{
		Domain:   domain,
		Key:      key,
		Profiles: profiles,
		Timeout:  5 * time.Second,
		Rel:      rel.Config{MinRTO: 20 * time.Millisecond, MaxRTO: 1 * time.Second},
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer cli.Close()

	err = cli.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	if err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}

	b := make([]byte, 100)
	_, err = cli.Read(b)
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("expected timeout error, got %v", err)
	}
}

func TestTunnelLeak(t *testing.T) {
	domain := "tunnel.example.com"
	key := "secret"
	logger := newTestLogger()

	srv := NewServer(domain, key, logger)
	srv.relCfg = rel.Config{MinRTO: 20 * time.Millisecond, MaxRTO: 1 * time.Second}
	addr, stop, err := newTestResolver(srv.responder)
	if err != nil {
		t.Fatalf("newTestResolver: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	profiles := []sel.Profile{
		{Resolver: addr, RRType: 16, Transport: "udp", Cap: 700}, // TXT
	}

	cli, err := Dial(ctx, DialParams{
		Domain:   domain,
		Key:      key,
		Profiles: profiles,
		Timeout:  5 * time.Second,
		Rel:      rel.Config{MinRTO: 20 * time.Millisecond, MaxRTO: 1 * time.Second},
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	cli.Close()
	srv.Close()
	stop()
	cancel()
	time.Sleep(100 * time.Millisecond)
}

func TestWriteAfterCloseFails(t *testing.T) {
	srv := NewServer("tunnel.example.com", "secret", newTestLogger())
	defer srv.Close()
	addr, stop, err := newTestResolver(srv.responder)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	cli, err := Dial(context.Background(), DialParams{
		Domain: "tunnel.example.com", Key: "secret",
		Profiles: []sel.Profile{{Resolver: addr, RRType: 16, Transport: "udp", Cap: 700}},
		Timeout:  2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	cli.Close()
	if n, err := cli.Write([]byte("late")); err == nil || n != 0 {
		t.Fatalf("Write after Close = (%d, %v), want (0, error)", n, err)
	}
}

func TestServerCloseStopsListener(t *testing.T) {
	srv := NewServer("tunnel.example.com", "secret", newTestLogger())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(context.Background(), "127.0.0.1:0") }()
	time.Sleep(300 * time.Millisecond) // let the listeners come up
	srv.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Serve still running after Close")
	}
}

// stallConn delays replies, like a resolver that sits on a query for a couple
// of seconds before answering, and records how many stalled replies were pending
// at once (= exchanges the client had in flight).
type stallConn struct {
	net.PacketConn
	delay   time.Duration
	pending atomic.Int32
	peak    atomic.Int32
}

func (c *stallConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	n := c.pending.Add(1)
	for {
		old := c.peak.Load()
		if n <= old || c.peak.CompareAndSwap(old, n) {
			break
		}
	}
	cp := append([]byte(nil), p...)
	time.AfterFunc(c.delay, func() {
		_, _ = c.PacketConn.WriteTo(cp, addr)
		c.pending.Add(-1)
	})
	return len(p), nil
}

// peakInflight runs a client against a resolver that delays every reply by
// 1.5s and returns the most exchanges it ever had in flight.
func peakInflight(t *testing.T, noHedge bool) int32 {
	t.Helper()
	domain, key := "tunnel.example.com", "secret"
	srv := NewServer(domain, key, newTestLogger())
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	sc := &stallConn{PacketConn: pc, delay: 1500 * time.Millisecond}
	ds := &dns.Server{PacketConn: sc, Handler: dns.HandlerFunc(srv.responder.handle)}
	go func() { _ = ds.ActivateAndServe() }()
	defer ds.Shutdown()
	defer srv.Close()

	cli, err := Dial(context.Background(), DialParams{
		Domain: domain, Key: key, Workers: 4, NoHedge: noHedge,
		Profiles: []sel.Profile{{Resolver: pc.LocalAddr().String(), RRType: 16, Transport: "udp", Cap: 700}},
		Timeout:  4 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = cli.Write(payload(11, 32*1024)) }() // keep data pending so helpers are allowed
	time.Sleep(5 * time.Second)
	cli.Close()
	return sc.peak.Load()
}

func TestHedgingKeepsThePipeFullDuringStalls(t *testing.T) {
	plain := peakInflight(t, true)
	hedged := peakInflight(t, false)
	t.Logf("peak in-flight exchanges with every reply stalled 1.5s (4 workers): no hedging %d, hedging %d", plain, hedged)
	if plain > 4 {
		t.Errorf("without hedging the client had %d in flight, want <= 4 workers", plain)
	}
	if hedged <= 4 {
		t.Errorf("with hedging the client never exceeded its 4 workers (peak %d)", hedged)
	}
}

func TestServerCloseRefusesNewSessions(t *testing.T) {
	srv := NewServer("t.example.com", "k", newTestLogger())
	srv.Close()
	pk := rel.Packet{}
	if out, flags := srv.handle(42, FlagSYN, pk.Marshal(), 400); out != nil || flags != FlagRST {
		t.Fatalf("closed server created/answered a session: flags=%d", flags)
	}
	srv.mu.Lock()
	n := len(srv.sessions)
	srv.mu.Unlock()
	if n != 0 {
		t.Fatalf("closed server holds %d sessions", n)
	}
}

func TestSustainedOutageEndsTheCarrier(t *testing.T) {
	srv := NewServer("t.example.com", "k", newTestLogger())
	defer srv.Close()
	addr, stop, err := newTestResolver(srv.responder)
	if err != nil {
		t.Fatal(err)
	}
	cli, err := Dial(context.Background(), DialParams{
		Domain: "t.example.com", Key: "k", Timeout: 500 * time.Millisecond, OutageLimit: 2 * time.Second,
		Profiles: []sel.Profile{{Resolver: addr, RRType: 16, Transport: "udp", Cap: 700}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	stop() // every resolver is gone now
	done := make(chan error, 1)
	go func() {
		_, err := cli.Read(make([]byte, 1))
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Read returned without error after the outage")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("carrier still alive long after a sustained outage")
	}
}

func TestFullAcceptQueueRefusesNewSessions(t *testing.T) {
	srv := NewServer("t.example.com", "k", newTestLogger())
	defer srv.Close()
	for i := 0; i < cap(srv.acceptCh); i++ {
		srv.acceptCh <- nil // nobody is accepting
	}
	pk := rel.Packet{}
	out, flags := srv.handle(7, FlagSYN, pk.Marshal(), 400)
	if out != nil || flags != FlagRST {
		t.Fatalf("a SYN with a full accept queue must be refused (RST), got flags=%d out=%v", flags, out != nil)
	}
	srv.mu.Lock()
	n := len(srv.sessions)
	srv.mu.Unlock()
	if n != 0 {
		t.Fatalf("refused SYN left %d sessions behind", n)
	}
}

func TestReceiveActivityIsAWindow(t *testing.T) {
	c := &clientConn{}
	if c.recentRX() {
		t.Fatal("no activity yet")
	}
	c.lastRX = time.Now()
	if !c.recentRX() {
		t.Fatal("fresh activity not visible")
	}
	c.lastRX = time.Now().Add(-2 * rxWindow)
	if c.recentRX() {
		t.Fatal("stale activity still counted")
	}
}

// A middlebox that answers the short empty polls but drops the longer
// data-carrying queries must not keep the tunnel "alive": every poll replies OK,
// yet no data gets through.
func TestDataBlackholeEndsTheCarrier(t *testing.T) {
	const domain, key = "t.example.com", "k"
	srv := NewServer(domain, key, newTestLogger())
	defer srv.Close()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ds := &dns.Server{PacketConn: pc, Handler: dns.HandlerFunc(func(w dns.ResponseWriter, req *dns.Msg) {
		if len(req.Question) == 1 && len(req.Question[0].Name) > 130 { // data-bearing QNAMEs are long
			return
		}
		srv.responder.handle(w, req)
	})}
	go func() { _ = ds.ActivateAndServe() }()
	defer ds.Shutdown()

	cli, err := Dial(context.Background(), DialParams{
		Domain: domain, Key: key, Timeout: 400 * time.Millisecond, OutageLimit: 3 * time.Second,
		Profiles: []sel.Profile{{Resolver: pc.LocalAddr().String(), RRType: 16, Transport: "udp", Cap: 700}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	go func() { _, _ = cli.Write(payload(5, 4096)) }()
	done := make(chan error, 1)
	go func() {
		_, err := cli.Read(make([]byte, 1))
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Read returned without error although no data can get through")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("carrier still alive although data never gets through")
	}
}
