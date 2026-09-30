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
