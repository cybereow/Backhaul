package handlers

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"io"
	"math/big"
	mrand "math/rand"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// tlsPair returns the two ends of a TLS connection over loopback, so the moves
// are also exercised on the connection type a client's tunnel really is.
func tlsPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "x"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), DNSNames: []string{"x"}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	cert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	pool := x509.NewCertPool()
	leaf, _ := x509.ParseCertificate(der)
	pool.AddCert(leaf)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	require.NoError(t, err)
	defer ln.Close()
	ch := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err == nil {
			c.(*tls.Conn).Handshake()
		}
		ch <- c
	}()
	c, err := tls.Dial("tcp", ln.Addr().String(), &tls.Config{RootCAs: pool, ServerName: "x"})
	require.NoError(t, err)
	s := <-ch
	t.Cleanup(func() { c.Close(); s.Close() })
	return c, s
}

// flowPair is a raw flow as the two ends of a tunnel run it:
//
//	user <-> srv flow <== tunnel ==> cli flow <-> service
type flowPair struct {
	t        *testing.T
	user     *net.TCPConn // the test's end of the user's connection
	service  *net.TCPConn // the test's end of the service's connection
	srv, cli *RawFlow
	tls      bool
	finished sync.WaitGroup
}

func newFlowPair(t *testing.T, useTLS bool) *flowPair {
	t.Helper()
	p := &flowPair{t: t, tls: useTLS}
	user, srvApp := tcpPair(t)
	svc, cliApp := tcpPair(t)
	p.user, p.service = user, svc
	var tunS, tunC net.Conn
	if useTLS {
		tunC, tunS = tlsPair(t)
	} else {
		a, b := tcpPair(t)
		tunC, tunS = a, b
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	p.srv = NewRawFlow(ctx, RawFlowConfig{ID: "f", Tunnel: &RawTunnel{Conn: tunS, Born: time.Now()}, App: srvApp, Logger: quietLogger()})
	p.cli = NewRawFlow(ctx, RawFlowConfig{ID: "f", Tunnel: &RawTunnel{Conn: tunC, Born: time.Now()}, App: cliApp, Logger: quietLogger()})
	p.finished.Add(2)
	go func() { defer p.finished.Done(); p.srv.Run() }()
	go func() { defer p.finished.Done(); p.cli.Run() }()
	return p
}

// move runs one move of the flow to a fresh tunnel connection, both ends at once.
func (p *flowPair) move() (srvErr, cliErr error) {
	var s, c net.Conn
	if p.tls {
		c, s = tlsPair(p.t)
	} else {
		a, b := tcpPair(p.t)
		c, s = a, b
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); srvErr = p.srv.Swap(&RawTunnel{Conn: s, Born: time.Now()}) }()
	go func() { defer wg.Done(); cliErr = p.cli.Swap(&RawTunnel{Conn: c, Born: time.Now()}) }()
	wg.Wait()
	return
}

// stream is a deterministic byte stream of the given length.
func stream(seed int64, n int64) io.Reader {
	return io.LimitReader(mrand.New(mrand.NewSource(seed)), n)
}

func sumOf(seed, n int64) string {
	h := sha256.New()
	io.Copy(h, stream(seed, n))
	return fmt.Sprintf("%x", h.Sum(nil))
}

// pushAndCheck sends nUp bytes user -> service and nDown bytes service -> user at
// once, in bursts with random pauses when jitter is set, half-closes at the end
// and returns the hashes of what arrived.
func (p *flowPair) pushAndCheck(seed, nUp, nDown int64, jitter bool) (upGot, downGot string) {
	var wg sync.WaitGroup
	send := func(c *net.TCPConn, seed, n int64) {
		defer wg.Done()
		src := stream(seed, n)
		rng := mrand.New(mrand.NewSource(seed))
		buf := make([]byte, 128<<10)
		for {
			k := len(buf)
			if jitter {
				k = 1 + rng.Intn(len(buf))
			}
			m, err := io.ReadFull(src, buf[:k])
			if m > 0 {
				if _, werr := c.Write(buf[:m]); werr != nil {
					return
				}
			}
			if err != nil {
				break
			}
			if jitter && rng.Intn(20) == 0 {
				time.Sleep(time.Duration(rng.Intn(3000)) * time.Microsecond)
			}
		}
		c.CloseWrite()
	}
	recv := func(c *net.TCPConn, out *string) {
		defer wg.Done()
		h := sha256.New()
		io.Copy(h, c)
		*out = fmt.Sprintf("%x", h.Sum(nil))
	}
	wg.Add(4)
	go send(p.user, seed, nUp)
	go send(p.service, seed+1, nDown)
	go recv(p.service, &upGot)
	go recv(p.user, &downGot)
	wg.Wait()
	return
}

// Moving the flow to another connection over and over while 64 MiB go each way
// delivers every byte exactly once, in order.
func TestRawFlowMovesKeepEveryByte(t *testing.T) {
	for _, useTLS := range []bool{false, true} {
		useTLS := useTLS
		t.Run(fmt.Sprintf("tls=%v", useTLS), func(t *testing.T) {
			p := newFlowPair(t, useTLS)
			n := int64(160 << 20)
			if useTLS {
				n = 128 << 20
			}
			stop := make(chan struct{})
			var moves, aborts atomic.Int64
			var mv sync.WaitGroup
			mv.Add(1)
			go func() {
				defer mv.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					se, ce := p.move()
					if se == nil && ce == nil {
						moves.Add(1)
					} else {
						aborts.Add(1)
						if se != nil && !isAbort(se) || ce != nil && !isAbort(ce) {
							t.Errorf("a move failed hard: srv=%v cli=%v", se, ce)
							return
						}
					}
					time.Sleep(5 * time.Millisecond)
				}
			}()
			up, down := p.pushAndCheck(7, n, n, true)
			close(stop)
			mv.Wait()
			require.Equal(t, sumOf(7, n), up, "user -> service bytes differ")
			require.Equal(t, sumOf(8, n), down, "service -> user bytes differ")
			p.finished.Wait()
			t.Logf("tls=%v: %d moves, %d given up, flow reports %d/%d", useTLS, moves.Load(), aborts.Load(), p.srv.Swaps(), p.cli.Swaps())
			require.Greater(t, moves.Load(), int64(20), "too few moves for the test to mean anything")
		})
	}
}

func isAbort(err error) bool { return err != nil && !errorsIs(err, ErrFlowCut) }

func errorsIs(err, target error) bool {
	for err != nil {
		if err == target {
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// A flow with no traffic at all is moved again and again and still works after.
func TestRawFlowIdleMovesThenData(t *testing.T) {
	p := newFlowPair(t, false)
	for i := 0; i < 30; i++ {
		se, ce := p.move()
		require.NoError(t, se, "move %d", i)
		require.NoError(t, ce, "move %d", i)
	}
	require.Equal(t, int64(30), p.srv.Swaps())
	up, down := p.pushAndCheck(3, 5<<20, 5<<20, false)
	require.Equal(t, sumOf(3, 5<<20), up)
	require.Equal(t, sumOf(4, 5<<20), down)
}

// Many flows at once, bursty and with random pauses, each moved at random
// moments; every byte of every flow arrives exactly.
func TestRawFlowStressManyFlowsRandomMoves(t *testing.T) {
	if testing.Short() {
		t.Skip("stress")
	}
	const flows = 40
	goroutinesBefore := runtime.NumGoroutine()
	var wg sync.WaitGroup
	var totalMoves atomic.Int64
	for i := 0; i < flows; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := mrand.New(mrand.NewSource(int64(i)))
			p := newFlowPair(t, i%5 == 0)
			up, down := rng.Int63n(24<<20)+1, rng.Int63n(24<<20)+1
			stop := make(chan struct{})
			var mv sync.WaitGroup
			mv.Add(1)
			go func() {
				defer mv.Done()
				for {
					select {
					case <-stop:
						return
					case <-time.After(time.Duration(rng.Intn(40)) * time.Millisecond):
					}
					se, ce := p.move()
					if se == nil && ce == nil {
						totalMoves.Add(1)
					} else if !isAbort(se) && se != nil || !isAbort(ce) && ce != nil {
						t.Errorf("flow %d: a move failed hard: %v / %v", i, se, ce)
						return
					}
				}
			}()
			gotUp, gotDown := p.pushAndCheck(int64(100+i*2), up, down, true)
			close(stop)
			mv.Wait()
			if sumOf(int64(100+i*2), up) != gotUp || sumOf(int64(101+i*2), down) != gotDown {
				t.Errorf("flow %d: bytes differ after moves (up %d, down %d)", i, up, down)
			}
			p.finished.Wait()
		}()
	}
	wg.Wait()
	t.Logf("%d flows, %d moves in all", flows, totalMoves.Load())
	require.Greater(t, totalMoves.Load(), int64(flows), "barely any moves happened")
	require.True(t, waitUntil(10*time.Second, func() bool { return runtime.NumGoroutine() < goroutinesBefore+30 }),
		"goroutines left behind: %d now, %d before", runtime.NumGoroutine(), goroutinesBefore)
}

func waitUntil(d time.Duration, cond func() bool) bool {
	for deadline := time.Now().Add(d); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if cond() {
			return true
		}
	}
	return cond()
}

// A slow reader on the user's side (the old connection backs up) does not
// deadlock a move, and no byte is lost when it catches up.
func TestRawFlowMoveWithASlowConsumer(t *testing.T) {
	p := newFlowPair(t, false)
	const n = 96 << 20
	go func() { // service -> user, fast
		io.Copy(p.service, stream(21, n))
		p.service.CloseWrite()
	}()
	go io.Copy(io.Discard, p.service) // nothing user -> service in this test; the direction stays open

	stop := make(chan struct{})
	var moves atomic.Int64
	var mv sync.WaitGroup
	mv.Add(1)
	go func() { // moves, while the reader below is slow and the pipe is full
		defer mv.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(30 * time.Millisecond):
			}
			se, ce := p.move()
			if se == nil && ce == nil {
				moves.Add(1)
			} else if !isAbort(se) && se != nil || !isAbort(ce) && ce != nil {
				t.Errorf("a move failed hard: %v / %v", se, ce)
				return
			}
		}
	}()
	h := sha256.New()
	buf := make([]byte, 16<<10)
	var got int64
	for {
		k, err := p.user.Read(buf)
		h.Write(buf[:k])
		got += int64(k)
		time.Sleep(300 * time.Microsecond) // slow reader: ~50 MB/s
		if err != nil {
			break
		}
	}
	close(stop)
	mv.Wait()
	p.user.Close()
	require.Equal(t, int64(n), got)
	require.Equal(t, sumOf(21, n), fmt.Sprintf("%x", h.Sum(nil)))
	t.Logf("%d moves while the reader was slow", moves.Load())
	require.Greater(t, moves.Load(), int64(3))
}

// If the other end has already finished one direction the move is given up on
// both ends, and the flow carries on over the connection it was on.
func TestRawFlowMoveGivenUpWhenADirectionIsOver(t *testing.T) {
	p := newFlowPair(t, false)
	p.user.CloseWrite() // user -> service is over
	time.Sleep(100 * time.Millisecond)
	se, ce := p.move()
	require.Error(t, se)
	require.Error(t, ce)
	require.False(t, errorsIs(se, ErrFlowCut) || errorsIs(ce, ErrFlowCut), "a plain give-up must not cut the flow: %v / %v", se, ce)
	// The flow still works: service -> user.
	go func() { io.Copy(p.service, stream(31, 1<<20)); p.service.CloseWrite() }()
	h := sha256.New()
	io.Copy(h, p.user)
	require.Equal(t, sumOf(31, 1<<20), fmt.Sprintf("%x", h.Sum(nil)))
}

// A new connection that never answers: the move is given up and the flow keeps
// working on the old one.
func TestRawFlowMoveGivenUpWhenTheNewConnectionIsSilent(t *testing.T) {
	old := swapTimeout
	swapTimeout = 400 * time.Millisecond
	defer func() { swapTimeout = old }()
	p := newFlowPair(t, false)
	a, _ := tcpPair(t) // the far end is never served
	start := time.Now()
	err := p.srv.Swap(&RawTunnel{Conn: a, Born: time.Now()})
	require.Error(t, err)
	require.False(t, errorsIs(err, ErrFlowCut), "an unanswered new connection must not cut the flow: %v", err)
	require.Less(t, time.Since(start), 5*time.Second)
	up, down := p.pushAndCheck(41, 4<<20, 4<<20, false)
	require.Equal(t, sumOf(41, 4<<20), up)
	require.Equal(t, sumOf(42, 4<<20), down)
}

// The new connection is closed before the move starts: given up, flow intact.
func TestRawFlowMoveGivenUpWhenTheNewConnectionIsDead(t *testing.T) {
	p := newFlowPair(t, false)
	a, b := tcpPair(t)
	a.Close()
	b.Close()
	err := p.srv.Swap(&RawTunnel{Conn: a, Born: time.Now()})
	require.Error(t, err)
	require.False(t, errorsIs(err, ErrFlowCut), "%v", err)
	up, down := p.pushAndCheck(51, 2<<20, 2<<20, false)
	require.Equal(t, sumOf(51, 2<<20), up)
	require.Equal(t, sumOf(52, 2<<20), down)
}

// Flows that end right while a move is under way: nothing hangs, and whatever
// was delivered is correct.
func TestRawFlowMoveRacingTheEndOfTheFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("stress")
	}
	var wg sync.WaitGroup
	for i := 0; i < 60; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := newFlowPair(t, false)
			var mv sync.WaitGroup
			mv.Add(1)
			go func() {
				defer mv.Done()
				time.Sleep(time.Duration(i%7) * time.Millisecond)
				for k := 0; k < 3; k++ {
					p.move() // may be given up, may cut: only hanging is wrong here
				}
			}()
			done := make(chan struct{})
			go func() {
				p.pushAndCheck(int64(i), 20000, 20000, true)
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(30 * time.Second):
				t.Errorf("flow %d hung", i)
			}
			mv.Wait()
		}()
	}
	wg.Wait()
}
