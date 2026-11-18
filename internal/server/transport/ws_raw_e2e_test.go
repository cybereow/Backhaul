package transport

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	mrand "math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/musix/backhaul/config"
	client_transport "github.com/musix/backhaul/internal/client/transport"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// startWsE2E runs a ws server and client in-process, with a destination service
// handled by dest, and returns the server's public address. raw selects the
// splice-able form of the flow (see handlers.RawFlowPrefix).
func startWsE2E(t *testing.T, raw bool, dest func(net.Conn)) string {
	t.Helper()
	return startWsE2EAged(t, raw, dest, 0)
}

// startWsE2EAged is startWsE2E with idle-connection rotation on: an idle pool
// connection is replaced once it is about maxAge old (see RotationPlan).
func startWsE2EAged(t *testing.T, raw bool, dest func(net.Conn), maxAge time.Duration) string {
	t.Helper()
	addr, _ := startWsE2EServer(t, raw, dest, maxAge)
	return addr
}

// startWsE2EServer also returns the server, for tests that restart it.
func startWsE2EServer(t *testing.T, raw bool, dest func(net.Conn), maxAge time.Duration) (string, *WsTransport) {
	t.Helper()
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	if os.Getenv("RAW_TEST_LOG") != "" {
		logger.SetOutput(os.Stderr)
		logger.SetLevel(logrus.DebugLevel)
		logger.SetFormatter(&logrus.TextFormatter{FullTimestamp: true, TimestampFormat: "05.000"})
	}
	// Its own logger: Restart changes the level of the server's, and in a real
	// deployment the two are different processes.
	clientLogger := logrus.New()
	clientLogger.SetOutput(logger.Out)
	clientLogger.SetLevel(logger.Level)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go dest(c)
		}
	}()

	tunnelPort := testPort(t, "tcp")
	publicPort := testPort(t, "tcp")
	server := NewWSServer(ctx, &WsConfig{
		BindAddr:    fmt.Sprintf("127.0.0.1:%d", tunnelPort),
		Token:       "test_token",
		ChannelSize: 64,
		Heartbeat:   30 * time.Second,
		KeepAlive:   30 * time.Second,
		Nodelay:     true,
		Mode:        config.WS,
		Ports:       []string{fmt.Sprintf("127.0.0.1:%d=%s", publicPort, ln.Addr().String())},
		Raw:         raw,
		MaxConnAge:  maxAge,
	}, logger)
	go server.Start()
	time.Sleep(500 * time.Millisecond)

	client := client_transport.NewWSClient(ctx, &client_transport.WsConfig{
		RemoteAddr:   fmt.Sprintf("127.0.0.1:%d", tunnelPort),
		Token:        "test_token",
		KeepAlive:    30 * time.Second,
		DialTimeOut:  10 * time.Second,
		ConnPoolSize: 8,
		Nodelay:      true,
		Mode:         config.WS,
		Path:         "/",
	}, clientLogger)
	go client.Start()

	addr := fmt.Sprintf("127.0.0.1:%d", publicPort)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		// A probe flow: the pool is ready once an echo round-trips.
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			c.SetDeadline(time.Now().Add(2 * time.Second))
			if _, err := c.Write([]byte("p")); err == nil {
				c.(*net.TCPConn).CloseWrite()
				b := make([]byte, 1)
				if _, err := io.ReadFull(c, b); err == nil {
					c.Close()
					return addr, server
				}
			}
			c.Close()
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("ws tunnel never carried a probe flow")
	return "", nil
}

func echoDest(c net.Conn) {
	defer c.Close()
	io.Copy(c, c)
}

func rawModes(t *testing.T, f func(t *testing.T, raw bool)) {
	for _, raw := range []bool{false, true} {
		raw := raw
		t.Run(fmt.Sprintf("raw=%v", raw), func(t *testing.T) { f(t, raw) })
	}
}

// Bulk data both ways on one flow, byte for byte.
func TestWsRawBulkBothWays(t *testing.T) {
	rawModes(t, func(t *testing.T, raw bool) {
		addr := startWsE2E(t, raw, echoDest)
		payload := make([]byte, 8<<20)
		rand.Read(payload)

		c, err := net.Dial("tcp", addr)
		require.NoError(t, err)
		defer c.Close()
		c.SetDeadline(time.Now().Add(30 * time.Second))
		go func() {
			c.Write(payload)
			c.(*net.TCPConn).CloseWrite()
		}()
		got, err := io.ReadAll(c)
		require.NoError(t, err)
		require.Equal(t, len(payload), len(got))
		require.True(t, bytes.Equal(payload, got), "echoed payload differs")
	})
}

// Many concurrent flows, each with its own data: nothing crosses between them.
func TestWsRawConcurrentFlows(t *testing.T) {
	rawModes(t, func(t *testing.T, raw bool) {
		addr := startWsE2E(t, raw, echoDest)
		var wg sync.WaitGroup
		errs := make(chan error, 64)
		for i := 0; i < 32; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				payload := make([]byte, 256<<10)
				rand.Read(payload)
				c, err := net.Dial("tcp", addr)
				if err != nil {
					errs <- err
					return
				}
				defer c.Close()
				c.SetDeadline(time.Now().Add(30 * time.Second))
				go func() {
					c.Write(payload)
					c.(*net.TCPConn).CloseWrite()
				}()
				got, err := io.ReadAll(c)
				if err != nil {
					errs <- err
					return
				}
				if !bytes.Equal(payload, got) {
					errs <- fmt.Errorf("flow payload differs (%d vs %d bytes)", len(payload), len(got))
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
	})
}

// A request that ends with a FIN still gets its whole, late reply.
func TestWsRawHalfCloseThenReply(t *testing.T) {
	reply := make([]byte, 1<<20)
	rand.Read(reply)
	dest := func(c net.Conn) {
		defer c.Close()
		req, _ := io.ReadAll(c) // until the client's FIN
		if string(req) == "p" {
			c.Write(req) // the readiness probe of startWsE2E
			return
		}
		time.Sleep(300 * time.Millisecond)
		c.Write(reply)
	}
	rawModes(t, func(t *testing.T, raw bool) {
		addr := startWsE2E(t, raw, dest)
		c, err := net.Dial("tcp", addr)
		require.NoError(t, err)
		defer c.Close()
		c.SetDeadline(time.Now().Add(20 * time.Second))
		_, err = c.Write([]byte("GET / request"))
		require.NoError(t, err)
		require.NoError(t, c.(*net.TCPConn).CloseWrite())
		got, err := io.ReadAll(c)
		require.NoError(t, err)
		require.True(t, bytes.Equal(reply, got), "late reply truncated: %d of %d bytes", len(got), len(reply))
	})
}

// The destination speaks first, at once: those bytes reach the client side of
// the tunnel in the same read as the open message, so they sit in its buffer at
// the moment the stream turns raw.
func TestWsRawServerSpeaksFirst(t *testing.T) {
	greeting := []byte("220 hello, you are connected\r\n")
	dest := func(c net.Conn) {
		defer c.Close()
		c.Write(greeting)
		io.Copy(io.Discard, c)
	}
	rawModes(t, func(t *testing.T, raw bool) {
		// The readiness probe of startWsE2E expects an echo, so use a
		// destination that does both: greet, then echo.
		addr := startWsE2E(t, raw, func(c net.Conn) {
			defer c.Close()
			c.Write(greeting)
			io.Copy(c, c)
		})
		_ = dest
		for i := 0; i < 20; i++ {
			c, err := net.Dial("tcp", addr)
			require.NoError(t, err)
			c.SetDeadline(time.Now().Add(10 * time.Second))
			got := make([]byte, len(greeting))
			_, err = io.ReadFull(c, got)
			require.NoError(t, err)
			require.Equal(t, greeting, got)
			c.Close()
		}
	})
}

// Empty and tiny flows, repeated, so pool connections are reused and replaced.
func TestWsRawTinyFlows(t *testing.T) {
	rawModes(t, func(t *testing.T, raw bool) {
		addr := startWsE2E(t, raw, echoDest)
		for i := 0; i < 40; i++ {
			c, err := net.Dial("tcp", addr)
			require.NoError(t, err)
			c.SetDeadline(time.Now().Add(10 * time.Second))
			if i%4 != 0 { // every fourth flow sends nothing at all
				msg := bytes.Repeat([]byte{byte(i)}, 1+i)
				_, err = c.Write(msg)
				require.NoError(t, err)
				got := make([]byte, len(msg))
				_, err = io.ReadFull(c, got)
				require.NoError(t, err)
				require.Equal(t, msg, got)
			}
			c.Close()
		}
	})
}

// The production shape: `ports` is empty, so plain HTTP requests to the tunnel
// listener are forwarded through the tunnel to the same port on the client. Those
// user sockets arrive wrapped (prefixConn: the request line the HTTP server had
// already read), and a keep-alive connection carries several requests.
func TestWsRawBindForwardHTTP(t *testing.T) {
	rawModes(t, func(t *testing.T, raw bool) {
		logger := logrus.New()
		logger.SetOutput(io.Discard)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		port := testPort(t, "tcp")
		down := make([]byte, 6<<20)
		rand.Read(down)
		mux := http.NewServeMux()
		mux.HandleFunc("/down", func(w http.ResponseWriter, r *http.Request) { w.Write(down) })
		var uploaded sync.Map
		mux.HandleFunc("/up", func(w http.ResponseWriter, r *http.Request) {
			h := sha256.New()
			n, _ := io.Copy(h, r.Body)
			uploaded.Store(r.URL.Query().Get("id"), fmt.Sprintf("%d:%x", n, h.Sum(nil)))
		})
		dest, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		require.NoError(t, err)
		defer dest.Close()
		go http.Serve(dest, mux)

		server := NewWSServer(ctx, &WsConfig{
			BindAddr:    fmt.Sprintf("127.0.0.2:%d", port),
			Token:       "test_token",
			ChannelSize: 64,
			Heartbeat:   30 * time.Second,
			KeepAlive:   30 * time.Second,
			Nodelay:     true,
			Mode:        config.WS,
			Raw:         raw,
		}, logger)
		go server.Start()
		time.Sleep(500 * time.Millisecond)
		client := client_transport.NewWSClient(ctx, &client_transport.WsConfig{
			RemoteAddr:   fmt.Sprintf("127.0.0.2:%d", port),
			Token:        "test_token",
			KeepAlive:    30 * time.Second,
			DialTimeOut:  10 * time.Second,
			ConnPoolSize: 8,
			Nodelay:      true,
			Mode:         config.WS,
			Path:         "/",
		}, logger)
		go client.Start()

		hc := &http.Client{Timeout: 30 * time.Second}
		base := fmt.Sprintf("http://127.0.0.2:%d", port)
		var resp *http.Response
		for i := 0; i < 60; i++ { // wait for the pool
			resp, err = hc.Get(base + "/down")
			if err == nil {
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
		require.NoError(t, err, "tunnel never came up")
		for round := 0; round < 3; round++ { // same keep-alive connection
			if round > 0 {
				resp, err = hc.Get(base + "/down")
				require.NoError(t, err)
			}
			got, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			require.NoError(t, err)
			require.True(t, bytes.Equal(down, got), "download %d differs (%d bytes)", round, len(got))

			up := make([]byte, 5<<20)
			rand.Read(up)
			want := fmt.Sprintf("%d:%x", len(up), sha256.Sum256(up))
			id := fmt.Sprintf("r%d", round)
			r2, err := hc.Post(base+"/up?id="+id, "application/octet-stream", bytes.NewReader(up))
			require.NoError(t, err)
			r2.Body.Close()
			v, _ := uploaded.Load(id)
			require.Equal(t, want, v, "upload %d differs", round)
		}
	})
}

// A long transfer in both directions on several flows at once, checked by hash
// so nothing has to be held in memory.
func TestWsRawStreamIntegrity(t *testing.T) {
	if testing.Short() {
		t.Skip("long transfer")
	}
	perFlow := int64(96 << 20)
	if v, err := strconv.Atoi(os.Getenv("RAW_TEST_MB")); err == nil {
		perFlow = int64(v) << 20
	}
	stream := func(seed int64) io.Reader {
		return io.LimitReader(mrand.New(mrand.NewSource(seed)), perFlow)
	}
	sum := func(seed int64) string {
		h := sha256.New()
		io.Copy(h, stream(seed))
		return fmt.Sprintf("%x", h.Sum(nil))
	}
	want := map[int64]string{}
	for seed := int64(1); seed <= 4; seed++ {
		want[seed] = sum(seed)
	}
	rawModes(t, func(t *testing.T, raw bool) {
		// The destination answers a request with the stream for the seed it was
		// given, and hashes what it receives.
		recv := sync.Map{}
		dest := func(c net.Conn) {
			defer c.Close()
			var seedb [1]byte
			if _, err := io.ReadFull(c, seedb[:]); err != nil {
				return
			}
			if seedb[0] == 'p' { // readiness probe
				c.Write([]byte("p"))
				return
			}
			seed := int64(seedb[0])
			sent := make(chan struct{})
			go func() {
				defer close(sent)
				io.Copy(c, stream(seed+100))
				c.(*net.TCPConn).CloseWrite()
			}()
			h := sha256.New()
			io.Copy(h, c)
			recv.Store(seed, fmt.Sprintf("%x", h.Sum(nil)))
			<-sent // closing before the reply is out would cut it
		}
		addr := startWsE2E(t, raw, dest)
		var wg sync.WaitGroup
		for seed := int64(1); seed <= 4; seed++ {
			seed := seed
			wg.Add(1)
			go func() {
				defer wg.Done()
				c, err := net.Dial("tcp", addr)
				if err != nil {
					t.Error(err)
					return
				}
				defer c.Close()
				c.SetDeadline(time.Now().Add(120 * time.Second))
				c.Write([]byte{byte(seed)})
				upDone := make(chan struct{})
				go func() {
					defer close(upDone)
					io.Copy(c, stream(seed))
					c.(*net.TCPConn).CloseWrite()
				}()
				defer func() { <-upDone }() // closing before the upload is out would cut it
				h := sha256.New()
				n, cerr := io.Copy(h, c)
				if got, w := fmt.Sprintf("%x", h.Sum(nil)), sum(seed+100); got != w {
					t.Errorf("flow %d download hash differs (%d of %d bytes, err %v)", seed, n, perFlow, cerr)
				}
			}()
		}
		wg.Wait()
		// The destination stores its hash when it sees the upload end, which can
		// be a moment after the clients are done.
		for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
			n := 0
			recv.Range(func(_, _ any) bool { n++; return true })
			if n == 4 {
				break
			}
		}
		for seed := int64(1); seed <= 4; seed++ {
			if v, _ := recv.Load(seed); v != want[seed] {
				t.Errorf("flow %d upload hash differs: %v", seed, v)
			}
		}
	})
}

// Idle pool connections that reached their rotation age are closed by the
// server. They stay queued until a flow picks one; a flow must then be served by
// another connection, not hang on the retired one.
func TestWsFlowsAfterIdleRotation(t *testing.T) {
	rawModes(t, func(t *testing.T, raw bool) {
		addr := startWsE2EAged(t, raw, echoDest, 2*time.Second)
		time.Sleep(7 * time.Second) // several rotation cycles with no traffic
		for i := 0; i < 12; i++ {
			c, err := net.Dial("tcp", addr)
			require.NoError(t, err)
			c.SetDeadline(time.Now().Add(6 * time.Second))
			msg := []byte(fmt.Sprintf("flow %d", i))
			_, err = c.Write(msg)
			require.NoError(t, err)
			got := make([]byte, len(msg))
			_, err = io.ReadFull(c, got)
			require.NoError(t, err, "flow %d after rotation was not served", i)
			require.Equal(t, msg, got)
			c.Close()
		}
	})
}

// A generation that has had idle connections retired still stops: nothing is
// left waiting on a retired connection's lock, so Restart joins at once instead
// of running into its 30s join timeout and retrying for ever.
func TestWsRestartAfterIdleRotation(t *testing.T) {
	rawModes(t, func(t *testing.T, raw bool) {
		addr, srv := startWsE2EServer(t, raw, echoDest, 2*time.Second)
		time.Sleep(6 * time.Second) // rotation cycles; retired entries stay queued
		// Make the handle loops dequeue them: a burst of flows.
		for i := 0; i < 8; i++ {
			c, err := net.Dial("tcp", addr)
			require.NoError(t, err)
			c.SetDeadline(time.Now().Add(6 * time.Second))
			c.Write([]byte("x"))
			b := make([]byte, 1)
			_, err = io.ReadFull(c, b)
			require.NoError(t, err)
			c.Close()
		}
		before := srv.gen
		start := time.Now()
		done := make(chan struct{})
		go func() { srv.Restart(); close(done) }()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Fatal("Restart did not return: a worker of the old generation is stuck")
		}
		require.Less(t, time.Since(start), 10*time.Second)
		require.NotSame(t, before, srv.gen, "no new generation was started")
	})
}
