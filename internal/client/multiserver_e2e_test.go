package client

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/musix/backhaul/config"
	server_transport "github.com/musix/backhaul/internal/server/transport"
	"github.com/sirupsen/logrus"
)

// freePort returns a bindable TCP port below the ephemeral range, so outgoing
// connections of other tests cannot take it between pick and bind (see
// testPort in the server transport tests).
func freePort(t *testing.T) int {
	t.Helper()
	for i := 0; i < 200; i++ {
		port := 20000 + rand.Intn(12000)
		l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			continue
		}
		l.Close()
		return port
	}
	t.Fatal("no free port")
	return 0
}

// echoBackend accepts connections and echoes them, prefixing replies with tag
// so a test can tell which backend - which server's tunnel - answered.
func echoBackend(t *testing.T, tag string) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				buf := make([]byte, 4096)
				for {
					n, err := c.Read(buf)
					if n > 0 {
						if _, werr := c.Write(append([]byte(tag+":"), buf[:n]...)); werr != nil {
							return
						}
					}
					if err != nil {
						return
					}
				}
			}()
		}
	}()
	return l.Addr().String()
}

type testServer struct {
	tunnelAddr string
	publicAddr string
	cancel     context.CancelFunc
}

// startServer runs a real wsmux server on tunnelPort whose public port forwards
// to backend. A cancelled server can be started again on the same ports.
func startServer(t *testing.T, tunnelPort, publicPort int, token, backend string) *testServer {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)
	srv := server_transport.NewWSMuxServer(ctx, &server_transport.WsMuxConfig{
		BindAddr:         fmt.Sprintf("127.0.0.1:%d", tunnelPort),
		Token:            token,
		Mode:             config.WSMUX,
		Path:             "/",
		MuxVersion:       2,
		MuxCon:           8,
		ChannelSize:      100,
		MaxFrameSize:     32768,
		MaxReceiveBuffer: 4194304,
		MaxStreamBuffer:  65536,
		StripeFactor:     1,
		KeepAlive:        30 * time.Second,
		Heartbeat:        2 * time.Second,
		WSFraming:        true,
		Ports:            []string{fmt.Sprintf("127.0.0.1:%d=%s", publicPort, backend)},
	}, logger)
	go srv.Start()
	s := &testServer{
		tunnelAddr: fmt.Sprintf("127.0.0.1:%d", tunnelPort),
		publicAddr: fmt.Sprintf("127.0.0.1:%d", publicPort),
		cancel:     cancel,
	}
	t.Cleanup(cancel)
	return s
}

// roundTrip sends msg through the server's public port and returns the reply.
func roundTrip(addr, msg string) (string, error) {
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return "", err
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.Write([]byte(msg)); err != nil {
		return "", err
	}
	buf := make([]byte, 4096)
	n, err := io.ReadAtLeast(c, buf, 1)
	if err != nil {
		return "", err
	}
	return string(buf[:n]), nil
}

// waitRoundTrip retries until a round trip through addr returns want.
func waitRoundTrip(t *testing.T, what, addr, msg, want string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var got string
	var err error
	for time.Now().Before(deadline) {
		got, err = roundTrip(addr, msg)
		if err == nil && got == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s: last reply %q, err %v; want %q", what, got, err, want)
}

// TestMultiServerClientE2E runs one client against two independent servers
// (different tokens) and checks that both tunnels carry traffic, that losing
// one server leaves the other untouched, and that the lost one recovers on its
// own once it comes back.
func TestMultiServerClientE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("end-to-end")
	}
	tunA, pubA := freePort(t), freePort(t)
	tunB, pubB := freePort(t), freePort(t)
	backendA := echoBackend(t, "A")
	backendB := echoBackend(t, "B")

	srvA := startServer(t, tunA, pubA, "token-a", backendA)
	srvB := startServer(t, tunB, pubB, "token-b", backendB)

	cfg := &config.ClientConfig{
		Transport:           config.WSMUX,
		LogLevel:            "fatal",
		ConnectionPool:      2,
		RetryInterval:       1,
		DialTimeout:         2,
		Keepalive:           30,
		MuxVersion:          2,
		MaxFrameSize:        32768,
		MaxReceiveBuffer:    4194304,
		MaxStreamBuffer:     65536,
		StripeFactor:        1,
		Path:                "/",
		MuxWSFraming:        true,
		MuxStealthHandshake: true,
		ResumeWindow:        30,
		Servers: []config.ClientServer{
			{Name: "a", RemoteAddr: srvA.tunnelAddr, Token: "token-a"},
			{Name: "b", RemoteAddr: srvB.tunnelAddr, Token: "token-b"},
		},
	}
	if _, err := ResolveServers(cfg); err != nil {
		t.Fatal(err)
	}
	c := NewClient(cfg, context.Background())
	go c.Start()
	t.Cleanup(c.Stop)

	waitRoundTrip(t, "server A tunnel", srvA.publicAddr, "hello", "A:hello")
	waitRoundTrip(t, "server B tunnel", srvB.publicAddr, "hello", "B:hello")
	if len(c.tunnelLoggers) != 2 || c.tunnelLoggers[0] == c.tunnelLoggers[1] {
		t.Fatal("each tunnel must have its own logger")
	}

	// Server A goes away. B must keep working the whole time A's tunnel is
	// failing, reconnecting and restarting.
	srvA.cancel()
	stop := time.Now().Add(6 * time.Second)
	for time.Now().Before(stop) {
		if got, err := roundTrip(srvB.publicAddr, "still"); err != nil || got != "B:still" {
			t.Fatalf("server B disturbed while A is down: reply %q, err %v", got, err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// A comes back on the same address; its tunnel reconnects by itself.
	srvA = startServer(t, tunA, pubA, "token-a", backendA)
	waitRoundTrip(t, "server A after it came back", srvA.publicAddr, "again", "A:again")
	waitRoundTrip(t, "server B after A came back", srvB.publicAddr, "again", "B:again")
}
