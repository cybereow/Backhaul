package transport

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"reflect"
	"testing"
	"time"

	clienttransport "github.com/musix/backhaul/internal/client/transport"

	"github.com/sirupsen/logrus"
	"github.com/xtaci/smux"
)

func TestExpandPorts(t *testing.T) {
	got, err := expandPorts([]string{"8080", "9000=10.0.0.1:80", "7000-7002", "6000-6001=host:1", "127.0.0.1:5000=x:5"})
	if err != nil {
		t.Fatal(err)
	}
	want := []dnsPortMap{
		{":8080", "8080"}, {":9000", "10.0.0.1:80"},
		{":7000", "7000"}, {":7001", "7001"}, {":7002", "7002"},
		{":6000", "host:1"}, {":6001", "host:1"},
		{"127.0.0.1:5000", "x:5"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v\nwant %v", got, want)
	}
	for _, bad := range []string{"0", "70000", "5-3", "abc", "a-b", "1.2.3.4:80"} {
		if _, err := expandPorts([]string{bad}); err == nil {
			t.Errorf("expandPorts(%q) accepted", bad)
		}
	}
}

func freeAddr(t *testing.T, network string) string {
	t.Helper()
	return testAddr(t, network)
}

// TestDnsMuxEndToEnd runs a real forward through the whole stack: TCP client ->
// server listener -> smux -> DNS carrier (real UDP on loopback, the responder
// itself standing in for the recursive resolver) -> client -> TCP echo server.
func TestDnsMuxEndToEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	echo, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer echo.Close()
	go func() {
		for {
			c, err := echo.Accept()
			if err != nil {
				return
			}
			go func() { defer c.Close(); io.Copy(c, c) }()
		}
	}()

	dnsAddr := freeAddr(t, "udp")
	fwdAddr := freeAddr(t, "tcp")
	_, fwdPort, _ := net.SplitHostPort(fwdAddr)

	const domain, key, token = "t.example.com", "dns-key", "tok"

	srv := NewDnsMuxServer(ctx, &DnsMuxConfig{
		Domain: domain, Key: key, Listen: dnsAddr, Token: token,
		Ports: []string{fmt.Sprintf("%s=%s", fwdPort, echo.Addr().String())},
	}, logger)
	go srv.Start()
	defer srv.Close()

	cli := clienttransport.NewDnsMuxClient(ctx, &clienttransport.DnsMuxConfig{
		Domain: domain, Key: key, Token: token,
		Resolvers:     []string{dnsAddr},
		RecordTypes:   []string{"TXT"},
		Timeout:       2 * time.Second,
		RetryInterval: time.Second,
	}, logger)
	cli.Start()
	defer cli.Close()

	payload := make([]byte, 16*1024)
	rand.Read(payload)

	deadline := time.Now().Add(90 * time.Second)
	var got []byte
	for {
		if time.Now().After(deadline) {
			t.Fatal("no echo through the dnsmux tunnel before the deadline")
		}
		got, err = roundTrip(fwdAddr, payload)
		if err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond) // tunnel not up yet: the server drops the conn
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("echoed bytes differ from what was sent")
	}
}

func roundTrip(addr string, payload []byte) ([]byte, error) {
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(60 * time.Second))
	go func() { c.Write(payload) }()
	buf := make([]byte, len(payload))
	if _, err := io.ReadFull(c, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func TestPickSkipsClosedAndExcludedSessions(t *testing.T) {
	mk := func() *smux.Session {
		a, b := net.Pipe()
		t.Cleanup(func() { a.Close(); b.Close() })
		ses, err := smux.Client(a, smux.DefaultConfig())
		if err != nil {
			t.Fatal(err)
		}
		return ses
	}
	dead, live := mk(), mk()
	dead.Close()
	s := &DnsMuxTransport{sessions: []*dnsSession{{mux: dead}, {mux: live}}}
	if got := s.pick(nil); got != live {
		t.Fatal("pick returned a closed session")
	}
	if got := s.pick(map[*smux.Session]bool{live: true}); got != nil {
		t.Fatal("pick returned an excluded session")
	}
}
