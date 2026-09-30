package transport_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	clienttransport "github.com/musix/backhaul/internal/client/transport"
	servertransport "github.com/musix/backhaul/internal/server/transport"
	"github.com/musix/backhaul/internal/transport/dns/rel"
	"github.com/musix/backhaul/internal/transport/dns/sel"
	"github.com/sirupsen/logrus"
)

func freeTCPAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func startEcho(t *testing.T) (string, func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()
	return l.Addr().String(), func() { _ = l.Close() }
}

func TestDNSMuxEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 59*time.Second)
	defer cancel()

	echoAddr, stopEcho := startEcho(t)
	defer stopEcho()
	dnsAddr := freeTCPAddr(t)
	forwardAddr := freeTCPAddr(t)
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	relCfg := rel.Config{RecvBuf: 256 << 10, SendBuf: 256 << 10, MinRTO: 10 * time.Millisecond, MaxRTO: 200 * time.Millisecond}

	srv := servertransport.NewDNSMuxServer(ctx, &servertransport.DNSMuxConfig{
		TcpMuxConfig: servertransport.TcpMuxConfig{
			Token: "auth-token", Ports: []string{forwardAddr + "=" + echoAddr},
			ChannelSize: 16, MuxCon: 4, MuxVersion: 1,
			MaxFrameSize: 1024, MaxReceiveBuffer: 4 << 20, MaxStreamBuffer: 64 << 10,
			Heartbeat: time.Second,
		},
		Domain: "tunnel.example.test", Key: "dns-secret", Listen: dnsAddr, Rel: relCfg,
	}, logger)
	go srv.Start()

	cli := clienttransport.NewDNSMuxClient(ctx, &clienttransport.DNSMuxConfig{
		TcpMuxConfig: clienttransport.TcpMuxConfig{
			Token: "auth-token", ConnPoolSize: 1, RetryInterval: 20 * time.Millisecond,
			MuxVersion: 1, MaxFrameSize: 1024,
			MaxReceiveBuffer: 4 << 20, MaxStreamBuffer: 64 << 10,
		},
		Domain: "tunnel.example.test", Key: "dns-secret", Timeout: 500 * time.Millisecond,
		Profiles: []sel.Profile{{Resolver: dnsAddr, RRType: 10, Transport: "tcp", Cap: 1200}},
		Rel:      relCfg,
	}, logger)
	go cli.Start()

	var conn net.Conn
	var err error
	for ctx.Err() == nil {
		conn, err = net.DialTimeout("tcp", forwardAddr, 100*time.Millisecond)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("forward listener did not start: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(58 * time.Second))

	want := make([]byte, 64<<10)
	for i := range want {
		want[i] = byte(i * 31)
	}
	if _, err := conn.Write(want); err != nil {
		t.Fatalf("write: %v", err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("64 KiB echo differs")
	}
}
