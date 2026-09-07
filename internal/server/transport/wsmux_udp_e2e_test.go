package transport

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/musix/backhaul/config"
	client_transport "github.com/musix/backhaul/internal/client/transport"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// TestWSMuxUDPForwardingE2E stands up a real wssmux(ws) server+client pair with
// accept_udp enabled and checks that a UDP datagram sent to the server's public
// port is forwarded over the mux tunnel to a local UDP echo service and the
// reply comes back the whole way. This is the L2TP-style path (UDP carried over
// wssmux).
func TestWSMuxUDPForwardingE2E(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Local UDP echo destination (stands in for the L2TP service).
	destConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	assert.NoError(t, err)
	defer destConn.Close()
	destAddr := destConn.LocalAddr().String()

	go func() {
		buf := make([]byte, 64*1024)
		for {
			n, addr, err := destConn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			// Echo the payload back with a prefix so we know it round-tripped.
			reply := append([]byte("echo:"), buf[:n]...)
			if _, err := destConn.WriteToUDP(reply, addr); err != nil {
				return
			}
		}
	}()

	// Pick free ports for the tunnel and the public UDP port.
	tunnelLn, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	tunnelPort := tunnelLn.Addr().(*net.TCPAddr).Port
	tunnelLn.Close()

	pubConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	assert.NoError(t, err)
	pubPort := pubConn.LocalAddr().(*net.UDPAddr).Port
	pubConn.Close() // free it for the server to bind

	// 2. Server with accept_udp on.
	serverConfig := &WsMuxConfig{
		BindAddr:         fmt.Sprintf("127.0.0.1:%d", tunnelPort),
		MuxVersion:       2,
		MuxCon:           16,
		ChannelSize:      100,
		MaxFrameSize:     32768,
		MaxReceiveBuffer: 4194304,
		MaxStreamBuffer:  65536,
		StripeFactor:     1,
		Mode:             config.WSMUX,
		KeepAlive:        30 * time.Second,
		Heartbeat:        30 * time.Second,
		Token:            "test_token",
		AcceptUDP:        true,
		Ports:            []string{fmt.Sprintf("127.0.0.1:%d=%s", pubPort, destAddr)},
	}
	server := NewWSMuxServer(ctx, serverConfig, logger)
	server.Start()

	time.Sleep(1 * time.Second) // let the tunnel listener bind

	// 3. Client.
	clientConfig := &client_transport.WsMuxConfig{
		RemoteAddr:       fmt.Sprintf("127.0.0.1:%d", tunnelPort),
		MuxVersion:       2,
		ConnPoolSize:     8,
		DialTimeOut:      10 * time.Second,
		KeepAlive:        30 * time.Second,
		MaxFrameSize:     32768,
		MaxReceiveBuffer: 4194304,
		MaxStreamBuffer:  65536,
		StripeFactor:     1,
		Mode:             config.WSMUX,
		Path:             "/",
		Token:            "test_token",
	}
	client := client_transport.NewWSMuxClient(ctx, clientConfig, logger)
	go client.Start()

	// 4. Send a datagram to the server's public UDP port and wait for the echo.
	serverUDPAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("127.0.0.1:%d", pubPort))
	assert.NoError(t, err)

	want := []byte("l2tp-hello")
	wantReply := append([]byte("echo:"), want...)

	var got []byte
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		sender, derr := net.DialUDP("udp", nil, serverUDPAddr)
		if derr != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}

		_, _ = sender.Write(want)
		_ = sender.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		buf := make([]byte, 64*1024)
		n, rerr := sender.Read(buf)
		sender.Close()
		if rerr == nil && n > 0 {
			got = append([]byte(nil), buf[:n]...)
			break
		}
		// Pool may still be warming up; retry with a fresh datagram.
		time.Sleep(200 * time.Millisecond)
	}

	assert.NotNil(t, got, "no UDP reply received through the wssmux tunnel")
	if got != nil {
		assert.True(t, bytes.Equal(wantReply, got), "UDP echo payload mismatch: got %q want %q", got, wantReply)
	}

	// 5. Burst exchange on ONE socket: a stateful handshake (IKE) sends several
	// datagrams back and forth in quick succession from the same source. This
	// guards against the flow being torn down and re-created mid-exchange (the
	// clock-skew "congestion" churn that broke IKE): every datagram in the burst
	// must round-trip on the same flow.
	burst, err := net.DialUDP("udp", nil, serverUDPAddr)
	assert.NoError(t, err)
	defer burst.Close()

	const rounds = 10
	rbuf := make([]byte, 64*1024)
	for i := 0; i < rounds; i++ {
		msg := []byte(fmt.Sprintf("pkt-%d", i))
		_, werr := burst.Write(msg)
		assert.NoError(t, werr)

		_ = burst.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, rerr := burst.Read(rbuf)
		assert.NoError(t, rerr, "no echo for burst packet %d (flow likely churned)", i)
		if rerr == nil {
			assert.True(t, bytes.Equal(append([]byte("echo:"), msg...), rbuf[:n]),
				"burst packet %d echo mismatch: got %q", i, rbuf[:n])
		}
		time.Sleep(20 * time.Millisecond) // rapid, like handshake retransmits
	}
}
