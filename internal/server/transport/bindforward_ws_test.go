package transport

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/musix/backhaul/config"
	client_transport "github.com/musix/backhaul/internal/client/transport"
	"github.com/sirupsen/logrus"
)

// A real WebSocket handshake and frames, from a user to a WebSocket service that
// sits behind the real client, all through the bind port with no ports listed.
func TestBindForwardWebSocketEndToEnd(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The tunnel listens on 127.0.0.2:P, the client-side service on 127.0.0.1:P.
	_, port, _ := net.SplitHostPort(lcFreeAddr(t))
	serverAddr := "127.0.0.2:" + port
	backend, err := net.Listen("tcp", "127.0.0.1:"+port)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	go http.Serve(backend, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			msg, op, err := wsutil.ReadClientData(conn)
			if err != nil {
				return
			}
			if wsutil.WriteServerMessage(conn, op, msg) != nil {
				return
			}
		}
	}))

	srv := NewWSMuxServer(ctx, &WsMuxConfig{
		BindAddr: serverAddr, Token: lcToken, Mode: config.WSMUX, Path: "/tun", MuxVersion: 2, MuxCon: 8,
		ChannelSize: 1024, MaxFrameSize: 32768, MaxReceiveBuffer: 4194304, MaxStreamBuffer: 65536,
		KeepAlive: 30 * time.Second, Heartbeat: 20 * time.Second, Nodelay: true, StripeFactor: 1, WSFraming: true,
	}, logger)
	srv.Start()
	lcWaitFor(t, "server", func() bool {
		c, err := net.Dial("tcp", serverAddr)
		if err != nil {
			return false
		}
		c.Close()
		return true
	})
	cli := client_transport.NewWSMuxClient(ctx, &client_transport.WsMuxConfig{
		RemoteAddr: serverAddr, Token: lcToken, Mode: config.WSMUX, Path: "/tun", MuxVersion: 2,
		ConnPoolSize: 3, DialTimeOut: 5 * time.Second, RetryInterval: 100 * time.Millisecond,
		KeepAlive: 30 * time.Second, Nodelay: true, MaxFrameSize: 32768, MaxReceiveBuffer: 4194304,
		MaxStreamBuffer: 65536, StripeFactor: 1, WSFraming: true,
	}, logger)
	go cli.Start()

	var conn net.Conn
	lcWaitFor(t, "a websocket through the bind port", func() bool {
		d := ws.Dialer{Timeout: 2 * time.Second}
		c, _, _, err := d.Dial(ctx, "ws://"+serverAddr+"/app?ed=2048")
		if err != nil {
			return false
		}
		conn = c
		return true
	})
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	for _, size := range []int{5, 100 << 10, 3 << 20} {
		msg := bytes.Repeat([]byte{byte(size)}, size)
		if err := wsutil.WriteClientMessage(conn, ws.OpBinary, msg); err != nil {
			t.Fatalf("write %d: %v", size, err)
		}
		got, _, err := wsutil.ReadServerData(conn)
		if err != nil || !bytes.Equal(got, msg) {
			t.Fatalf("echo of %d bytes: got %d bytes, err %v", size, len(got), err)
		}
	}
}
