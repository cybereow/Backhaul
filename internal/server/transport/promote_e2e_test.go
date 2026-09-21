package transport

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/musix/backhaul/config"
	client_transport "github.com/musix/backhaul/internal/client/transport"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// TestWSMuxStripedTransferE2E transfers 2MB each way over a flow that is striped
// from its first byte: StripeFactor 2 with no StripePorts filter, so shouldStripe
// is true and the promotion path is never taken. Promotion itself is covered by
// TestWSMuxPromotionE2E in wsmux_promotion_test.go.
func TestWSMuxStripedTransferE2E(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Setup local destination server
	destServer, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	defer destServer.Close()
	destAddr := destServer.Addr().String()

	// We'll generate 2MB of random data
	const payloadSize = 2 * 1024 * 1024
	serverPayload := make([]byte, payloadSize)
	clientPayload := make([]byte, payloadSize)
	rand.Read(serverPayload)
	rand.Read(clientPayload)

	var serverReceived []byte
	var wg sync.WaitGroup

	// Start destination server logic
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := destServer.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// Write serverPayload
		go func() {
			conn.Write(serverPayload)
			if cw, ok := conn.(interface{ CloseWrite() error }); ok {
				cw.CloseWrite()
			} else {
				conn.Close()
			}
		}()

		// Read clientPayload
		recv, _ := io.ReadAll(conn)
		serverReceived = recv
	}()

	serverTunnelListener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	serverTunnelPort := serverTunnelListener.Addr().(*net.TCPAddr).Port
	serverTunnelListener.Close() // find open port for tunnel

	serverListener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	serverPort := serverListener.Addr().(*net.TCPAddr).Port
	serverListener.Close() // find open port for public

	// 2. Setup server WsMux
	serverConfig := &WsMuxConfig{
		BindAddr:         fmt.Sprintf("127.0.0.1:%d", serverTunnelPort),
		MuxVersion:       2,
		MuxCon:           16, // Enough for multiple legs
		ChannelSize:      100,
		MaxFrameSize:     32768,
		MaxReceiveBuffer: 4194304,
		MaxStreamBuffer:  65536,
		StripeFactor:     2,
		StripeParity:     0,
		Mode:             config.WSMUX,
		KeepAlive:        30 * time.Second,
		Heartbeat:        30 * time.Second,
		Token:            "test_token",
		PromoteBytes:     512 * 1024, // Promote midway
		Ports:            []string{fmt.Sprintf("127.0.0.1:%d=%s", serverPort, destAddr)},
	}
	server := NewWSMuxServer(ctx, serverConfig, logger)
	server.Start()

	// Wait a moment for server to bind its tunnel listener
	time.Sleep(1 * time.Second)

	// 3. Setup client WsMux
	clientConfig := &client_transport.WsMuxConfig{
		RemoteAddr:       fmt.Sprintf("127.0.0.1:%d", serverTunnelPort),
		MuxVersion:       2,
		ConnPoolSize:     8,
		DialTimeOut:      10 * time.Second,
		KeepAlive:        30 * time.Second,
		MaxFrameSize:     32768,
		MaxReceiveBuffer: 4194304,
		MaxStreamBuffer:  65536,
		StripeFactor:     2,
		StripeParity:     0,
		Mode:             config.WSMUX,
		Path:             "/",
		Token:            "test_token",
	}
	clientLogger := logrus.New()
	clientLogger.SetLevel(logrus.DebugLevel)
	client := client_transport.NewWSMuxClient(ctx, clientConfig, clientLogger)

	// Start client
	go client.Start()

	// Wait for control channel and pool to be ready and server listener to bind
	var clientLocalWrite net.Conn
	var dialErr error
	for i := 0; i < 100; i++ {
		clientLocalWrite, dialErr = net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", serverPort))
		if dialErr == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	assert.NoError(t, dialErr, "Server public listener never came up")

	// Send clientPayload
	go func() {
		if clientLocalWrite != nil {
			clientLocalWrite.Write(clientPayload)
			if cw, ok := clientLocalWrite.(interface{ CloseWrite() error }); ok {
				cw.CloseWrite()
			} else {
				clientLocalWrite.Close()
			}
		}
	}()

	var clientReceived []byte
	if clientLocalWrite != nil {
		clientReceived, _ = io.ReadAll(clientLocalWrite)
	}

	wg.Wait()

	assert.Equal(t, len(serverPayload), len(clientReceived), "Download length mismatch")
	if len(serverPayload) == len(clientReceived) {
		assert.True(t, bytes.Equal(serverPayload, clientReceived), "Download payload mismatch")
	}

	assert.Equal(t, len(clientPayload), len(serverReceived), "Upload length mismatch")
	if len(clientPayload) == len(serverReceived) {
		assert.True(t, bytes.Equal(clientPayload, serverReceived), "Upload payload mismatch")
	}
}
