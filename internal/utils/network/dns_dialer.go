package network

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net"
	"time"
)

func generateConnID() string {
	b := make([]byte, 4)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func DNSDialer(ctx context.Context, serverAddr, domain string, timeout time.Duration) (net.Conn, error) {
	connID := generateConnID()

	// Create the connection
	conn := NewDNSConnClient(connID, domain, serverAddr)

	// Start polling in background to receive server pushed data
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond) // Poll every 500ms
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				conn.Close()
				return
			case <-conn.closeChan:
				return
			case <-ticker.C:
				conn.pollServer()
			}
		}
	}()

	// Initial poll to establish connection (dummy)
	err := conn.pollServer()
	if err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
}
