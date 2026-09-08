//go:build !windows

package network

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListenWithBuffers(t *testing.T) {
	tests := []struct {
		name        string
		network     string
		address     string
		rcvBufSize  int
		sndBufSize  int
		mss         int
		dis_nodelay bool
		expectErr   bool
	}{
		{
			name:        "Default values",
			network:     "tcp",
			address:     "127.0.0.1:0",
			rcvBufSize:  0,
			sndBufSize:  0,
			mss:         0,
			dis_nodelay: false,
			expectErr:   false,
		},
		{
			name:        "With buffer sizes and MSS",
			network:     "tcp",
			address:     "127.0.0.1:0",
			rcvBufSize:  8192,
			sndBufSize:  8192,
			mss:         1400,
			dis_nodelay: true,
			expectErr:   false,
		},
		{
			name:        "Negative buffer sizes (should be ignored)",
			network:     "tcp",
			address:     "127.0.0.1:0",
			rcvBufSize:  -1,
			sndBufSize:  -1,
			mss:         -1,
			dis_nodelay: false,
			expectErr:   false,
		},
		{
			name:        "Invalid network",
			network:     "invalid_network",
			address:     "127.0.0.1:0",
			rcvBufSize:  0,
			sndBufSize:  0,
			mss:         0,
			dis_nodelay: false,
			expectErr:   true,
		},
		{
			name:        "Invalid address",
			network:     "tcp",
			address:     "127.0.0.1:9999999", // Invalid port
			rcvBufSize:  0,
			sndBufSize:  0,
			mss:         0,
			dis_nodelay: false,
			expectErr:   true,
		},
		{
			name:        "Extremely large buffer sizes (may be ignored/capped by OS, but won't crash)",
			network:     "tcp",
			address:     "127.0.0.1:0",
			rcvBufSize:  1 << 30,
			sndBufSize:  1 << 30,
			mss:         1 << 30,
			dis_nodelay: false,
			expectErr:   false, // Most OS kernels cap the value rather than fail, but it hits the code path
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			listener, err := ListenWithBuffers(tt.network, tt.address, tt.rcvBufSize, tt.sndBufSize, tt.mss, 30*time.Second, tt.dis_nodelay)

			if tt.expectErr {
				assert.Error(t, err)
				assert.Nil(t, listener)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, listener)
			defer listener.Close()

			// Ensure we can connect to the listener
			addr := listener.Addr().String()
			conn, err := net.Dial("tcp", addr)
			require.NoError(t, err, "Failed to dial to the listener")

			err = conn.Close()
			assert.NoError(t, err)
		})
	}
}
