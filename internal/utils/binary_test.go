package utils

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// mockErrorConn is a mock net.Conn that returns an error on Read.
type mockErrorConn struct{}

func (m *mockErrorConn) Read(b []byte) (n int, err error) {
	return 0, errors.New("mock read error")
}
func (m *mockErrorConn) Write(b []byte) (n int, err error) {
	return len(b), nil
}
func (m *mockErrorConn) Close() error {
	return nil
}
func (m *mockErrorConn) LocalAddr() net.Addr {
	return nil
}
func (m *mockErrorConn) RemoteAddr() net.Addr {
	return nil
}
func (m *mockErrorConn) SetDeadline(t time.Time) error {
	return nil
}
func (m *mockErrorConn) SetReadDeadline(t time.Time) error {
	return nil
}
func (m *mockErrorConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func TestReceiveBinaryByte_ErrorPath(t *testing.T) {
	conn := &mockErrorConn{}
	b, err := ReceiveBinaryByte(conn)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mock read error")
	assert.Equal(t, byte(0), b)
}

// TestFlowPlainHCHeader pins the FlowPlainHC wire header: kind 0x06 followed by
// the exact FlowPlain layout (8-byte flowID, 2-byte address length, address),
// so the two kinds differ only in the first byte and share one receiver.
func TestFlowPlainHCHeader(t *testing.T) {
	assert.Equal(t, byte(0x06), FlowPlainHC)
	assert.NotEqual(t, FlowPlain, FlowPlainHC)

	send := func(t *testing.T, f func(net.Conn, uint64, string) error, flowID uint64, addr string) []byte {
		t.Helper()
		a, b := net.Pipe()
		defer a.Close()
		defer b.Close()
		out := make(chan []byte, 1)
		go func() {
			buf := make([]byte, 11+len(addr))
			b.SetReadDeadline(time.Now().Add(5 * time.Second))
			n, _ := io.ReadFull(b, buf)
			out <- buf[:n]
		}()
		assert.NoError(t, f(a, flowID, addr))
		return <-out
	}

	const addr = "127.0.0.1:8080"
	plain := send(t, SendFlowPlain, 0, addr)
	hc := send(t, SendFlowPlainHC, 0, addr)
	assert.Equal(t, byte(FlowPlain), plain[0])
	assert.Equal(t, byte(0x06), hc[0])
	assert.Equal(t, plain[1:], hc[1:], "headers must differ only in the kind byte")

	// Round trip through the receiver: kind via ReadFlowKind, then the header.
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	go SendFlowPlainHC(a, 0, addr)
	b.SetReadDeadline(time.Now().Add(5 * time.Second))
	kind, err := ReadFlowKind(b)
	assert.NoError(t, err)
	assert.Equal(t, FlowPlainHC, kind)
	flowID, gotAddr, err := ReceiveFlowPlainHC(b)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), flowID)
	assert.Equal(t, addr, gotAddr)
}
