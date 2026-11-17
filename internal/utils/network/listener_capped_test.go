//go:build linux

package network

import (
	"net"
	"testing"

	"golang.org/x/sys/unix"
)

func recvBuf(t *testing.T, c net.Conn) int {
	t.Helper()
	raw, err := c.(*net.TCPConn).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var v int
	var gerr error
	if err := raw.Control(func(fd uintptr) {
		v, gerr = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_RCVBUF)
	}); err != nil {
		t.Fatal(err)
	}
	if gerr != nil {
		t.Fatal(gerr)
	}
	return v
}

// A connection accepted from a capped listener has its buffers held to the cap
// (the kernel reports twice what was set, or what it was allowed to set), and
// the listener otherwise behaves like any other.
func TestListenCappedHoldsAcceptedBuffers(t *testing.T) {
	const capBytes = 256 << 10
	ln, err := ListenCapped("127.0.0.1:0", capBytes, capBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := net.Dial("tcp", ln.Addr().String())
		if err == nil {
			defer c.Close()
			c.Write([]byte("x"))
			buf := make([]byte, 1)
			c.Read(buf)
		}
	}()
	c, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	buf := make([]byte, 1)
	if _, err := c.Read(buf); err != nil || buf[0] != 'x' {
		t.Fatalf("a capped listener did not carry data: %v", err)
	}
	if got := recvBuf(t, c); got > 2*capBytes+8<<10 {
		t.Fatalf("accepted socket has a %d byte receive buffer, want at most about %d", got, 2*capBytes)
	}
	c.Write([]byte("y"))
}
