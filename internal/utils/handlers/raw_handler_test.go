package handlers

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func tcpPair(t *testing.T) (a, b *net.TCPConn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	ch := make(chan *net.TCPConn, 1)
	go func() {
		c, _ := ln.Accept()
		ch <- c.(*net.TCPConn)
	}()
	c, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })
	s := <-ch
	t.Cleanup(func() { s.Close() })
	return c.(*net.TCPConn), s
}

func quietLogger() *logrus.Logger {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return l
}

// rawFlow wires peerT <-tunnel-> handler <-local-> peerL and returns the two
// outer ends. The handler runs until both directions end.
func rawFlow(t *testing.T, leftover []byte, wrapLocal func(net.Conn) net.Conn) (peerT, peerL *net.TCPConn, done <-chan struct{}) {
	t.Helper()
	peerT, tunnelEnd := tcpPair(t)
	peerL, localEnd := tcpPair(t)
	var local net.Conn = localEnd
	if wrapLocal != nil {
		local = wrapLocal(localEnd)
	}
	d := make(chan struct{})
	go func() {
		defer close(d)
		RawConnectionHandler(context.Background(), false, tunnelEnd, leftover, local, quietLogger(), nil, 0, false)
	}()
	return peerT, peerL, d
}

func TestRawHandlerBothDirections(t *testing.T) {
	peerT, peerL, done := rawFlow(t, nil, nil)
	down := make([]byte, 3<<20)
	up := make([]byte, 2<<20)
	rand.Read(down)
	rand.Read(up)
	go func() { peerT.Write(down); peerT.CloseWrite() }()
	go func() { peerL.Write(up); peerL.CloseWrite() }()
	gotL, err := io.ReadAll(peerL)
	require.NoError(t, err)
	gotT, err := io.ReadAll(peerT)
	require.NoError(t, err)
	require.True(t, bytes.Equal(down, gotL), "tunnel -> local differs")
	require.True(t, bytes.Equal(up, gotT), "local -> tunnel differs")
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not finish after both sides ended")
	}
}

// Bytes already read off the tunnel socket come out before anything else.
func TestRawHandlerLeftoverFirst(t *testing.T) {
	peerT, peerL, _ := rawFlow(t, []byte("HEAD-"), nil)
	go func() { peerT.Write([]byte("tail")); peerT.CloseWrite() }()
	got, err := io.ReadAll(peerL)
	require.NoError(t, err)
	require.Equal(t, "HEAD-tail", string(got))
}

type testPrefixConn struct {
	net.Conn
	pre []byte
}

func (c *testPrefixConn) TakePrefix() ([]byte, net.Conn) { return c.pre, c.Conn }

// A local socket with a prefix (what an HTTP server already read) sends the
// prefix first, then the rest, and the socket underneath is what gets spliced.
func TestRawHandlerPrefixedLocal(t *testing.T) {
	peerT, peerL, _ := rawFlow(t, nil, func(c net.Conn) net.Conn {
		return &testPrefixConn{Conn: c, pre: []byte("GET / HTTP/1.1\r\n\r\n")}
	})
	go func() { peerL.Write([]byte("BODY")); peerL.CloseWrite() }()
	got, err := io.ReadAll(peerT)
	require.NoError(t, err)
	require.Equal(t, "GET / HTTP/1.1\r\n\r\nBODY", string(got))
}

// A FIN on one side reaches the other as EOF while the reverse direction keeps
// working: the late reply is not cut.
func TestRawHandlerHalfClose(t *testing.T) {
	peerT, peerL, done := rawFlow(t, nil, nil)
	_, err := peerL.Write([]byte("request"))
	require.NoError(t, err)
	require.NoError(t, peerL.CloseWrite())
	req, err := io.ReadAll(peerT) // sees EOF after the request
	require.NoError(t, err)
	require.Equal(t, "request", string(req))
	time.Sleep(100 * time.Millisecond)
	reply := make([]byte, 1<<20)
	rand.Read(reply)
	go func() { peerT.Write(reply); peerT.CloseWrite() }()
	got, err := io.ReadAll(peerL)
	require.NoError(t, err)
	require.True(t, bytes.Equal(reply, got))
	<-done
}

// A tunnel that dies mid-flow takes the local socket with it, and the handler
// returns: nothing is left running.
func TestRawHandlerTunnelDeathEndsFlow(t *testing.T) {
	before := runtime.NumGoroutine()
	peerT, peerL, done := rawFlow(t, nil, nil)
	peerL.Write([]byte("x"))
	peerT.SetLinger(0)
	peerT.Close() // RST
	peerL.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, _ = io.Copy(io.Discard, peerL) // ends with EOF or reset, but ends
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler still running after the tunnel died")
	}
	time.Sleep(100 * time.Millisecond)
	require.LessOrEqual(t, runtime.NumGoroutine(), before+3, "goroutines leaked")
}

func TestRawHandlerContextCancelClosesBoth(t *testing.T) {
	peerT, tunnelEnd := tcpPair(t)
	peerL, localEnd := tcpPair(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		RawConnectionHandler(ctx, false, tunnelEnd, nil, localEnd, quietLogger(), nil, 0, false)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler ignored cancellation")
	}
	peerT.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err := peerT.Read(make([]byte, 1))
	require.Error(t, err)
	peerL.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = peerL.Read(make([]byte, 1))
	require.Error(t, err)
}

func TestRawHandlerProxyProtocolHeaderFirst(t *testing.T) {
	peerT, tunnelEnd := tcpPair(t)
	peerL, localEnd := tcpPair(t)
	go RawConnectionHandler(context.Background(), true, tunnelEnd, nil, localEnd, quietLogger(), nil, 0, false)
	hdr := make([]byte, 12)
	peerT.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err := io.ReadFull(peerT, hdr)
	require.NoError(t, err)
	require.Equal(t, []byte("\r\n\r\n\x00\r\nQUIT\n"), hdr, "PROXY v2 signature expected first on the tunnel")
	peerL.Close()
}
