package handlers

import (
	"context"
	"io"
	"net"
	"sync"

	"github.com/musix/backhaul/internal/web"
	"github.com/sirupsen/logrus"
)

// RawFlowPrefix opens the "raw" form of the ws/wss flow-open message: the text
// frame that tells the client where to connect is "RAW1|<address>", and after it
// neither side frames anything - the tunnel connection is a plain byte pipe for
// the rest of the flow.
//
// That is what lets the server hand the two sockets to the kernel: with a
// *net.TCPConn on each side io.Copy becomes splice(2), so the payload is never
// copied through user space (measured: half the CPU per byte of a copying relay).
// The price is everything that needs to see the bytes: no WebSocket masking, no
// replay, no resume. A tunnel connection that dies takes its flow with it.
const RawFlowPrefix = "RAW1|"

// PrefixedConn is a plain socket with some bytes already read off it that its
// reader must still be given first (the request an HTTP server consumed before
// the connection was taken over). It hides the socket from io.Copy, which is what
// would otherwise switch to splice(2); RawConnectionHandler asks for the pieces
// instead and forwards the prefix itself.
type PrefixedConn interface {
	net.Conn
	// TakePrefix returns the bytes still owed to a reader, and the socket
	// underneath. The caller reads and writes that socket from then on.
	TakePrefix() ([]byte, net.Conn)
}

// RawConnectionHandler moves data between a tunnel connection that has left
// WebSocket framing (see network.WebSocketConn.Detach) and a TCP connection, in
// both directions, and tears the pair down once both have finished. leftover is
// what had already been read off the tunnel socket when the stream turned raw.
//
// A direction that ends half-closes the write side of its destination, so a
// request that ends with a FIN still gets its reply.
func RawConnectionHandler(ctx context.Context, proxyProtocol bool, tunnel net.Conn, leftover []byte, tcpConn net.Conn, logger *logrus.Logger, usage *web.Usage, remotePort int, sniffer bool) {
	var toTunnel []byte // bytes of the local peer already read, to go out first
	if pc, ok := tcpConn.(PrefixedConn); ok {
		toTunnel, tcpConn = pc.TakePrefix()
	}
	if proxyProtocol {
		header, err := ProxyProtocolHeader(tcpConn.RemoteAddr(), tunnel.RemoteAddr())
		if err != nil {
			logger.Error(err)
			tunnel.Close()
			tcpConn.Close()
			return
		}
		if _, err := tunnel.Write(header); err != nil {
			logger.Errorf("failed to send Proxy Protocol v2 header: %v", err)
			tunnel.Close()
			tcpConn.Close()
			return
		}
	}

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			tunnel.Close()
			tcpConn.Close()
		case <-done:
		}
	}()

	account := func(n int64) {
		if sniffer && n > 0 {
			usage.AddOrUpdatePort(remotePort, uint64(n))
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { // tunnel -> local
		defer wg.Done()
		if len(leftover) > 0 {
			if _, err := tcpConn.Write(leftover); err != nil {
				tunnel.Close()
				tcpConn.Close()
				return
			}
			account(int64(len(leftover)))
		}
		n, err := io.Copy(tcpConn, tunnel)
		logger.Debugf("raw tunnel -> local ended: %d bytes, err %v (tunnel %T, local %T)", n, err, tunnel, tcpConn)
		account(n)
		if err != nil {
			logger.Trace("raw tunnel -> local: ", err)
			tunnel.Close()
			tcpConn.Close()
			return
		}
		closeWrite(tcpConn)
	}()
	go func() { // local -> tunnel
		defer wg.Done()
		if len(toTunnel) > 0 {
			if _, err := tunnel.Write(toTunnel); err != nil {
				tunnel.Close()
				tcpConn.Close()
				return
			}
			account(int64(len(toTunnel)))
		}
		n, err := io.Copy(tunnel, tcpConn)
		logger.Debugf("raw local -> tunnel ended: %d bytes, err %v (tunnel %T, local %T)", n, err, tunnel, tcpConn)
		account(n)
		if err != nil {
			logger.Trace("raw local -> tunnel: ", err)
			tunnel.Close()
			tcpConn.Close()
			return
		}
		closeWrite(tunnel)
	}()
	wg.Wait()
	close(done)
	tunnel.Close()
	tcpConn.Close()
}
