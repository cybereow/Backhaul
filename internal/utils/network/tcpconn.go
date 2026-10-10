package network

import "net"

// underlyingTCP unwraps c down to its TCP socket: through Go's tls.Conn and this
// package's wrappers (NetConn) and the OpenSSL engine's conn (UnderlyingConn).
// It returns nil when c is not built on a TCP socket it can reach.
func underlyingTCP(c net.Conn) *net.TCPConn {
	for i := 0; i < 8 && c != nil; i++ { // a wrapper chain is never this deep
		switch v := c.(type) {
		case *net.TCPConn:
			return v
		case interface{ NetConn() net.Conn }:
			c = v.NetConn()
		case interface{ UnderlyingConn() net.Conn }:
			c = v.UnderlyingConn()
		default:
			return nil
		}
	}
	return nil
}
