package network

import (
	"crypto/tls"
	"net"
	"time"
)

// TuneUserConn gives an already-accepted user socket the same options a socket
// accepted on a mapped `ports` listener gets: the configured SO_RCVBUF/SO_SNDBUF
// (0 keeps autotuning), TCP_NODELAY and keep-alive. It is for sockets that arrive
// on a listener not built with ListenWithBuffers. A TLS conn is tuned through
// the socket under it; anything that is not TCP underneath is left untouched.
func TuneUserConn(conn net.Conn, rcvBuf, sndBuf int, nodelay bool, keepAlive time.Duration) {
	for {
		tc, ok := conn.(*tls.Conn)
		if !ok {
			break
		}
		conn = tc.NetConn()
	}
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	_ = tcp.SetNoDelay(nodelay)
	_ = tcp.SetKeepAlive(true)
	if keepAlive > 0 {
		_ = tcp.SetKeepAlivePeriod(keepAlive)
	}
	if rcvBuf <= 0 && sndBuf <= 0 {
		return
	}
	raw, err := tcp.SyscallConn()
	if err != nil {
		return
	}
	_ = raw.Control(func(fd uintptr) {
		if rcvBuf > 0 {
			_ = setRecvBuf(int(fd), rcvBuf)
		}
		if sndBuf > 0 {
			_ = setSendBuf(int(fd), sndBuf)
		}
	})
}
