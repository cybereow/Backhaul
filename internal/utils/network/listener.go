//go:build !windows
// +build !windows

package network

import (
	"context"
	"fmt"
	"net"
	"syscall"
	"time"
)

// listenWithBuffers creates a TCP listener with specified SO_RCVBUF and SO_SNDBUF sizes.
// It returns a net.Listener and an error if any.
func ListenWithBuffers(network, address string, rcvBufSize, sndBufSize, mss int, keepAlivePeriod time.Duration, dis_nodelay bool) (net.Listener, error) {
	// Options
	listenerCfg := &net.ListenConfig{
		Control: func(network, address string, s syscall.RawConn) error {
			// Set socket options for SO_REUSEADDR and SO_REUSEPORT
			err := ReusePortControl(network, address, s)
			if err != nil {
				return err
			}

			// Set SO_RCVBUF (via SO_RCVBUFFORCE when privileged; see setRecvBuf).
			// Accepted connections inherit the listener's buffer sizes, so forcing
			// them here lifts the rmem_max clamp for every wssmux leg the server
			// accepts.
			if rcvBufSize > 0 {
				err = s.Control(func(fd uintptr) {
					if e := setRecvBuf(int(fd), rcvBufSize); e != nil {
						err = fmt.Errorf("failed to set SO_RCVBUF: %v", e)
					}
				})
			}
			if err != nil {
				return err
			}

			// Set SO_SNDBUF (via SO_SNDBUFFORCE when privileged; see setSendBuf).
			if sndBufSize > 0 {
				err = s.Control(func(fd uintptr) {
					if e := setSendBuf(int(fd), sndBufSize); e != nil {
						err = fmt.Errorf("failed to set SO_SNDBUF: %v", e)
					}
				})
			}

			// Set MSS (Maximum Segment Size)
			if mss > 0 {
				err = s.Control(func(fd uintptr) {
					if err = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_MAXSEG, mss); err != nil {
						err = fmt.Errorf("failed to set MSS: %v", err)
					}
				})
			}

			// Set TCP_NODELAY based on tcpNoDelay flag (enabled or disabled)
			if !dis_nodelay {
				err = s.Control(func(fd uintptr) {
					if err = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1); err != nil {
						err = fmt.Errorf("failed to set TCP_NODELAY: %v", err)
					}
				})
			} else {
				err = s.Control(func(fd uintptr) {
					if err = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 0); err != nil {
						err = fmt.Errorf("failed to unset TCP_NODELAY: %v", err)
					}
				})
			}

			return err

		},
		KeepAliveConfig: net.KeepAliveConfig{Enable: true, Interval: keepAlivePeriod, Count: 9, Idle: 0},
	}

	// Dial the TCP connection with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	listener, err := listenerCfg.Listen(ctx, network, address)
	if err != nil {
		return nil, err
	}

	return listener, err
}

// ListenCapped is net.Listen("tcp", address) with the receive and send buffer of
// every accepted connection held to rcv and snd bytes (0 = left to the kernel).
//
// A socket left to autotuning may take megabytes for itself (tcp_rmem and
// tcp_wmem allow it), and the kernel charges them to the container: on a server
// held to a couple of hundred megabytes, a pool of sockets whose reader is
// starved for CPU fills them and the container is killed. A cap on the buffer is
// also what pushes back on a fast peer, through the TCP window, in time.
func ListenCapped(address string, rcv, snd int) (net.Listener, error) {
	lc := &net.ListenConfig{
		Control: func(network, addr string, c syscall.RawConn) error {
			var err error
			if cerr := c.Control(func(fd uintptr) {
				if rcv > 0 {
					if e := setRecvBuf(int(fd), rcv); e != nil {
						err = fmt.Errorf("failed to set SO_RCVBUF: %v", e)
						return
					}
				}
				if snd > 0 {
					if e := setSendBuf(int(fd), snd); e != nil {
						err = fmt.Errorf("failed to set SO_SNDBUF: %v", e)
					}
				}
			}); cerr != nil {
				return cerr
			}
			return err
		},
	}
	return lc.Listen(context.Background(), "tcp", address)
}
