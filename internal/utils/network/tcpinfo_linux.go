//go:build linux

package network

import (
	"net"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// TCPDelivery is the kernel's view of how a TCP connection is delivering.
type TCPDelivery struct {
	// BytesAcked counts every byte the peer has acknowledged.
	BytesAcked uint64
	// Busy is how long the connection has had data waiting to be sent or
	// acknowledged, in total.
	Busy time.Duration
	// NotSent is what sits in the send queue that TCP has not sent yet: more than
	// nothing means the path, not the sender, is setting the pace right now.
	NotSent uint32
	// AppLimited is set when the latest delivery-rate sample was taken with less
	// to send than the path would take.
	AppLimited bool
}

// tcpInfoFlagsOffset is the byte of struct tcp_info that carries
// tcpi_delivery_rate_app_limited in its lowest bit. x/sys leaves it (and the
// window-scale byte before it) as padding after Options, so it is read by offset.
const tcpInfoFlagsOffset = 7

// TCPDeliveryInfo reads TCP_INFO from the TCP connection under c. c may be the
// socket itself or a wrapper that exposes it (a TLS connection of either engine,
// a WebSocket connection). ok is false when no TCP socket can be reached.
func TCPDeliveryInfo(c net.Conn) (d TCPDelivery, ok bool) {
	tc := underlyingTCP(c)
	if tc == nil {
		return d, false
	}
	raw, err := tc.SyscallConn()
	if err != nil {
		return d, false
	}
	cerr := raw.Control(func(fd uintptr) {
		info, err := unix.GetsockoptTCPInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_INFO)
		if err != nil {
			return
		}
		flags := *(*uint8)(unsafe.Add(unsafe.Pointer(info), tcpInfoFlagsOffset))
		d = TCPDelivery{
			BytesAcked: info.Bytes_acked,
			Busy:       time.Duration(info.Busy_time) * time.Microsecond,
			NotSent:    info.Notsent_bytes,
			AppLimited: flags&1 != 0,
		}
		ok = true
	})
	if cerr != nil {
		return TCPDelivery{}, false
	}
	return d, ok
}
