//go:build !linux

package network

import (
	"net"
	"time"
)

// TCPDelivery mirrors the Linux type; it is never filled in here.
type TCPDelivery struct {
	BytesAcked uint64
	Busy       time.Duration
	NotSent    uint32
	AppLimited bool
	Backoff    uint8
}

// TCPDeliveryInfo needs Linux's TCP_INFO; elsewhere there is nothing to report.
func TCPDeliveryInfo(net.Conn) (TCPDelivery, bool) { return TCPDelivery{}, false }

// TCPPath mirrors the Linux type; it is never filled in here.
type TCPPath struct {
	RTT           time.Duration
	NotSent       uint32
	Unacked       uint32
	Cwnd          uint32
	MSS           uint32
	DeliveryRate  uint64
	Retrans       uint32
	BytesAcked    uint64
	BytesRecv     uint64
	Busy          time.Duration
	RwndLimited   time.Duration
	SndbufLimited time.Duration
	Unread        uint32
}

// TCPPathInfo needs Linux's TCP_INFO; elsewhere there is nothing to report.
func TCPPathInfo(net.Conn) (TCPPath, bool) { return TCPPath{}, false }
