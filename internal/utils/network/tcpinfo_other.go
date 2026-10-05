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
