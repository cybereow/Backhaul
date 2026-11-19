package handlers

import (
	"context"
	"net"
	"time"

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

// The raw probe: how a client shows the server that raw flows get through the
// path between them (see the server's rawMode).
const (
	// RawProbeSuffix is the path, under the tunnel's base path, a client probes on.
	RawProbeSuffix = "/rawprobe"
	// RawTunnelSuffix is the path a client dials a pool connection on when its
	// entry point has passed a probe. It still starts with "/tunnel", so a server
	// that does not know it takes the connection as any other.
	RawTunnelSuffix = "/tunnelr"
	// RawProbeMsg is the text message after which the probe connection turns raw.
	RawProbeMsg = "RAWPROBE2"
	// RawProbeLen is the size of the payload the server writes and the client echoes.
	RawProbeLen = 32
	// RawProbeTimeout bounds one probe, from the upgrade to the echo.
	RawProbeTimeout = 8 * time.Second
)

// Capabilities a client reports after the echo of its probe.
const (
	// CapRawResume: the client can move a raw flow to another connection.
	CapRawResume = 1 << 0
)

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
	NewRawFlow(ctx, RawFlowConfig{
		Tunnel:        &RawTunnel{Conn: tunnel, Left: leftover, Born: time.Now()},
		App:           tcpConn,
		ProxyProtocol: proxyProtocol,
		Logger:        logger,
		Usage:         usage,
		RemotePort:    remotePort,
		Sniffer:       sniffer,
	}).Run()
}
