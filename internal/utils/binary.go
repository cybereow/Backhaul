package utils

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

func SendBinaryString(conn interface{}, message string) error {
	// Header size
	const headerSize = 2

	// Create a buffer with the appropriate size for the message
	buf := make([]byte, headerSize+len(message))

	// Encode the length of the message as a big-endian 2-byte unsigned integer
	binary.BigEndian.PutUint16(buf[:headerSize], uint16(len(message)))

	// Copy the message into the buffer after the length
	copy(buf[headerSize:], message)

	switch c := conn.(type) {
	case net.Conn:
		// Send the buffer over the connection
		if _, err := c.Write(buf); err != nil {
			return fmt.Errorf("failed to send message: %w", err)
		}

	default:
		// Handle unsupported connection types
		return fmt.Errorf("unsupported connection type: %T", conn)
	}
	// Successful
	return nil
}

func ReceiveBinaryString(conn interface{}) (string, error) {
	// Header size
	const headerSize = 2

	// Create a buffer to read the first 2 bytes (the length of the message)
	lenBuf := make([]byte, headerSize)

	switch c := conn.(type) {
	case net.Conn:
		// Read exactly 2 bytes for the message length
		if _, err := io.ReadFull(c, lenBuf); err != nil {
			return "", fmt.Errorf("failed to read message length from net.Conn: %w", err)
		}

	default:
		return "", fmt.Errorf("unsupported connection type: %T", conn)
	}

	// Decode the length of the message from the 2-byte buffer
	messageLength := binary.BigEndian.Uint16(lenBuf[:2])

	// Create a buffer of the appropriate size to hold the message
	messageBuf := make([]byte, messageLength)

	switch c := conn.(type) {
	case net.Conn:
		if _, err := io.ReadFull(c, messageBuf); err != nil {
			return "", fmt.Errorf("failed to read message from net.Conn: %w", err)
		}

	default:
		return "", fmt.Errorf("unsupported connection type: %T", conn)
	}

	// Convert the message buffer to a string and return it
	return string(messageBuf), nil
}

func SendBinaryTransportString(conn interface{}, message string, transport byte) error {
	// Header size
	const headerSize = 3

	// Create a buffer with the appropriate size for the message
	buf := make([]byte, headerSize+len(message))

	// Encode the length of the message as a big-endian 2-byte unsigned integer
	binary.BigEndian.PutUint16(buf[:headerSize], uint16(len(message)))

	// encode the transport tyope
	buf[2] = transport

	// Copy the message into the buffer after the length
	copy(buf[headerSize:], message)

	switch c := conn.(type) {
	case net.Conn:
		// Send the buffer over the connection
		if _, err := c.Write(buf); err != nil {
			return fmt.Errorf("failed to send message: %w", err)
		}

	default:
		// Handle unsupported connection types
		return fmt.Errorf("unsupported connection type: %T", conn)
	}
	// Successful
	return nil
}

func ReceiveBinaryTransportString(conn interface{}) (string, byte, error) {
	// Header size
	const headerSize = 3

	// Create a buffer to read the first 2 bytes (the length of the message)
	lenBuf := make([]byte, headerSize)

	switch c := conn.(type) {
	case net.Conn:
		// Read exactly 2 bytes for the message length
		if _, err := io.ReadFull(c, lenBuf); err != nil {
			return "", 0, fmt.Errorf("failed to read message length from net.Conn: %w", err)
		}

	default:
		return "", 0, fmt.Errorf("unsupported connection type: %T", conn)
	}

	// Decode the length of the message from the 2-byte buffer
	messageLength := binary.BigEndian.Uint16(lenBuf[:2])

	// decode the transport
	transport := lenBuf[2]

	// Create a buffer of the appropriate size to hold the message
	messageBuf := make([]byte, messageLength)

	switch c := conn.(type) {
	case net.Conn:
		if _, err := io.ReadFull(c, messageBuf); err != nil {
			return "", 0, fmt.Errorf("failed to read message from net.Conn: %w", err)
		}

	default:
		return "", 0, fmt.Errorf("unsupported connection type: %T", conn)
	}

	// Convert the message buffer to a string and return it
	return string(messageBuf), transport, nil
}

// SendStripeHeader identifies which striped group a freshly opened stream
// belongs to: groupID correlates legs opened for the same logical
// connection, index/total let the receiver know how many legs to wait for
// before it can assemble them, parity is how many of those total legs carry
// Reed-Solomon parity shards rather than data (0 for plain striping, so the
// data-shard count is always total-parity), and remoteAddr is carried once
// per leg since every leg is otherwise indistinguishable from a plain tunnel
// stream.
func SendStripeHeader(conn net.Conn, groupID uint32, index, total, parity uint8, remoteAddr string) error {
	const headerSize = 4 + 1 + 1 + 1 + 2 // groupID + index + total + parity + addr length
	buf := make([]byte, headerSize+len(remoteAddr))

	binary.BigEndian.PutUint32(buf[0:4], groupID)
	buf[4] = index
	buf[5] = total
	buf[6] = parity
	binary.BigEndian.PutUint16(buf[7:9], uint16(len(remoteAddr)))
	copy(buf[9:], remoteAddr)

	if _, err := conn.Write(buf); err != nil {
		return fmt.Errorf("failed to send stripe header: %w", err)
	}
	return nil
}

// ReceiveStripeHeader reads a header written by SendStripeHeader.
func ReceiveStripeHeader(conn net.Conn) (groupID uint32, index, total, parity uint8, remoteAddr string, err error) {
	const headerSize = 4 + 1 + 1 + 1 + 2
	head := make([]byte, headerSize)
	if _, err = io.ReadFull(conn, head); err != nil {
		return 0, 0, 0, 0, "", fmt.Errorf("failed to read stripe header: %w", err)
	}

	groupID = binary.BigEndian.Uint32(head[0:4])
	index = head[4]
	total = head[5]
	parity = head[6]
	addrLen := binary.BigEndian.Uint16(head[7:9])

	addrBuf := make([]byte, addrLen)
	if addrLen > 0 {
		if _, err = io.ReadFull(conn, addrBuf); err != nil {
			return 0, 0, 0, 0, "", fmt.Errorf("failed to read stripe header address: %w", err)
		}
	}

	return groupID, index, total, parity, string(addrBuf), nil
}

// SendPort sends the port number as a 2-byte big-endian unsigned integer.
func SendBinaryInt(conn net.Conn, port uint16) error {
	// Create a 2-byte slice to hold the port number
	buf := make([]byte, 2)

	// Encode the port number as a big-endian 2-byte unsigned integer
	binary.BigEndian.PutUint16(buf, port)

	// Send the 2-byte buffer over the connection
	if _, err := conn.Write(buf); err != nil {
		return fmt.Errorf("failed to send port number %d: %w", port, err)
	}

	// Successful
	return nil
}

// ReceivePort reads a 2-byte big-endian unsigned integer directly from the connection
func ReceiveBinaryInt(conn net.Conn) (uint16, error) {
	var port uint16

	// Use binary.Read to read the port directly from the connection
	err := binary.Read(conn, binary.BigEndian, &port)
	if err != nil {
		return 0, fmt.Errorf("failed to read port number from connection: %w", err)
	}

	// Successful
	return port, nil
}

func SendBinaryByte(conn interface{}, message byte) error {
	// Create a 1-byte buffer and send the message
	messageBuf := [1]byte{message}

	switch c := conn.(type) {
	case net.Conn:
		if _, err := c.Write(messageBuf[:]); err != nil {
			return fmt.Errorf("failed to read message from net.Conn: %w", err)
		}

	default:
		return fmt.Errorf("unsupported connection type: %T", conn)
	}

	// Successful
	return nil
}

func ReceiveBinaryByte(conn net.Conn) (byte, error) {
	var messageBuf [1]byte

	switch c := conn.(type) {
	case net.Conn:
		if _, err := io.ReadFull(c, messageBuf[:]); err != nil {
			return 0, fmt.Errorf("failed to read message from net.Conn: %w", err)
		}

	default:
		return 0, fmt.Errorf("unsupported connection type: %T", conn)
	}

	// Convert the message buffer to a string and return it
	return messageBuf[0], nil
}

const (
	FlowPlain     byte = 0x00
	FlowStriped   byte = 0x01
	FlowPromote   byte = 0x02
	FlowUDP       byte = 0x03
	FlowPing      byte = 0x04
	FlowSpeedtest byte = 0x05
	// FlowPlainHC is a non-promotable plain flow whose stream payload is wrapped
	// in the half-close envelope (handlers.NewHalfCloseConn, plan 024). Header
	// identical to FlowPlain (flowID is always 0). Only ever sent to a peer that
	// offered the halfclose-v1 capability: an older client would misparse it.
	FlowPlainHC byte = 0x06
	// FlowResumable is a plain flow that can be moved to another tunnel stream
	// while it runs (see docs/resumable-flows-plan.md). Header identical to
	// FlowPlain, with a random non-zero flowID; the stream payload is wrapped in
	// the half-close envelope from its first byte, because the payload format of
	// a stream cannot change after it is opened.
	FlowResumable byte = 0x07
	// FlowAttach asks the peer to move the resumable flow flowID onto the stream
	// it arrives on (see SendFlowAttach).
	FlowAttach byte = 0x08
	// FlowResumableReplay is a FlowResumable flow whose two ends also keep what they
	// sent until it is acknowledged, so that it survives its stream or session dying
	// without warning (resume, AttachResume). Same header as FlowResumable. The
	// server decides per flow (memory budget), the client follows.
	FlowResumableReplay byte = 0x09
)

// Attach modes (FlowAttach header).
const (
	// AttachDrained: planned move. The old tunnel is still healthy, so both ends
	// freeze, exchange their sent counts and drain it; nothing is replayed.
	AttachDrained byte = 0
	// AttachResume: the flow's tunnel died without warning. Both ends suspend it,
	// exchange how many bytes each has delivered, and replay the rest from their
	// replay rings (FlowResumableReplay flows only).
	AttachResume byte = 1
	// AttachProbe attaches nothing (flowID 0): it asks whether the client lets a
	// flow that keeps replay state be promoted, giving that state up on the striped
	// group. A client that does accepts; an older one refuses the mode, and the
	// server then opens promotable flows without replay, as it always did.
	AttachProbe byte = 2
)

// AttachFlagReopen, on an AttachResume: the server has heard nothing back for
// this flow and can open it again from its first byte, so a client that has
// never seen it should say so (AttachRejectNeverSeen) rather than unknown. Only
// sent to a client that announced AttachCapReopen.
const AttachFlagReopen byte = 1

// AttachCapReopen is set in the second byte of the verdict that accepts an
// AttachProbe, by a client that understands AttachFlagReopen. (An older client
// leaves the byte zero; an older server does not look at it.)
const AttachCapReopen byte = 1

// Why a peer refused an attach (second byte of the verdict).
const (
	AttachRejectUnknownFlow byte = 1 // no such flow, or it already finished
	AttachRejectBusy        byte = 2 // a swap of that flow is already in progress
	AttachRejectUnsupported byte = 3 // mode or flags not understood
	// AttachRejectNeverSeen answers an AttachResume with AttachFlagReopen: the
	// flow is not running here and never was, and one opening of it is now
	// expected.
	AttachRejectNeverSeen byte = 4
)

// SendFlowResumable writes the FlowResumable header (same layout as FlowPlain).
func SendFlowResumable(conn net.Conn, flowID uint64, remoteAddr string) error {
	return sendFlowPlainKind(conn, FlowResumable, flowID, remoteAddr)
}

// ReceiveFlowResumable reads the FlowResumable header; the kind byte is consumed
// separately by ReadFlowKind.
func ReceiveFlowResumable(conn net.Conn) (flowID uint64, remoteAddr string, err error) {
	return ReceiveFlowPlain(conn)
}

// SendFlowResumableReplay writes the FlowResumableReplay header (same layout as
// FlowPlain).
func SendFlowResumableReplay(conn net.Conn, flowID uint64, remoteAddr string) error {
	return sendFlowPlainKind(conn, FlowResumableReplay, flowID, remoteAddr)
}

// SendFlowAttach writes the FlowAttach header: kind | flowID(8) | mode(1) |
// flags(1). flags is reserved (must be 0 today) so a later capability does not
// have to change the header.
func SendFlowAttach(conn net.Conn, flowID uint64, mode, flags byte) error {
	var buf [1 + 8 + 1 + 1]byte
	buf[0] = FlowAttach
	binary.BigEndian.PutUint64(buf[1:9], flowID)
	buf[9] = mode
	buf[10] = flags
	if _, err := conn.Write(buf[:]); err != nil {
		return fmt.Errorf("failed to send attach header: %w", err)
	}
	return nil
}

// ReceiveFlowAttach reads the FlowAttach header after the kind byte.
func ReceiveFlowAttach(conn net.Conn) (flowID uint64, mode, flags byte, err error) {
	var buf [8 + 1 + 1]byte
	if _, err = io.ReadFull(conn, buf[:]); err != nil {
		return 0, 0, 0, fmt.Errorf("failed to read attach header: %w", err)
	}
	return binary.BigEndian.Uint64(buf[0:8]), buf[8], buf[9], nil
}

// WriteAttachVerdict answers an attach: accept, or reject with one of the
// AttachReject* reasons. A rejection happens before either end freezes, so the
// flow simply stays where it is.
func WriteAttachVerdict(conn net.Conn, accept bool, reason byte) error {
	buf := [2]byte{0, reason}
	if accept {
		buf[0] = 1
		buf[1] = 0
	}
	if _, err := conn.Write(buf[:]); err != nil {
		return fmt.Errorf("failed to send attach verdict: %w", err)
	}
	return nil
}

// WriteAttachCaps accepts an AttachProbe, announcing caps (AttachCap* bits).
func WriteAttachCaps(conn net.Conn, caps byte) error {
	if _, err := conn.Write([]byte{1, caps}); err != nil {
		return fmt.Errorf("failed to send attach verdict: %w", err)
	}
	return nil
}

// ReadAttachVerdict reads the answer to an attach.
func ReadAttachVerdict(conn net.Conn) (accept bool, reason byte, err error) {
	var buf [2]byte
	if _, err = io.ReadFull(conn, buf[:]); err != nil {
		return false, 0, fmt.Errorf("failed to read attach verdict: %w", err)
	}
	return buf[0] == 1, buf[1], nil
}

func SendFlowPlain(conn net.Conn, flowID uint64, remoteAddr string) error {
	return sendFlowPlainKind(conn, FlowPlain, flowID, remoteAddr)
}

// SendFlowPlainHC writes the FlowPlainHC header; the caller then wraps the
// stream in the half-close envelope before any payload.
func SendFlowPlainHC(conn net.Conn, flowID uint64, remoteAddr string) error {
	return sendFlowPlainKind(conn, FlowPlainHC, flowID, remoteAddr)
}

func sendFlowPlainKind(conn net.Conn, kind byte, flowID uint64, remoteAddr string) error {
	const headerSize = 1 + 8 + 2
	buf := make([]byte, headerSize+len(remoteAddr))
	buf[0] = kind
	binary.BigEndian.PutUint64(buf[1:9], flowID)
	binary.BigEndian.PutUint16(buf[9:11], uint16(len(remoteAddr)))
	copy(buf[11:], remoteAddr)
	if _, err := conn.Write(buf); err != nil {
		return fmt.Errorf("failed to send plain flow header: %w", err)
	}
	return nil
}

func ReceiveFlowPlain(conn net.Conn) (flowID uint64, remoteAddr string, err error) {
	const headerSize = 8 + 2
	head := make([]byte, headerSize)
	if _, err = io.ReadFull(conn, head); err != nil {
		return 0, "", fmt.Errorf("failed to read plain flow header: %w", err)
	}
	flowID = binary.BigEndian.Uint64(head[0:8])
	addrLen := binary.BigEndian.Uint16(head[8:10])
	addrBuf := make([]byte, addrLen)
	if addrLen > 0 {
		if _, err = io.ReadFull(conn, addrBuf); err != nil {
			return 0, "", fmt.Errorf("failed to read plain flow header address: %w", err)
		}
	}
	return flowID, string(addrBuf), nil
}

// ReceiveFlowPlainHC reads the FlowPlainHC header (same layout as FlowPlain);
// the kind byte is consumed separately by ReadFlowKind.
func ReceiveFlowPlainHC(conn net.Conn) (flowID uint64, remoteAddr string, err error) {
	return ReceiveFlowPlain(conn)
}

// SendFlowUDP marks a freshly opened stream as carrying a UDP flow and carries
// the target address for the client to dial. Unlike a plain TCP flow, the
// payload on the stream is length-framed UDP datagrams (see accept_udp), so the
// receiver must route it to a UDP dialer rather than a TCP one. UDP over mux is
// only available with mux_version >= 2, since flow kinds are read on that path.
func SendFlowUDP(conn net.Conn, remoteAddr string) error {
	const headerSize = 1 + 2 // kind + addr length
	buf := make([]byte, headerSize+len(remoteAddr))
	buf[0] = FlowUDP
	binary.BigEndian.PutUint16(buf[1:3], uint16(len(remoteAddr)))
	copy(buf[3:], remoteAddr)
	if _, err := conn.Write(buf); err != nil {
		return fmt.Errorf("failed to send udp flow header: %w", err)
	}
	return nil
}

// ReceiveFlowUDP reads the address written by SendFlowUDP. The one-byte flow
// kind is consumed separately by ReadFlowKind before this is called.
func ReceiveFlowUDP(conn net.Conn) (remoteAddr string, err error) {
	const headerSize = 2 // addr length
	head := make([]byte, headerSize)
	if _, err = io.ReadFull(conn, head); err != nil {
		return "", fmt.Errorf("failed to read udp flow header: %w", err)
	}
	addrLen := binary.BigEndian.Uint16(head[0:2])
	addrBuf := make([]byte, addrLen)
	if addrLen > 0 {
		if _, err = io.ReadFull(conn, addrBuf); err != nil {
			return "", fmt.Errorf("failed to read udp flow header address: %w", err)
		}
	}
	return string(addrBuf), nil
}

// SendFlowPing opens an RTT probe on a pool session: the flow-kind byte plus an
// 8-byte nonce the peer echoes back unchanged. The sender times the round-trip
// to estimate the session's latency, used to steer striped-leg selection toward
// the lowest-latency CDN. Only meaningful on mux_version >= 2, where the peer
// reads the flow kind and can route the stream to the ping echoer.
func SendFlowPing(conn net.Conn, nonce uint64) error {
	var buf [1 + 8]byte
	buf[0] = FlowPing
	binary.BigEndian.PutUint64(buf[1:], nonce)
	if _, err := conn.Write(buf[:]); err != nil {
		return fmt.Errorf("failed to send ping probe: %w", err)
	}
	return nil
}

// ReceiveFlowPing reads the 8-byte nonce of a ping probe. The one-byte flow kind
// is consumed separately by ReadFlowKind before this is called.
func ReceiveFlowPing(conn net.Conn) (nonce uint64, err error) {
	var buf [8]byte
	if _, err = io.ReadFull(conn, buf[:]); err != nil {
		return 0, fmt.Errorf("failed to read ping probe: %w", err)
	}
	return binary.BigEndian.Uint64(buf[:]), nil
}

// EchoFlowPing writes a ping nonce back unchanged, completing the round-trip the
// prober is timing.
func EchoFlowPing(conn net.Conn, nonce uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], nonce)
	if _, err := conn.Write(buf[:]); err != nil {
		return fmt.Errorf("failed to echo ping probe: %w", err)
	}
	return nil
}

// WriteUDPFrame writes one UDP datagram to a stream as a 2-byte big-endian
// length header followed by the payload. Used by the UDP-over-mux path, where
// smux already provides reliability and ordering, so no timestamp/congestion
// metadata is needed - just enough framing to recover datagram boundaries on a
// byte stream.
func WriteUDPFrame(conn net.Conn, data []byte) error {
	if len(data) > 65535 {
		return fmt.Errorf("udp datagram too large to frame: %d bytes", len(data))
	}
	buf := make([]byte, 2+len(data))
	binary.BigEndian.PutUint16(buf[:2], uint16(len(data)))
	copy(buf[2:], data)
	if _, err := conn.Write(buf); err != nil {
		return fmt.Errorf("failed to write udp frame: %w", err)
	}
	return nil
}

// ReadUDPFrame reads one framed datagram written by WriteUDPFrame into buf and
// returns its length. It errors if the datagram would not fit in buf.
func ReadUDPFrame(conn net.Conn, buf []byte) (int, error) {
	var hdr [2]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		return 0, err
	}
	n := int(binary.BigEndian.Uint16(hdr[:]))
	if n > len(buf) {
		return 0, fmt.Errorf("udp frame size %d exceeds buffer %d", n, len(buf))
	}
	if _, err := io.ReadFull(conn, buf[:n]); err != nil {
		return 0, err
	}
	return n, nil
}

func SendFlowStriped(conn net.Conn, groupID uint32, index, total, parity uint8, remoteAddr string) error {
	const headerSize = 1 + 4 + 1 + 1 + 1 + 2
	buf := make([]byte, headerSize+len(remoteAddr))
	buf[0] = FlowStriped
	binary.BigEndian.PutUint32(buf[1:5], groupID)
	buf[5] = index
	buf[6] = total
	buf[7] = parity
	binary.BigEndian.PutUint16(buf[8:10], uint16(len(remoteAddr)))
	copy(buf[10:], remoteAddr)
	if _, err := conn.Write(buf); err != nil {
		return fmt.Errorf("failed to send FlowStriped: %w", err)
	}
	return nil
}

func SendFlowPromote(conn net.Conn, flowID uint64, groupID uint32, index, total, parity uint8) error {
	const headerSize = 1 + 8 + 4 + 1 + 1 + 1
	buf := make([]byte, headerSize)
	buf[0] = FlowPromote
	binary.BigEndian.PutUint64(buf[1:9], flowID)
	binary.BigEndian.PutUint32(buf[9:13], groupID)
	buf[13] = index
	buf[14] = total
	buf[15] = parity
	if _, err := conn.Write(buf); err != nil {
		return fmt.Errorf("failed to send promote flow header: %w", err)
	}
	return nil
}

func ReceiveFlowPromote(conn net.Conn) (flowID uint64, groupID uint32, index, total, parity uint8, err error) {
	const headerSize = 8 + 4 + 1 + 1 + 1
	head := make([]byte, headerSize)
	if _, err = io.ReadFull(conn, head); err != nil {
		return 0, 0, 0, 0, 0, fmt.Errorf("failed to read promote flow header: %w", err)
	}
	flowID = binary.BigEndian.Uint64(head[0:8])
	groupID = binary.BigEndian.Uint32(head[8:12])
	index = head[12]
	total = head[13]
	parity = head[14]
	return flowID, groupID, index, total, parity, nil
}

func WriteCount(conn net.Conn, count uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], count)
	if _, err := conn.Write(buf[:]); err != nil {
		return fmt.Errorf("failed to write count: %w", err)
	}
	return nil
}

func ReadCount(conn net.Conn) (uint64, error) {
	var buf [8]byte
	if _, err := io.ReadFull(conn, buf[:]); err != nil {
		return 0, fmt.Errorf("failed to read count: %w", err)
	}
	return binary.BigEndian.Uint64(buf[:]), nil
}

func ReadFlowKind(conn net.Conn) (byte, error) {
	var buf [1]byte
	if _, err := io.ReadFull(conn, buf[:]); err != nil {
		return 0, err
	}
	return buf[0], nil
}
