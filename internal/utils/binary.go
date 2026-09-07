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
	FlowPlain   byte = 0x00
	FlowStriped byte = 0x01
	FlowPromote byte = 0x02
	FlowUDP     byte = 0x03
)

func SendFlowPlain(conn net.Conn, flowID uint64, remoteAddr string) error {
	const headerSize = 1 + 8 + 2
	buf := make([]byte, headerSize+len(remoteAddr))
	buf[0] = FlowPlain
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
