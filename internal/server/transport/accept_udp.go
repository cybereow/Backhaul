package transport

import (
	"encoding/binary"
	"io"
	"net"
	"sync"
	"time"

	"github.com/musix/backhaul/internal/utils"
	"github.com/musix/backhaul/internal/web"
	"github.com/sirupsen/logrus"
)

const BufferSize = 16 * 1024

func (s *TcpTransport) udpListener(localAddr string, remoteAddr string) {
	localUDPAddr, err := net.ResolveUDPAddr("udp", localAddr)
	if err != nil {
		s.logger.Fatalf("failed to resolve local address: %v", err)
	}

	listener, err := net.ListenUDP("udp", localUDPAddr)
	if err != nil {
		s.logger.Fatalf("failed to listen on local UDP port: %v", err)
	}

	defer listener.Close()

	s.logger.Infof("UDP listener started successfully, listening on address: %s", listener.LocalAddr().String())

	// Track active connections
	activeConnections := map[string]*LocalAcceptUDPConn{}

	// Buffer for UDP reads
	buf := make([]byte, BufferSize-2) // 2 bytes reserved for header

	// make a new channel for recieve udp packets
	udpChan := make(chan *LocalAcceptUDPConn, s.config.ChannelSize)

	//mutex
	mu := &sync.Mutex{}

	// handle channel
	go s.handleUDPLoop(udpChan, &activeConnections, mu)

	go func() {
		for {
			select {
			case <-s.ctx.Done():
				return
			default:
				n, addr, err := listener.ReadFromUDP(buf)
				if err != nil {
					s.logger.Errorf("failed to read from UDP listener: %v", err)
					continue
				}

				// Create a unique identifier for the connection based on IP and port
				key := addr.String()

				mu.Lock()
				// Check if the connection is already active
				if existingConn, exists := activeConnections[key]; exists {
					if existingConn.IsCongested.Load() {
						s.logger.Debugf("connection with timestamp %d congested. Removing %s from active connections due to network congestion", existingConn.timeCreated, addr.String())
						// For congested connections, closing the payload channel immediately can cause abrupt TCP disconnection,
						// potentially leading to data loss. Instead, allow the connection to keep transferring data for 30 more
						// seconds (or until the payload channel becomes idle). The timer will close the TCP connection once it
						// times out. Further testing is needed to confirm this strategy's effect on overall performance and congestion handling.

					} else {
						// If it exists, send the payload to the existing connection's payload channel
						select {
						case existingConn.payload <- append([]byte(nil), buf[:n]...): // Copy the packet to avoid data overwriting
							s.logger.Tracef("buffered %d bytes for existing connection %s", n, addr.String())

						default:
							s.logger.Warnf("payload channel for connection %s is full, dropping udp packet", addr.String())
						}
						mu.Unlock()
						continue
					}
				}

				mu.Unlock()

				// Create a new payload channel for this connection,  Buffer up to 100,0000 packets for the connection
				// Generally affect the upload speed
				payloadChan := make(chan []byte, 100_000)

				// build the UDP packet
				newUDPConn := LocalAcceptUDPConn{
					timeCreated: time.Now().UnixNano(), // Just for debugging
					payload:     payloadChan,
					remoteAddr:  remoteAddr,
					listener:    listener,
					clientAddr:  addr,
				}

				mu.Lock()
				// store the connection info
				activeConnections[key] = &newUDPConn
				mu.Unlock()

				select {
				case udpChan <- &newUDPConn:
					s.logger.Debugf("accepted UDP connection from %s", addr.String())
					payloadChan <- append([]byte(nil), buf[:n]...) // send a copy of the new payload to the channel

					select {
					case s.reqNewConnChan <- struct{}{}: // Successfully requested a new tcp connection
					default: // The channel is full, do nothing
						s.logger.Warn("channel is full, cannot request a new connection")
					}

				default:
					s.logger.Warn("UDP channel is full, dropping packet.")
				}
			}
		}
	}()

	<-s.ctx.Done()
}

func (s *TcpTransport) handleUDPLoop(udpChan chan *LocalAcceptUDPConn, activeConnections *map[string]*LocalAcceptUDPConn, mu *sync.Mutex) {
	for {
		select {
		case <-s.ctx.Done():
			return
		case localConn := <-udpChan:
		loop:
			for {
				select {
				case <-s.ctx.Done():
					return

				case tunnelConn := <-s.tunnelChannel:
					// Send the target addr over the connection
					if err := utils.SendBinaryTransportString(tunnelConn, localConn.remoteAddr, utils.SG_UDP); err != nil {
						s.logger.Errorf("%v", err)
						tunnelConn.Close()
						continue loop
					}

					// Handle data exchange between connections
					go UDPConnectionHandler(localConn, tunnelConn, s.logger, s.usageMonitor, localConn.listener.LocalAddr().(*net.UDPAddr).Port, s.config.Sniffer, s.rtt, activeConnections, mu)

					s.logger.Debugf("initiate new handler for connection %s with timestamp %d", localConn.clientAddr.String(), localConn.timeCreated)
					break loop
				}
			}
		}
	}
}

func UDPConnectionHandler(udp *LocalAcceptUDPConn, tcp net.Conn, logger *logrus.Logger, usage *web.Usage, remotePort int, sniffer bool, rtt int64, activeConnections *map[string]*LocalAcceptUDPConn, mu *sync.Mutex) {
	done := make(chan struct{})

	if rtt == 0 {
		// RTT of 0 indicates that either the backhaul is running in a local environment
		// (with negligible latency), or RTT measurement failed.
		// Set a default RTT of 100ms to ensure proper functioning of TCP congestion control.
		rtt = 100
	}

	go func() {
		udpToTCP(tcp, udp, logger, usage, remotePort, sniffer)
		tcp.Close()
		done <- struct{}{}
	}()

	tcpToUDP(tcp, udp, logger, usage, remotePort, sniffer, rtt)
	tcp.Close()

	<-done

	mu.Lock()
	close(udp.payload)

	if !udp.IsCongested.Load() {
		delete(*activeConnections, udp.clientAddr.String())
	}
	mu.Unlock()
}

func udpToTCP(tcp net.Conn, udp *LocalAcceptUDPConn, logger *logrus.Logger, usage *web.Usage, remotePort int, sniffer bool) {
	udpToTCPWithTimeout(tcp, udp, logger, usage, remotePort, sniffer, 60*time.Second)
}

// udpToTCPWithTimeout is udpToTCP with the inactivity timeout injected, so
// tests can exercise the idle path without waiting a real minute.
func udpToTCPWithTimeout(tcp net.Conn, udp *LocalAcceptUDPConn, logger *logrus.Logger, usage *web.Usage, remotePort int, sniffer bool, inactivityTimeout time.Duration) {
	// One idle timer for the whole pump, re-armed per packet, instead of a
	// fresh time.After on every loop turn. time.After allocates a Timer plus
	// its channel each turn and - worse - parks that timer in the runtime's
	// timer heap for the full 60s even though the select abandons it after the
	// next packet. A flow doing P packets/sec therefore carries ~60*P dead
	// timers at steady state (600k at a modest 10k pps), which every other
	// deadline in the process then has to sift past. Re-arming one timer keeps
	// the semantics identical - fire 60s after the last packet - at zero
	// steady-state cost.
	idle := time.NewTimer(inactivityTimeout)
	defer idle.Stop()

	// Reusable staging buffer holding the 2-byte length header followed by the
	// payload. The previous `append(header, data...)` allocated a fresh
	// packetSize+2 buffer for every packet, because header's capacity was
	// exactly 2 and so append always had to grow. BufferSize is an exact fit:
	// udpListener reads into a BufferSize-2 buffer, so header+payload never
	// exceeds it. The grow path below only guards against a future caller
	// feeding this pump larger packets. This mirrors tcpToUDP, which already
	// keeps a per-connection BufferSize read buffer for the other direction.
	packet := make([]byte, BufferSize)

	for {
		select {
		case data, ok := <-udp.payload: // Wait for data on the UDP payload channel
			if !ok {
				return
			}

			packetSize := len(data) // Calculate the packet size (data length)

			// the listener buffer size is 16KB, just for preventing bugs in the future!
			if packetSize > 65535 { // Check for overflow, since 2 bytes can only store values up to 65535 ~ 64KB
				logger.Errorf("packet too large to send, size: %d bytes", packetSize)
				continue
			}

			// Build header+payload in the reusable buffer.
			frameLen := 2 + packetSize
			if cap(packet) < frameLen {
				packet = make([]byte, frameLen)
			}
			frame := packet[:frameLen]
			binary.BigEndian.PutUint16(frame, uint16(packetSize)) // Store the packet size at 2 bytes
			copy(frame[2:], data)

			totalWritten := 0
			for totalWritten < frameLen { // Use the total packet length (header + data)
				w, err := tcp.Write(frame[totalWritten:])
				if err != nil {
					logger.Errorf("failed to write UDP payload to TCP: %v", err)
					return
				}
				totalWritten += w
			}

			// Guarded because the arguments to a variadic log call are boxed
			// into an []any before the call, so an unguarded Tracef allocates
			// on every packet even at the default (non-trace) level.
			if logger.IsLevelEnabled(logrus.TraceLevel) {
				logger.Tracef("received %d bytes, forwarded %d bytes from UDP to TCP", packetSize, totalWritten-2)
			}

			if sniffer {
				usage.AddOrUpdatePort(remotePort, uint64(totalWritten))
			}

			// Re-arm for the next packet. Stop-then-drain before Reset so a
			// timeout that fired while this packet was being written does not
			// leave a stale value queued on the channel.
			if !idle.Stop() {
				select {
				case <-idle.C:
				default:
				}
			}
			idle.Reset(inactivityTimeout)

		case <-idle.C: // Timeout after 60 seconds of inactivity
			logger.Debugf("connection with timestamp %d and address %s idle for 60 seconds, closing", udp.timeCreated, udp.clientAddr.String())
			return
		}
	}
}

func tcpToUDP(tcp net.Conn, udp *LocalAcceptUDPConn, logger *logrus.Logger, usage *web.Usage, remotePort int, sniffer bool, rtt int64) {
	buf := make([]byte, BufferSize)
	lenBuf := make([]byte, 2)       // Buffer to store the 2-byte packet length
	timestampBuf := make([]byte, 4) // Buffer for timestamp (4 bytes)

	for {
		// First, read the 4-byte timestamp from the packet
		_, err := io.ReadFull(tcp, timestampBuf)
		if err != nil {
			if err == io.EOF {
				logger.Debug("TCP connection closed.")
			} else {
				logger.Debugf("failed to read timestamp from TCP connection: %v", err)
			}
			return
		}

		// 4-byte timestamp header
		packetTimestamp := int64(binary.BigEndian.Uint32(timestampBuf))

		// Get the current time and calculate the time difference
		timestamp := time.Now().UnixMilli()
		lastMillis := timestamp % (10 * 60 * 1000)

		packetAge := lastMillis - packetTimestamp

		// If the packet age exceeds the threshold (3x RTT), flag the connection as congested
		if packetAge > 3*rtt {
			udp.IsCongested.Store(true)
		}

		// Read the 2-byte packet length header from the TCP connection
		_, err = io.ReadFull(tcp, lenBuf)
		if err != nil {
			if err == io.EOF {
				logger.Debug("TCP connection closed.")
			} else {
				logger.Errorf("failed to read packet length from TCP connection: %v", err)
			}
			return
		}

		// Convert the 2-byte length header into an integer
		packetSize := int(binary.BigEndian.Uint16(lenBuf))

		// Check if the packet size is valid
		if packetSize > len(buf) {
			logger.Errorf("packet size exceeds buffer size: %d bytes", packetSize)
			return
		}

		// Now use io.ReadFull to read the actual packet data from TCP based on the packetSize
		_, err = io.ReadFull(tcp, buf[:packetSize])
		if err != nil {
			if err == io.EOF {
				logger.Debug("TCP connection closed.")
			} else {
				logger.Errorf("failed to read from TCP connection: %v", err)
			}
			return
		}

		// Forward the data to the UDP client address
		if udp.clientAddr != nil {
			totalWritten := 0
			for totalWritten < packetSize {
				w, err := udp.listener.WriteToUDP(buf[totalWritten:packetSize], udp.clientAddr)
				if err != nil {
					logger.Errorf("failed to forward TCP response to UDP client: %v", err)
					return
				}

				totalWritten += w
			}

			if sniffer {
				usage.AddOrUpdatePort(remotePort, uint64(totalWritten))
			}

			logger.Tracef("read %d bytes from TCP, forwarded %d bytes to UDP", packetSize, totalWritten)
		}
	}
}
