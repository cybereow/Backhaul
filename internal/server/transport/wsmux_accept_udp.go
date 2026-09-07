package transport

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/musix/backhaul/internal/utils"
)

// udpListener forwards UDP for one mapped port over the wssmux/wsmux tunnel.
// It mirrors the TCP-transport accept_udp path (see accept_udp.go), but instead
// of pulling a raw pooled TCP connection it opens a smux stream on a live pool
// session for each distinct UDP source. Each datagram is length-framed and
// carried over that stream using the exact same wire format the TCP transport
// uses, so UDPConnectionHandler and the client's UDP dialer are reused verbatim.
//
// UDP over mux relies on the flow-kind byte, which only exists on mux_version >=
// 2; startPortListeners guards this, so udpListener is never started on a v1
// tunnel.
func (s *WsMuxTransport) udpListener(localAddr, remoteAddr string) {
	localUDPAddr, err := net.ResolveUDPAddr("udp", localAddr)
	if err != nil {
		s.logger.Fatalf("failed to resolve local udp address: %v", err)
	}

	listener, err := net.ListenUDP("udp", localUDPAddr)
	if err != nil {
		s.logger.Fatalf("failed to listen on local UDP port: %v", err)
	}

	defer listener.Close()

	s.logger.Infof("UDP listener started successfully, listening on address: %s", listener.LocalAddr().String())

	// Track active connections keyed by source address
	activeConnections := map[string]*LocalAcceptUDPConn{}

	// 2 bytes reserved for the length header prepended to each datagram
	buf := make([]byte, BufferSize-2)

	udpChan := make(chan *LocalAcceptUDPConn, s.config.ChannelSize)

	mu := &sync.Mutex{}

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

				key := addr.String()

				mu.Lock()
				if existingConn, exists := activeConnections[key]; exists {
					if existingConn.IsCongested.Load() {
						s.logger.Debugf("connection with timestamp %d congested. Removing %s from active connections due to network congestion", existingConn.timeCreated, addr.String())
						// Fall through and open a fresh stream for this source.
					} else {
						select {
						case existingConn.payload <- append([]byte(nil), buf[:n]...): // copy to avoid overwrite
							s.logger.Tracef("buffered %d bytes for existing connection %s", n, addr.String())
						default:
							s.logger.Warnf("payload channel for connection %s is full, dropping udp packet", addr.String())
						}
						mu.Unlock()
						continue
					}
				}
				mu.Unlock()

				// Buffer up to 100,000 datagrams for this source
				payloadChan := make(chan []byte, 100_000)

				newUDPConn := LocalAcceptUDPConn{
					timeCreated: time.Now().UnixNano(),
					payload:     payloadChan,
					remoteAddr:  remoteAddr,
					listener:    listener,
					clientAddr:  addr,
				}

				mu.Lock()
				activeConnections[key] = &newUDPConn
				mu.Unlock()

				select {
				case udpChan <- &newUDPConn:
					s.logger.Debugf("accepted UDP connection from %s", addr.String())
					payloadChan <- append([]byte(nil), buf[:n]...) // hand the first datagram to the new flow
				default:
					s.logger.Warn("UDP channel is full, dropping packet.")
					mu.Lock()
					// Roll back the just-added entry so a dropped flow doesn't
					// pin the source's key forever.
					if activeConnections[key] == &newUDPConn {
						delete(activeConnections, key)
					}
					mu.Unlock()
				}
			}
		}
	}()

	<-s.ctx.Done()
}

// handleUDPLoop assigns each new UDP source a smux stream and starts the shared
// UDPConnectionHandler on it.
func (s *WsMuxTransport) handleUDPLoop(udpChan chan *LocalAcceptUDPConn, activeConnections *map[string]*LocalAcceptUDPConn, mu *sync.Mutex) {
	for {
		select {
		case <-s.ctx.Done():
			return
		case localConn := <-udpChan:
			stream, err := s.openUDPStream(localConn.remoteAddr)
			if err != nil {
				s.logger.Errorf("failed to open udp tunnel stream for %s: %v", localConn.clientAddr.String(), err)
				// Give up on this source; drop its buffered datagrams and free the key.
				mu.Lock()
				close(localConn.payload)
				if (*activeConnections)[localConn.clientAddr.String()] == localConn {
					delete(*activeConnections, localConn.clientAddr.String())
				}
				mu.Unlock()
				continue
			}

			// wssmux does not measure RTT; UDPConnectionHandler treats 0 as
			// "unknown" and applies a sane default for congestion detection.
			go UDPConnectionHandler(localConn, stream, s.logger, s.usageMonitor, localConn.listener.LocalAddr().(*net.UDPAddr).Port, s.config.Sniffer, 0, activeConnections, mu)

			s.logger.Debugf("initiate new udp handler for connection %s with timestamp %d", localConn.clientAddr.String(), localConn.timeCreated)
		}
	}
}

// openUDPStream opens one smux stream on a live pool session and tags it as a
// UDP flow carrying remoteAddr. If the pool has not warmed up yet it asks for a
// session and retries briefly rather than dropping the flow outright.
func (s *WsMuxTransport) openUDPStream(remoteAddr string) (net.Conn, error) {
	const maxAttempts = 50 // ~5s total with the 100ms backoff below

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		legs, err := s.openStripedLegs(1)
		if err == nil {
			stream := legs[0]
			if err := utils.SendFlowUDP(stream, remoteAddr); err != nil {
				stream.Close()
				return nil, fmt.Errorf("failed to send udp flow header: %w", err)
			}
			return stream, nil
		}
		lastErr = err

		// No live session yet: nudge the client to dial one and wait a moment.
		select {
		case s.reqNewConnChan <- struct{}{}:
		default:
		}

		select {
		case <-s.ctx.Done():
			return nil, s.ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return nil, fmt.Errorf("no pool session available for udp stream: %w", lastErr)
}
