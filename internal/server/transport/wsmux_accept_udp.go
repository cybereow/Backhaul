package transport

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/musix/backhaul/internal/utils"
)

// udpFlowIdleTimeout is how long a UDP flow (one source address) is kept open
// with no traffic in either direction before its tunnel stream is torn down.
// UDP is connectionless, so this is the only way to reclaim a stream for a
// client that has gone away. It is deliberately generous: a stateful handshake
// like IKE retransmits over tens of seconds, and an established VPN sends
// keepalives (L2TP HELLO, IPsec DPD) on the order of 30-60s, so a short idle
// would cut a live-but-quiet tunnel. The timer is reset on every datagram in
// either direction, so it only fires on a genuinely dead flow.
const udpFlowIdleTimeout = 120 * time.Second

// udpFlow is one UDP pseudo-connection: all datagrams from a single source
// address, carried over one smux stream. Unlike the TCP-transport accept_udp
// path this carries no timestamp/congestion metadata - smux already gives the
// stream reliability, ordering and flow control, and the cross-machine
// timestamp comparison that path uses false-positives on any clock skew between
// the two hosts, which would churn the stream mid-handshake and break IKE.
type udpFlow struct {
	payload    chan []byte
	clientAddr *net.UDPAddr
	lastActive atomic.Int64 // unix-nano of the last datagram in either direction
}

func (f *udpFlow) touch() {
	f.lastActive.Store(time.Now().UnixNano())
}

// udpListener forwards UDP for one mapped port over the wssmux/wsmux tunnel. For
// each distinct UDP source it opens one smux stream on a live pool session, tags
// it as a UDP flow, and shuttles length-framed datagrams both ways. UDP over mux
// relies on the flow-kind byte, which only exists on mux_version >= 2;
// startPortListeners guards this, so udpListener is never started on a v1 tunnel.
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

	active := map[string]*udpFlow{}
	mu := &sync.Mutex{}

	buf := make([]byte, 64*1024)

	go func() {
		for {
			select {
			case <-s.ctx.Done():
				return
			default:
				n, addr, err := listener.ReadFromUDP(buf)
				if err != nil {
					if s.ctx.Err() != nil {
						return
					}
					s.logger.Errorf("failed to read from UDP listener: %v", err)
					continue
				}

				key := addr.String()
				datagram := append([]byte(nil), buf[:n]...) // copy before the next read overwrites buf

				mu.Lock()
				if f, ok := active[key]; ok {
					select {
					case f.payload <- datagram:
						s.logger.Tracef("buffered %d bytes for existing udp flow %s", n, key)
					default:
						s.logger.Warnf("payload channel for udp flow %s is full, dropping packet", key)
					}
					mu.Unlock()
					continue
				}

				f := &udpFlow{
					payload:    make(chan []byte, 2048),
					clientAddr: addr,
				}
				f.touch()
				f.payload <- datagram // first datagram; channel is empty so this never blocks
				active[key] = f
				mu.Unlock()

				go s.serveUDPFlow(listener, remoteAddr, key, f, active, mu)
			}
		}
	}()

	<-s.ctx.Done()
}

// serveUDPFlow opens the tunnel stream for one source and pumps datagrams in
// both directions until the flow goes idle, the stream drops, or the transport
// shuts down.
func (s *WsMuxTransport) serveUDPFlow(listener *net.UDPConn, remoteAddr, key string, f *udpFlow, active map[string]*udpFlow, mu *sync.Mutex) {
	stream, err := s.openUDPStream(remoteAddr)
	if err != nil {
		s.logger.Errorf("failed to open udp tunnel stream for %s: %v", key, err)
		mu.Lock()
		if active[key] == f {
			delete(active, key)
		}
		mu.Unlock()
		return
	}

	s.logger.Debugf("initiated udp flow %s -> %s", key, remoteAddr)

	defer func() {
		stream.Close()
		mu.Lock()
		if active[key] == f {
			delete(active, key)
		}
		mu.Unlock()
		s.logger.Debugf("closed udp flow %s", key)
	}()

	port := listener.LocalAddr().(*net.UDPAddr).Port

	// Stream -> UDP client (replies coming back from the local service).
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		rbuf := make([]byte, 64*1024)
		for {
			n, err := utils.ReadUDPFrame(stream, rbuf)
			if err != nil {
				s.logger.Debugf("udp flow %s stream closed: %v", key, err)
				return
			}
			f.touch()
			if _, err := listener.WriteToUDP(rbuf[:n], f.clientAddr); err != nil {
				s.logger.Debugf("udp flow %s: failed to write reply to client: %v", key, err)
				return
			}
			if s.config.Sniffer {
				s.usageMonitor.AddOrUpdatePort(port, uint64(n))
			}
		}
	}()

	// UDP client -> stream (datagrams arriving on the public port), plus the
	// idle watchdog.
	idleCheck := time.NewTicker(5 * time.Second)
	defer idleCheck.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-readerDone:
			return
		case data := <-f.payload:
			if err := utils.WriteUDPFrame(stream, data); err != nil {
				s.logger.Debugf("udp flow %s: failed to write to stream: %v", key, err)
				return
			}
			f.touch()
			if s.config.Sniffer {
				s.usageMonitor.AddOrUpdatePort(port, uint64(len(data)))
			}
		case <-idleCheck.C:
			if time.Since(time.Unix(0, f.lastActive.Load())) > udpFlowIdleTimeout {
				s.logger.Debugf("udp flow %s idle for %s, closing", key, udpFlowIdleTimeout)
				return
			}
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
