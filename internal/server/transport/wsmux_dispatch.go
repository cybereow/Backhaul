package transport

import (
	"net"
	"sync/atomic"
	"time"

	"github.com/musix/backhaul/internal/utils"
	"github.com/musix/backhaul/internal/utils/handlers"
	"github.com/musix/backhaul/internal/utils/network"
	"github.com/musix/backhaul/internal/utils/striping"
)

func (s *WsMuxTransport) localListener(localAddr string, remoteAddr string) {
	// Force the socket buffers on the local ingress port (e.g. 6034), the same
	// way the tcp/tcpmux transports do. This port carries the user's traffic into
	// the tunnel; with a plain net.Listen it fell back to the OS default receive
	// buffer, which caps how fast the server can *read* an upload off a
	// high-RTT client connection (throughput ~= rcvbuf / RTT) - so upload was
	// throttled at ingress even though the tunnel legs themselves were tuned.
	listener, err := network.ListenWithBuffers("tcp", localAddr, s.config.SO_RCVBUF, s.config.SO_SNDBUF, 0, s.config.KeepAlive, !s.config.Nodelay)
	if err != nil {
		s.logger.Fatalf("failed to start listener on %s: %v", localAddr, err)
		return
	}

	//close local listener after context cancellation
	defer listener.Close()

	go s.acceptLocalConn(listener, remoteAddr)

	s.logger.Infof("listener started successfully, listening on address: %s", listener.Addr().String())

	<-s.ctx.Done()
}

func (s *WsMuxTransport) acceptLocalConn(listener net.Listener, remoteAddr string) {
	for {
		select {
		case <-s.ctx.Done():
			return

		default:
			conn := acceptWithBackoff(s.ctx, listener, s.logger)
			if conn == nil {
				return
			}

			// discard any non-tcp connection
			tcpConn, ok := conn.(*net.TCPConn)
			if !ok {
				s.logger.Warnf("disarded non-TCP connection from %s", conn.RemoteAddr().String())
				conn.Close()
				continue
			}

			// trying to enable tcpnodelay
			if !s.config.Nodelay {
				if err := tcpConn.SetNoDelay(s.config.Nodelay); err != nil {
					s.logger.Warnf("failed to set TCP_NODELAY for %s: %v", tcpConn.RemoteAddr().String(), err)
				} else {
					s.logger.Tracef("TCP_NODELAY disabled for %s", tcpConn.RemoteAddr().String())
				}
			}

			// Set keep-alive settings
			if err := tcpConn.SetKeepAlive(true); err != nil {
				s.logger.Warnf("failed to enable TCP keep-alive for %s: %v", tcpConn.RemoteAddr().String(), err)
			} else {
				s.logger.Tracef("TCP keep-alive enabled for %s", tcpConn.RemoteAddr().String())
			}
			if err := tcpConn.SetKeepAlivePeriod(s.config.KeepAlive); err != nil {
				s.logger.Warnf("failed to set TCP keep-alive period for %s: %v", tcpConn.RemoteAddr().String(), err)
			}

			select {
			case s.localChannel <- LocalTCPConn{conn: conn, remoteAddr: remoteAddr, timeCreated: time.Now().UnixMilli()}:
				s.logger.Debugf("accepted incoming TCP connection from %s", tcpConn.RemoteAddr().String())

				// +1 for stream counter
				atomic.AddInt32(&s.streamCounter, 1)

				if atomic.LoadInt32(&s.streamCounter) >= atomic.LoadInt32(&s.sessionCounter)*int32(s.config.MuxCon) {
					s.logger.Tracef("stream counter: %v, session counter: %v", atomic.LoadInt32(&s.streamCounter), atomic.LoadInt32(&s.sessionCounter))
					// Attempt to request a new connection
					select {
					case s.reqNewConnChan <- struct{}{}:
					default:
						s.logger.Warn("failed to request new connection. channel is full")
					}
				}

			default: // channel is full, discard the connection
				s.logger.Warnf("local listener channel is full, discarding TCP connection from %s", tcpConn.LocalAddr().String())
				conn.Close()
			}
		}
	}

}

func (s *WsMuxTransport) shouldStripe(incomingConn LocalTCPConn) bool {
	if len(s.config.StripePorts) > 0 {
		remotePort := ""
		if _, port, err := net.SplitHostPort(incomingConn.remoteAddr); err == nil {
			remotePort = port
		} else {
			remotePort = incomingConn.remoteAddr
		}
		for _, p := range s.config.StripePorts {
			if p == remotePort {
				return true
			}
		}
		return false
	}
	return s.config.StripeFactor > 1
}

func (s *WsMuxTransport) acquirePlainSlot() bool {
	for {
		active := atomic.LoadInt32(&s.plainFlows)
		budget := atomic.LoadInt32(&s.sessionCounter) * int32(s.config.MuxCon)
		if budget < 1 {
			budget = 1 // always admit at least one flow while the pool warms up
		}
		if (active + 1) <= budget {
			if atomic.CompareAndSwapInt32(&s.plainFlows, active, active+1) {
				return true
			}
			continue // lost the CAS race, re-read and retry
		}
		// Budget full: ask for a replacement session so the budget grows.
		select {
		case s.reqNewConnChan <- struct{}{}:
		default:
		}
		select {
		case <-s.ctx.Done():
			return false
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (s *WsMuxTransport) dispatchPlain(incomingConn LocalTCPConn) {
	if time.Now().UnixMilli()-incomingConn.timeCreated > 3000 { // 3000ms
		s.logger.Debugf("timeouted local connection: %d ms", time.Now().UnixMilli()-incomingConn.timeCreated)
		incomingConn.conn.Close()
		atomic.AddInt32(&s.streamCounter, -1)
		return
	}

	if !s.acquirePlainSlot() {
		incomingConn.conn.Close()
		atomic.AddInt32(&s.streamCounter, -1)
		return
	}

	stream, err := s.openPlainLeg()
	if err != nil {
		atomic.AddInt32(&s.plainFlows, -1)
		s.logger.Tracef("plain dispatch: %v, retrying shortly", err)
		time.Sleep(100 * time.Millisecond)
		incomingConn.timeCreated = time.Now().UnixMilli()
		s.requeueOrDrop(incomingConn)
		return
	}

	var flowID uint64
	// Promotion migrates a heavy plain flow onto a striped group, so it only
	// makes sense when a group is actually wider than one leg. In pure-plain mode
	// (StripeFactor 1, no parity) legsPerFlow is 1, so "promotion" would just wrap
	// the flow in single-leg striping framing and stall it through the promote
	// handshake for no aggregation gain - keep it plain. Per-port striping (a
	// plain port in a striped deployment) still has legsPerFlow > 1 and promotes.
	promotable := s.config.MuxVersion >= 2 && s.config.PromoteBytes > 0 && s.legsPerFlow() > 1

	if s.config.MuxVersion >= 2 {
		// A non-zero flowID signals the client this flow is promotable and must
		// be run through the promotable pump; flowID 0 means a plain flow.
		if promotable {
			flowID = uint64(time.Now().UnixNano())
		}
		if err := utils.SendFlowPlain(stream, flowID, incomingConn.remoteAddr); err != nil {
			atomic.AddInt32(&s.plainFlows, -1)
			stream.Close()
			s.requeueOrDrop(incomingConn)
			return
		}
	} else {
		if err := utils.SendBinaryString(stream, incomingConn.remoteAddr); err != nil {
			atomic.AddInt32(&s.plainFlows, -1)
			stream.Close()
			s.requeueOrDrop(incomingConn)
			return
		}
	}

	defer atomic.AddInt32(&s.plainFlows, -1)
	defer atomic.AddInt32(&s.streamCounter, -1)
	if promotable {
		// dispatchPromotable blocks until the flow (and any mid-stream
		// promotion) completes, so the counters above are released only when
		// the flow is truly done.
		s.dispatchPromotable(incomingConn.conn, stream, flowID, incomingConn.remoteAddr)
	} else {
		handlers.TCPConnectionHandler(s.ctx, s.config.ProxyProtocol, incomingConn.conn, stream, s.logger, s.usageMonitor, incomingConn.conn.LocalAddr().(*net.TCPAddr).Port, s.config.Sniffer)
	}
}

// dispatchStriped opens the striped legs for one incoming connection, sends the
// per-leg headers, and pumps the flow. streamCounter (incremented in
// localListener when the connection was accepted) is decremented exactly once
// for every connection that leaves this pipeline - dropped here, or handed to a
// handler that later finishes - mirroring handleSession on the non-striped path.
// A connection requeued onto localChannel keeps its count, since it passes
// through here again.
func (s *WsMuxTransport) dispatchStriped(incomingConn LocalTCPConn) {
	if time.Now().UnixMilli()-incomingConn.timeCreated > 3000 { // 3000ms
		s.logger.Debugf("timeouted local connection: %d ms", time.Now().UnixMilli()-incomingConn.timeCreated)
		incomingConn.conn.Close()
		atomic.AddInt32(&s.streamCounter, -1)
		return
	}

	// Bound concurrent striped flows to the pool's stream budget
	// (sessionCounter*MuxCon): each flow opens StripeFactor legs and drives a
	// local dial, so running more flows than the sessions can carry just floods
	// the pool and the local service. The bound scales with the live pool - as
	// load makes streamCounter request more sessions, more flows are admitted -
	// mirroring the non-striped path's per-session MuxCon cap. In the common
	// under-budget case the slot is taken immediately (no added latency); only a
	// genuinely saturated pool makes a new flow wait instead of piling on.
	if !s.acquireStripedSlot() {
		// ctx cancelled while waiting for a slot
		incomingConn.conn.Close()
		atomic.AddInt32(&s.streamCounter, -1)
		return
	}
	// The admission slot is released explicitly on each exit path rather than
	// via a single defer: a setup that fails and requeues must give the slot
	// back *before* the backoff sleep and requeue, otherwise it shrinks capacity
	// for everyone else while doing nothing.

	legs, err := s.openStripedLegs(s.legsPerFlow())
	if err != nil {
		atomic.AddInt32(&s.stripedFlows, -1) // release before backoff + requeue
		s.logger.Tracef("striped dispatch: %v, retrying shortly", err)
		time.Sleep(100 * time.Millisecond)
		// Refresh the creation time: a requeued conn keeps its original
		// timestamp otherwise, so the 3s setup-timeout check at the top would
		// almost always drop it on the retry, making the "retry" effectively
		// dead.
		incomingConn.timeCreated = time.Now().UnixMilli()
		s.requeueOrDrop(incomingConn)
		return
	}

	gid := atomic.AddUint32(&s.stripeGroupID, 1)
	for i, stream := range legs {
		var err error
		if s.config.MuxVersion >= 2 {
			err = utils.SendFlowStriped(stream, gid, uint8(i), uint8(len(legs)), uint8(s.config.StripeParity), incomingConn.remoteAddr)
		} else {
			err = utils.SendStripeHeader(stream, gid, uint8(i), uint8(len(legs)), uint8(s.config.StripeParity), incomingConn.remoteAddr)
		}
		if err != nil {
			atomic.AddInt32(&s.stripedFlows, -1) // release before requeue
			s.logger.Tracef("failed to send stripe header: %v", err)
			for _, st := range legs {
				st.Close()
			}
			incomingConn.timeCreated = time.Now().UnixMilli()
			s.requeueOrDrop(incomingConn)
			return
		}
	}

	conns := make([]net.Conn, len(legs))
	for i, st := range legs {
		conns[i] = st
	}
	var stripedConn net.Conn
	if s.config.StripeParity > 0 {
		fecConn, err := striping.NewFEC(conns, striping.DefaultChunkSize, s.config.StripeFactor, s.config.StripeParity)
		if err != nil {
			atomic.AddInt32(&s.stripedFlows, -1)
			s.logger.Errorf("striped dispatch: %v", err)
			for _, st := range legs {
				st.Close()
			}
			atomic.AddInt32(&s.streamCounter, -1)
			incomingConn.conn.Close()
			return
		}
		stripedConn = fecConn
	} else {
		stripedConn = striping.New(conns, striping.DefaultChunkSize)
	}

	defer atomic.AddInt32(&s.stripedFlows, -1) // hold the slot for the flow's lifetime
	defer atomic.AddInt32(&s.streamCounter, -1)
	handlers.TCPConnectionHandler(s.ctx, s.config.ProxyProtocol, incomingConn.conn, stripedConn, s.logger, s.usageMonitor, incomingConn.conn.LocalAddr().(*net.TCPAddr).Port, s.config.Sniffer)
}

// legsPerFlow is how many pool legs one striped flow opens: the plain
// stripe factor, plus any FEC parity legs on top.
func (s *WsMuxTransport) legsPerFlow() int {
	return s.config.StripeFactor + s.config.StripeParity
}

// acquireStripedSlot reserves one in-flight striped-flow slot, blocking (with a
// short poll) while the pool is at its stream budget so a burst of new flows
// can't open more legs than the sessions can carry. The budget is
// sessionCounter*MuxCon streams and each flow uses StripeFactor legs, so at most
// sessionCounter*MuxCon/StripeFactor flows run at once; the budget grows as the
// pool does. Returns false only if the transport is shutting down.
func (s *WsMuxTransport) acquireStripedSlot() bool {
	sf := int32(s.legsPerFlow())
	if sf < 1 {
		sf = 1
	}
	for {
		active := atomic.LoadInt32(&s.stripedFlows)
		budget := atomic.LoadInt32(&s.sessionCounter) * int32(s.config.MuxCon)
		if budget < sf {
			budget = sf // always admit at least one flow while the pool warms up
		}
		if (active+1)*sf <= budget {
			if atomic.CompareAndSwapInt32(&s.stripedFlows, active, active+1) {
				return true
			}
			continue // lost the CAS race, re-read and retry
		}
		// At budget: ask the client to dial another pool session so the budget
		// can grow. Under striping the usual growth trigger in localListener
		// (streamCounter >= sessionCounter*MuxCon) rarely fires - streamCounter
		// counts logical flows, but each flow consumes StripeFactor streams, so
		// the slot cap is hit long before that threshold and the pool would
		// otherwise never grow to meet striped demand.
		select {
		case s.reqNewConnChan <- struct{}{}:
		default:
		}
		select {
		case <-s.ctx.Done():
			return false
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// requeueOrDrop puts a connection whose striped setup could not complete back on
// localChannel to be retried, or closes it (and releases its streamCounter slot)
// if the channel is full.
func (s *WsMuxTransport) requeueOrDrop(incomingConn LocalTCPConn) {
	select {
	case s.localChannel <- incomingConn:
	default:
		incomingConn.conn.Close()
		atomic.AddInt32(&s.streamCounter, -1)
	}
}

func (s *WsMuxTransport) dispatchPromotable(appConn net.Conn, plainStream net.Conn, flowID uint64, remoteAddr string) {
	swapper := handlers.PromotablePump(s.ctx, s.config.ProxyProtocol, appConn, plainStream, s.logger, s.usageMonitor, appConn.LocalAddr().(*net.TCPAddr).Port, s.config.Sniffer)

	if swapper == nil {
		return // failed proxy protocol
	}

	go func() {
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-swapper.DoneWait(): // Need a wait channel
				return
			case <-time.After(100 * time.Millisecond):
				if swapper.UpBytes() >= s.config.PromoteBytes {
					if s.shouldPromote() {
						s.promoteFlow(flowID, remoteAddr, swapper, plainStream)
					}
					// Whether or not it promoted, stop checking: a flow that
					// stayed plain because the pool was busy keeps running plain
					// (already spread across CDNs by load-balancing) for its life.
					return
				}
			}
		}
	}()

	// Block until the flow (and any promotion) is fully done, so the caller's
	// pool-slot accounting is released only when the flow actually finishes.
	<-swapper.DoneWait()
}

// shouldPromote decides whether a flow that has crossed promote_bytes should
// migrate to a striped group or stay plain. Striping one flow across legsPerFlow
// legs helps a single heavy flow reach several CDNs it otherwise couldn't. But
// when many flows are already active, plain load-balancing is already spreading
// them across every CDN, and promoting each one (each grabbing legsPerFlow legs)
// over-subscribes the pool and couples the legs under in-order reassembly -
// which collapses aggregate throughput instead of raising it (the "climbs then
// crashes mid-test" upload symptom). So only promote while few enough flows are
// active that their striped groups still fit the pool on distinct CDNs; beyond
// that, staying plain aggregates better.
func (s *WsMuxTransport) shouldPromote() bool {
	s.sessionsMu.Lock()
	cdns := make(map[string]struct{}, len(s.sessions))
	for _, ps := range s.sessions {
		cdns[ps.cdn] = struct{}{}
	}
	distinct := len(cdns)
	s.sessionsMu.Unlock()

	legs := s.legsPerFlow()
	if legs < 1 {
		legs = 1
	}
	budget := int32(distinct / legs)
	if budget < 1 {
		budget = 1 // always let a single heavy flow promote, even with one CDN
	}
	return atomic.LoadInt32(&s.plainFlows) <= budget
}

func (s *WsMuxTransport) promoteFlow(flowID uint64, remoteAddr string, swapper *handlers.PumpSwapper, plainStream net.Conn) {
	// 2. Open legs
	legs, err := s.openStripedLegs(s.legsPerFlow())
	if err != nil {
		s.logger.Tracef("failed to open striped legs for promotion: %v", err)
		return // abort, flow continues on plain
	}

	gid := atomic.AddUint32(&s.stripeGroupID, 1)
	for i, stream := range legs {
		if err := utils.SendFlowPromote(stream, flowID, gid, uint8(i), uint8(len(legs)), uint8(s.config.StripeParity)); err != nil {
			s.logger.Tracef("failed to send promote header: %v", err)
			for _, st := range legs {
				st.Close()
			}
			return
		}
	}

	conns := make([]net.Conn, len(legs))
	for i, st := range legs {
		conns[i] = st
	}

	var serverStriped net.Conn
	if s.config.StripeParity > 0 {
		fecConn, err := striping.NewFEC(conns, striping.DefaultChunkSize, s.config.StripeFactor, s.config.StripeParity)
		if err != nil {
			s.logger.Errorf("promotion striped dispatch: %v", err)
			for _, st := range legs {
				st.Close()
			}
			return
		}
		serverStriped = fecConn
	} else {
		serverStriped = striping.New(conns, striping.DefaultChunkSize)
	}

	own := swapper.FreezeUp() // server is sending to Client (download direction for user)

	// Write own on leg 0 of the new stripe groups
	if err := utils.WriteCount(legs[0], own); err != nil {
		serverStriped.Close()
		return
	}

	// 5. Server reads peer off leg 0
	peer, err := utils.ReadCount(legs[0])
	if err != nil {
		serverStriped.Close()
		return
	}

	// 7. Resume: switch swapper
	swapper.Install(serverStriped, peer)
}
