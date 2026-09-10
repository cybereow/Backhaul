package transport

import (
	"net"
	"sync/atomic"
	"time"

	"github.com/musix/backhaul/internal/utils"
	"github.com/musix/backhaul/internal/utils/handlers"
	"github.com/xtaci/smux"
)

func (s *WsMuxTransport) handleSession(session *smux.Session) {
	counter := make(chan struct{}, s.config.MuxCon)
	defer session.Close() // runs after retireSession below has drained the session
	defer close(counter)

	// Retire this session before it gets old enough for the CDN's own max-age
	// reset to land on it. The age is jittered because the initial pool is
	// dialled all at once - on a fixed age every connection would rotate in the
	// same second, which is both a reconnect storm and a nice periodic
	// signature. A nil channel (rotation disabled) blocks forever in the select.
	var rotate <-chan time.Time
	if s.config.MaxConnAge > 0 {
		rotateTimer := time.NewTimer(utils.JitterDuration(s.config.MaxConnAge))
		defer rotateTimer.Stop()
		rotate = rotateTimer.C
	}
	// replaced closes once a replacement pool connection has actually been
	// admitted. Nil until rotation starts, so the select ignores it.
	var replaced chan struct{}

	for {
		// +1 for mux connection counter
		counter <- struct{}{}

		select {
		case <-s.ctx.Done():
			return

		case <-rotate:
			<-counter // hand back the slot reserved above; no connection used it
			rotate = nil

			// Make before break: order the replacement, but keep serving on this
			// connection until it is actually up. That is the point of waiting -
			// if the client cannot dial (edge IP blackholed, CDN refusing the
			// upgrade) an aging connection still carries traffic until the CDN
			// resets it, while a closed one carries nothing.
			replaced = make(chan struct{})
			go func(ch chan struct{}) {
				if s.awaitReplacement(session) {
					close(ch)
				}
			}(replaced)
			continue

		case <-replaced:
			<-counter // hand back the slot reserved above; no connection used it
			atomic.AddInt32(&s.sessionCounter, -1)
			s.retireSession(session)
			return

		case incomingConn := <-s.localChannel:
			if time.Now().UnixMilli()-incomingConn.timeCreated > 3000 { // 3000ms
				s.logger.Debugf("timeouted local connection: %d ms", time.Now().UnixMilli()-incomingConn.timeCreated)
				incomingConn.conn.Close()

				// Decrement the counter
				atomic.AddInt32(&s.streamCounter, -1)
				<-counter
				continue
			}

			stream, err := session.OpenStream()
			if err != nil {
				s.handleSessionError(&incomingConn, err)
				return
			}

			var flowID uint64
			var promotable bool

			if s.config.MuxVersion >= 2 && s.config.PromoteBytes > 0 {
				promotable = true
				flowID = uint64(time.Now().UnixNano())
				if err := utils.SendFlowPlain(stream, flowID, incomingConn.remoteAddr); err != nil {
					s.logger.Tracef("failed to send plain flow header: %v", err)
					stream.Close()
					select {
					case s.localChannel <- incomingConn:
					default:
						incomingConn.conn.Close()
						atomic.AddInt32(&s.streamCounter, -1)
					}
					<-counter
					continue
				}
			} else {
				// Send the target port over the tunnel connection (legacy mode)
				if err := utils.SendBinaryString(stream, incomingConn.remoteAddr); err != nil {
					s.logger.Tracef("failed to send address over stream: %v", err)
					// Close the stream that never served traffic - leaving it open
					// leaks the smux stream. Requeue non-blocking (a full
					// localChannel would otherwise park this goroutine forever and
					// permanently burn a MuxCon slot), dropping the conn if full.
					stream.Close()
					select {
					case s.localChannel <- incomingConn:
					default:
						incomingConn.conn.Close()
						atomic.AddInt32(&s.streamCounter, -1)
					}
					<-counter // release the mux slot reserved at the top of the loop
					continue
				}
			}

			// Handle data exchange between connections
			go func() {
				if promotable {
					s.dispatchPromotable(incomingConn.conn, stream, flowID, incomingConn.remoteAddr)
				} else {
					handlers.TCPConnectionHandler(s.ctx, s.config.ProxyProtocol, incomingConn.conn, stream, s.logger, s.usageMonitor, incomingConn.conn.LocalAddr().(*net.TCPAddr).Port, s.config.Sniffer)
				}
				atomic.AddInt32(&s.streamCounter, -1)
				<-counter // read signal from the channel
			}()
		}
	}
}

// rotateRetryInterval is how long rotation waits before re-checking for the
// replacement connection it asked for. Deliberately unhurried: the connection
// is only aging, and the client may be unable to dial at all for minutes.
const rotateRetryInterval = 30 * time.Second

// requestReplacement asks the client to bring up one more pool connection.
func (s *WsMuxTransport) requestReplacement() {
	select {
	case s.reqNewConnChan <- struct{}{}:
	default:
		s.logger.Warn("failed to request a replacement connection for rotation. channel is full")
	}
}

// retireSession takes a pool session out of service before it is old enough
// for a CDN/LB max-age reset to kill it mid-flow, waiting for the streams
// already running on it to finish. Callers only get here once a replacement
// connection has actually joined the pool (see requestReplacement), so this
// never shrinks the pool. The caller closes the session once this returns.
//
// Draining is the point: a long-lived flow - an SSH session, a large download -
// is pinned to one connection because smux cannot migrate a live stream, so
// cutting the connection at rotation time would cut the flow. Instead the
// retiring connection takes no new streams and stays up until its last one
// ends. That buys such a flow the whole window up to the CDN's hard limit; the
// limit itself is not something client-side code can extend.
func (s *WsMuxTransport) retireSession(session *smux.Session) {
	s.logger.Debugf("retiring pool session at max_conn_age, %d live stream(s) to drain", session.NumStreams())

	// ponytail: poll for drain rather than wiring per-stream completion
	// signalling; a 1s tick is plenty for a connection on its way out.
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		if session.IsClosed() {
			return
		}
		if session.NumStreams() == 0 {
			s.logger.Debug("retired pool session drained, closing")
			return
		}
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// rotateStripedSession is the striped path's equivalent of the rotation branch
// in handleSession: at max_conn_age the session is pulled out of the leg pool
// so no new stripe legs land on it, then drained and closed. The sessionCounter
// decrement is left to the CloseChan watcher registered in handleLoop.
func (s *WsMuxTransport) rotateStripedSession(session *smux.Session) {
	rotateTimer := time.NewTimer(utils.JitterDuration(s.config.MaxConnAge))
	defer rotateTimer.Stop()

	select {
	case <-s.ctx.Done():
		return
	case <-session.CloseChan():
		return
	case <-rotateTimer.C:
	}

	// Make before break. Unlike the non-striped path this can block: the session
	// keeps serving legs from the registry until unregisterSession below, so
	// waiting here costs no capacity.
	if !s.awaitReplacement(session) {
		return
	}

	s.unregisterSession(session)
	s.retireSession(session)
	session.Close()
}

// awaitReplacement asks for a replacement pool connection and waits until one
// has actually been admitted, re-asking on every retry because the client's
// tunnelDialer abandons a failed dial for good. Returns false if the context
// ended or the session died while waiting - in both cases there is nothing left
// to rotate. It never gives up otherwise: retiring a connection the pool has no
// replacement for would leave less capacity than before rotation started.
func (s *WsMuxTransport) awaitReplacement(session *smux.Session) bool {
	// Mark first, ask second: a replacement admitted from here on counts.
	mark := atomic.LoadInt32(&s.admittedSessions)
	s.requestReplacement()

	// ponytail: poll the counter instead of signalling admissions to whoever is
	// waiting. Rotation is not latency-sensitive - a second either way is noise
	// against a max_conn_age measured in minutes.
	poll := time.NewTicker(time.Second)
	defer poll.Stop()
	reask := time.NewTicker(utils.JitterDuration(rotateRetryInterval))
	defer reask.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return false
		case <-session.CloseChan():
			return false
		case <-reask.C:
			s.logger.Debugf("rotation deferred: replacement pool connection is not up, keeping the aging one in service (re-asking every ~%s)", rotateRetryInterval)
			s.requestReplacement()
		case <-poll.C:
		}
		if atomic.LoadInt32(&s.admittedSessions) > mark {
			return true
		}
	}
}

func (s *WsMuxTransport) handleSessionError(incomingConn *LocalTCPConn, err error) {
	s.logger.Tracef("failed to handle session: %v", err)

	// decrease session value
	atomic.AddInt32(&s.sessionCounter, -1)

	// Put local connection back to local channel (non-blocking): a blocking
	// send on a full localChannel would park this goroutine forever, and the
	// caller returns right after this - leaking the session and its counter slot.
	select {
	case s.localChannel <- *incomingConn:
	default:
		incomingConn.conn.Close()
		atomic.AddInt32(&s.streamCounter, -1)
	}

	// Attempt to request a new connection
	select {
	case s.reqNewConnChan <- struct{}{}:
	default:
		s.logger.Warn("request new connection channel is full")
	}
}
