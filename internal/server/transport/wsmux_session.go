package transport

import (
	"context"
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
			go func(ch chan struct{}, g *wsGeneration) {
				if s.awaitReplacement(g, session) {
					close(ch)
				}
			}(replaced, s.gen)
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
				// Always promotable, so it keeps FlowPlain (legacy full-close):
				// only dispatchPlain's non-promotable flows use FlowPlainHC (plan 024).
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
					s.dispatchPromotable(s.gen, incomingConn.conn, stream, flowID, incomingConn.remoteAddr)
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
// connection has actually joined the pool (see awaitReplacement), so this
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
//
// The session stays owned by the generation (see wsGeneration) while it drains:
// unregistering it only stops new legs landing on it, so a restart during the
// drain still closes it.
func (s *WsMuxTransport) rotateStripedSession(g *wsGeneration, session *smux.Session) {
	rotateTimer := time.NewTimer(utils.JitterDuration(s.config.MaxConnAge))
	defer rotateTimer.Stop()

	select {
	case <-g.ctx.Done():
		return
	case <-session.CloseChan():
		return
	case <-rotateTimer.C:
	}

	// Make before break. Unlike the non-striped path this can block: the session
	// keeps serving legs from the registry until awaitReplacement takes it out,
	// so waiting here costs no capacity. On true the session is already
	// unregistered and the decision gate already released: draining below must
	// not hold up another rotation's decision.
	if !s.awaitReplacement(g, session) {
		return
	}

	s.retireSession(session)
	session.Close()
}

// rotatePollEvery is how often a rotation re-checks the registry for its
// replacement (WsMuxTransport.rotatePoll overrides it, tests shorten it).
const rotatePollEvery = time.Second

// awaitReplacement is one rotation's make-before-break decision. It takes the
// generation's decision gate, snapshots which sessions the pool has right now,
// asks for a replacement, and waits until a session outside that snapshot is
// registered and live. Only then, in one step under the registry lock, it takes
// the old session out of eligibility, and releases the gate (before the caller
// drains it). Returns true if the caller now owns the draining of session, false
// if the generation ended or the session died while waiting - in both cases there
// is nothing left to rotate. It never gives up otherwise: retiring a connection
// the pool has no replacement for would leave less capacity than before.
//
// Why the gate and a per-decision snapshot: a bare admission counter let two
// rotations share one increment, or count a replacement that had already died,
// so both retired against a single (or no) successor. Serial decisions, each
// snapshotting only after it holds the gate, make a successor usable once: the
// next rotation's snapshot already contains it. The old session stays eligible
// (and serving) for the whole wait, re-asking on every retry because the client's
// tunnelDialer abandons a failed dial for good.
//
// ponytail: decisions are serial - one rotation waits for its replacement while
// the others queue behind it. Rotation is minutes apart, so per-request
// replacement tickets are unnecessary until measured rotation throughput says so.
func (s *WsMuxTransport) awaitReplacement(g *wsGeneration, session *smux.Session) bool {
	ctx := s.ctx
	if g != nil {
		ctx = g.ctx
	}
	if !g.acquireRotate(ctx, session) {
		return false
	}
	defer g.releaseRotate()

	// Snapshot first, ask second: only a session admitted from here on counts.
	seen := s.snapshotSessions()
	s.requestReplacement()

	// ponytail: poll the registry instead of signalling admissions to whoever is
	// waiting. Rotation is not latency-sensitive - a second either way is noise
	// against a max_conn_age measured in minutes.
	pollEvery := s.rotatePoll
	if pollEvery <= 0 {
		pollEvery = rotatePollEvery
	}
	poll := time.NewTicker(pollEvery)
	defer poll.Stop()
	reask := time.NewTicker(utils.JitterDuration(rotateRetryInterval))
	defer reask.Stop()

	// One event per decision for a replacement that is late (the anomaly worth
	// seeing), one when the aging session is retired: two per rotation at most,
	// so rotation cannot flush the ring of control-channel history.
	deferred := false
	for {
		select {
		case <-ctx.Done():
			return false
		case <-session.CloseChan():
			return false
		case <-reask.C:
			s.logger.Debugf("rotation deferred: replacement pool connection is not up, keeping the aging one in service (re-asking every ~%s)", rotateRetryInterval)
			if !deferred {
				deferred = true
				s.recordEvent("rotation_deferred", "replacement pool connection not up; aging session kept in service, re-asking")
			}
			s.requestReplacement()
		case <-poll.C:
		}
		if s.retireIfReplaced(session, seen) {
			s.recordEvent("rotation_retired", "replacement admitted; aging session out of eligibility, draining")
			return true
		}
	}
}

// acquireRotate takes the generation's rotation decision gate, giving up when
// ctx ends or session dies first (a nil generation has no gate to take).
func (g *wsGeneration) acquireRotate(ctx context.Context, session *smux.Session) bool {
	if g == nil {
		return true
	}
	select {
	case g.rotate <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	case <-session.CloseChan():
		return false
	}
}

func (g *wsGeneration) releaseRotate() {
	if g != nil {
		<-g.rotate
	}
}

// snapshotSessions is the set of pool sessions registered right now.
func (s *WsMuxTransport) snapshotSessions() map[*smux.Session]struct{} {
	s.sessionsMu.Lock()
	defer s.sessionsMu.Unlock()
	seen := make(map[*smux.Session]struct{}, len(s.sessions))
	for _, ps := range s.sessions {
		seen[ps.session] = struct{}{}
	}
	return seen
}

// retireIfReplaced authorizes retiring old if the registry holds a live session
// that was not in seen, and in that same critical section takes old out of
// eligibility, so no other rotation can observe old as still eligible after it
// has been paid for. A successor that has died since it was admitted does not
// count.
func (s *WsMuxTransport) retireIfReplaced(old *smux.Session, seen map[*smux.Session]struct{}) bool {
	s.sessionsMu.Lock()
	defer s.sessionsMu.Unlock()
	for _, ps := range s.sessions {
		if ps.session == old || ps.session == nil || ps.session.IsClosed() {
			continue
		}
		if _, known := seen[ps.session]; known {
			continue
		}
		s.unregisterLocked(old)
		return true
	}
	return false
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
