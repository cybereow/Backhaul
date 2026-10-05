package transport

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/musix/backhaul/internal/smux"
	"github.com/musix/backhaul/internal/utils"
	"github.com/musix/backhaul/internal/utils/handlers"
)

// Resumable flows (docs/resumable-flows-plan.md, level A).
//
// A plain flow normally lives and dies on one smux stream, which lives and dies
// with one pool session, which the CDN cuts at its max connection age. With
// cdn_max_age set, plain flows are opened as resumable instead: their payload is
// wrapped in the half-close envelope from the first byte and run through a
// PumpSwapper, so that when the session carrying them is retired the flow is moved
// to a stream on another session, byte for byte, instead of being cut.
//
// A promotable flow (promote_bytes) is opened the same way, without replay state:
// once it has been promoted its tunnel is a striped group with a leg on several
// sessions, and when any of them is retired the whole flow is moved onto a fresh
// group on the sessions that stay. Promotion and that move are the same swap.
//
// ponytail: a flow striped from its first byte (stripe_ports, or no promote_bytes)
// has no flow id on the wire and is still cut when a session under it is retired;
// giving it one is a protocol change, to be made when someone runs that with
// cdn_max_age.

var (
	errNoOtherSession = errors.New("no other live pool session to move the flow to")
	errAttachRefused  = errors.New("the client refused to move the flow")
)

// resumableFlow is one running resumable flow and the pool sessions carrying its
// tunnel: one for a plain stream, one per leg once it is promoted, and both the
// old and the new ones while a swap between two tunnels is in flight.
type resumableFlow struct {
	id uint64
	sw *handlers.PumpSwapper
	g  *wsGeneration // the generation it runs in: its follow-up work is that one's

	mu      sync.Mutex
	sess    []*smux.Session // where its tunnel is
	striped bool            // promoted: a move builds a new striped group, not a single stream
	moves   []*flowMove     // swaps in flight, each with the sessions it is taking the flow onto
	swaps   uint64          // tunnels built so far (see took)
	at      uint64          // which of them sess and striped describe
}

// flowMove is one swap in flight. Each attempt keeps its own entry: two of them
// often pick the same sessions, and the one that is refused must take back only
// what it recorded, not what the other, which went through, has made the flow's.
type flowMove struct {
	to  []*smux.Session
	seq uint64 // its place among the flow's swaps, once its tunnel is built
}

// session is the session of a flow on a plain stream (the first leg of a
// promoted one).
func (f *resumableFlow) session() *smux.Session {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.sess) == 0 {
		return nil
	}
	return f.sess[0]
}

// moving records that a swap is taking the flow onto ss: until it is settled the
// flow counts as carried by the sessions it is on and by these, so the retirement
// of any of them sees it. A swap that goes through ends with moved, one that is
// refused with stayed.
func (f *resumableFlow) moving(ss []*smux.Session) *flowMove {
	m := &flowMove{to: ss}
	f.mu.Lock()
	f.moves = append(f.moves, m)
	f.mu.Unlock()
	return m
}

// stayed ends a swap that was refused: the flow is where it was (or where
// another swap, which went through, has put it).
func (f *resumableFlow) stayed(m *flowMove) {
	f.mu.Lock()
	f.dropMoveLocked(m)
	f.mu.Unlock()
}

// took gives m its place among the flow's swaps. It is called while m's tunnel is
// being built: the swapper is frozen or suspended then and runs one swap at a
// time, so the order taken here is the order the tunnels were installed in.
func (f *resumableFlow) took(m *flowMove) {
	f.mu.Lock()
	f.swaps++
	m.seq = f.swaps
	f.mu.Unlock()
}

// moved ends a swap that went through: the flow's tunnel is now on m's sessions,
// unless a later swap has been through meanwhile. The caller of an earlier one
// can get here after it, and must not put the flow back on record where it no
// longer is.
func (f *resumableFlow) moved(m *flowMove, striped bool) {
	f.mu.Lock()
	f.dropMoveLocked(m)
	if m.seq > f.at {
		f.sess, f.striped, f.at = m.to, striped, m.seq
	}
	f.mu.Unlock()
}

func (f *resumableFlow) dropMoveLocked(m *flowMove) {
	for i, x := range f.moves {
		if x == m {
			f.moves = append(f.moves[:i:i], f.moves[i+1:]...)
			return
		}
	}
}

func (f *resumableFlow) on(s *smux.Session) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, x := range f.sess {
		if x == s {
			return true
		}
	}
	for _, m := range f.moves {
		for _, x := range m.to {
			if x == s {
				return true
			}
		}
	}
	return false
}

func (f *resumableFlow) isStriped() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.striped
}

// newFlowID is a random non-zero 64-bit id. It is not guessable, and it is never
// a time stamp, so two flows opened in the same nanosecond cannot collide.
func newFlowID() uint64 {
	for {
		var b [8]byte
		if _, err := crand.Read(b[:]); err != nil {
			continue
		}
		if id := binary.BigEndian.Uint64(b[:]); id != 0 {
			return id
		}
	}
}

// resumable reports whether plain flows are opened as resumable.
func (s *WsMuxTransport) resumable() bool {
	return s.config.MuxVersion >= 2 && s.config.MaxConnAge > 0
}

func (s *WsMuxTransport) registerFlow(f *resumableFlow) {
	s.flowsMu.Lock()
	if s.flows == nil {
		s.flows = make(map[uint64]*resumableFlow)
	}
	s.flows[f.id] = f
	s.flowsMu.Unlock()
}

func (s *WsMuxTransport) unregisterFlow(id uint64) {
	s.flowsMu.Lock()
	delete(s.flows, id)
	s.flowsMu.Unlock()
}

// flowsOn returns the resumable flows currently carried by sess.
func (s *WsMuxTransport) flowsOn(sess *smux.Session) []*resumableFlow {
	s.flowsMu.Lock()
	defer s.flowsMu.Unlock()
	var out []*resumableFlow
	for _, f := range s.flows {
		if f.on(sess) {
			out = append(out, f)
		}
	}
	return out
}

// dispatchResumable runs one resumable flow until it ends. The swapper is
// registered before its pumps start, so a move can never find a half-built flow.
// With replayLimit > 0 the flow also keeps what it sent until acknowledged and is
// resumed on another session if its own is cut without warning. A promotable flow
// is also moved onto a striped group once it has sent promote_bytes.
func (s *WsMuxTransport) dispatchResumable(g *wsGeneration, appConn net.Conn, stream *smux.Stream, ps *pooledSession, flowID uint64, grant *handlers.ReplayGrant, promotable bool) {
	defer grant.Release()
	sw := handlers.NewPromotablePump(g.ctx, s.config.ProxyProtocol, appConn, handlers.NewHalfCloseConn(stream), s.logger, s.usageMonitor, appConn.LocalAddr().(*net.TCPAddr).Port, s.config.Sniffer)
	if sw == nil {
		return // failed proxy protocol
	}
	sw.HoldReleasedTunnels() // this end closes the sessions; see PumpSwapper.release
	if grant != nil {
		// The client was told (by the flow's kind byte) that this flow keeps replay
		// state, so a flow that cannot is not an option: end it.
		sw.SetReplayGrower(grant.Grow)
		sw.OnReplayEnd(grant.Release)
		if err := sw.EnableReplay(grant.Limit()); err != nil {
			s.logger.Errorf("resumable flow %d: %v", flowID, err)
			sw.Abort()
			return
		}
		sw.SetResumeWindow(s.config.ResumeWindow)
	}
	f := &resumableFlow{id: flowID, sw: sw, g: g, sess: []*smux.Session{ps.session}}
	s.registerFlow(f)
	defer s.unregisterFlow(flowID)
	sw.Start()
	if grant != nil {
		go s.driveResume(g.ctx, f)
	}
	if promotable {
		s.watchPromotion(g, flowID, sw, f)
	}
	<-sw.DoneWait()
}

var errFlowGone = errors.New("the client no longer has this flow")

// driveResume resumes the flow each time it is suspended (its stream or session
// died without warning) by opening a stream on another live session and running
// the resume handshake on it, until the flow ends. The flow aborts itself when its
// resume window runs out, which ends this too.
func (s *WsMuxTransport) driveResume(ctx context.Context, f *resumableFlow) {
	for {
		// A flow idle in a socket read performs no tunnel I/O that could fail, so its
		// pumps may not notice its session died: watch the session too.
		sess := f.session()
		select {
		case <-f.sw.SuspendedCh():
		case <-sess.CloseChan():
			if f.session() != sess {
				continue // it was moved off this session meanwhile
			}
			if !f.sw.Suspend() {
				return // finished, or cannot be resumed
			}
		case <-f.sw.DoneWait():
			return
		case <-ctx.Done():
			return
		}
		backoff := 100 * time.Millisecond
		started := time.Now()
		for attempt := 1; ; attempt++ {
			err := s.resumeOnce(ctx, f)
			if err == nil {
				s.logger.Debugf("flow %d resumed on another session after %v (%d attempt(s))", f.id, time.Since(started).Round(time.Millisecond), attempt)
				break
			}
			if errors.Is(err, errFlowGone) {
				f.sw.Abort()
				return
			}
			select {
			case <-f.sw.DoneWait():
				return
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 2*time.Second {
				backoff *= 2
			}
		}
	}
}

// resumeOnce makes one attempt to resume a suspended flow on a fresh stream.
func (s *WsMuxTransport) resumeOnce(ctx context.Context, f *resumableFlow) error {
	stream, ps, err := s.openPlainLegPS()
	if err != nil {
		return fmt.Errorf("%w: %v", errNoOtherSession, err)
	}
	_ = stream.SetDeadline(time.Now().Add(handlers.PromoteHandshakeTimeout))
	if err := utils.SendFlowAttach(stream, f.id, utils.AttachResume, 0); err != nil {
		stream.Close()
		return err
	}
	accept, reason, err := utils.ReadAttachVerdict(stream)
	if err != nil {
		stream.Close()
		return err
	}
	if !accept {
		stream.Close()
		if reason == utils.AttachRejectUnknownFlow {
			return errFlowGone
		}
		return fmt.Errorf("%w (reason %d)", errAttachRefused, reason)
	}
	_ = stream.SetDeadline(time.Time{})

	m := f.moving([]*smux.Session{ps.session})
	err = f.sw.Resume(ctx, stream, func() (net.Conn, error) {
		f.took(m)
		return handlers.NewHalfCloseConn(stream), nil
	})
	if err != nil {
		f.stayed(m)
		return err
	}
	f.moved(m, false)
	return nil
}

// migrateFlow moves one flow off the session old, which is being retired: onto a
// fresh stream on another live session, or, for a promoted flow, onto a fresh
// striped group. The client may refuse before anything freezes (unknown or
// finished flow, swap already running), in which case the flow is untouched. A
// failure after both ends froze aborts the flow, which is no worse than the cut it
// was avoiding.
func (s *WsMuxTransport) migrateFlow(ctx context.Context, f *resumableFlow, old *smux.Session) error {
	if !f.on(old) {
		return nil // a swap that was in flight has taken it off already
	}
	if f.isStriped() {
		return s.promoteFlow(ctx, f.id, f.sw, f)
	}
	// The retiring session has already left the registry, so this picks among the
	// others: lowest score wins, and the age bias prefers the youngest.
	stream, ps, err := s.openPlainLegPS()
	if err != nil {
		return fmt.Errorf("%w: %v", errNoOtherSession, err)
	}
	if f.on(ps.session) {
		stream.Close()
		return errNoOtherSession
	}

	_ = stream.SetDeadline(time.Now().Add(handlers.PromoteHandshakeTimeout))
	if err := utils.SendFlowAttach(stream, f.id, utils.AttachDrained, 0); err != nil {
		stream.Close()
		return err
	}
	accept, reason, err := utils.ReadAttachVerdict(stream)
	if err != nil {
		stream.Close()
		return err
	}
	if !accept {
		stream.Close()
		return fmt.Errorf("%w (reason %d)", errAttachRefused, reason)
	}
	_ = stream.SetDeadline(time.Time{})

	// Record the new session before the swap, so a retirement of it that lands
	// during the swap already sees this flow. A swap refused before it froze
	// leaves the flow where it was.
	m := f.moving([]*smux.Session{ps.session})
	err = f.sw.Promote(ctx, []net.Conn{stream}, func() (net.Conn, error) {
		f.took(m)
		return handlers.NewHalfCloseConn(stream), nil
	})
	if err != nil {
		f.stayed(m)
		return err
	}
	f.moved(m, false)
	s.moveOffRetired(f, m.to)
	return nil
}

// moveOffRetired looks, once a flow has been put on sessions, whether any of them
// was retired or lost meanwhile, and moves the flow off again if so. A session picked for
// a move can begin its rotation before the move is on record: that rotation's
// look at the flows on it then comes too early to see this one, and nothing else
// would move it off before the session's drain ends.
//
// The follow-up is work of the flow's own generation: counted by it, so a Restart
// waits for it before it resets the registries, and bounded by its context.
func (s *WsMuxTransport) moveOffRetired(f *resumableFlow, sessions []*smux.Session) {
	g := f.g
	for _, sess := range sessions {
		sess := sess
		switch {
		case sess.IsClosed():
			// It ended while the move was under way, and its close was looked at
			// before the move was on record (see regroupFlowsOn). A striped group
			// that got through on its other legs is a leg short: rebuild it. A
			// single stream on it is gone with it (or is resumed by driveResume).
			if f.isStriped() {
				g.start(func() {
					ctx, cancel := context.WithTimeout(g.ctx, regroupWindow)
					defer cancel()
					s.moveFlowsOff(ctx, sess, []*resumableFlow{f}, 0, "lost session")
				})
			}
		case !s.registered(sess):
			g.start(func() {
				budget := s.config.MaxDrain
				if budget <= 0 {
					budget = regroupWindow
				}
				ctx, cancel := context.WithTimeout(g.ctx, budget)
				defer cancel()
				s.moveFlowsOff(ctx, sess, []*resumableFlow{f}, migrateAttempts, "retiring session")
			})
		}
	}
}

// registered reports whether session is in the pool's registry: taking new
// streams, not retired.
func (s *WsMuxTransport) registered(session *smux.Session) bool {
	s.sessionsMu.Lock()
	defer s.sessionsMu.Unlock()
	for _, ps := range s.sessions {
		if ps.session == session {
			return true
		}
	}
	return false
}

// Moving a flow is tried again when it could not be started: a promotion or
// another move of the same flow is still settling, or the pool is momentarily too
// narrow for a group. A rotation gives a flow a few tries, so that flows that
// cannot be moved do not hold the others up; rebuilding a group after a lost
// session keeps trying for its whole window, because what it waits for is the
// client dialling a replacement, which can take that long.
const (
	migrateAttempts = 5
	migrateRetry    = time.Second
)

// migrateParallel bounds how many flows are moved at once, so the handshakes of a
// busy session do not all compete for the same few streams and time out.
const migrateParallel = 8

// migrateFlowsOff moves every resumable flow off a session that is being retired.
// It returns when all of them have been moved or given up on; ctx bounds it.
func (s *WsMuxTransport) migrateFlowsOff(ctx context.Context, old *smux.Session) {
	flows := s.flowsOn(old)
	if failed, why := s.moveFlowsOff(ctx, old, flows, migrateAttempts, "retiring session"); failed > 0 {
		s.recordEvent("flows_not_moved", fmt.Sprintf("%d of %d flow(s) could not be moved off a retiring session; they stay on it until it drains or is cut (%v)", failed, len(flows), why))
	}
}

// regroupWindow bounds the rebuilding of the striped groups a lost session left
// short of a leg.
const regroupWindow = 30 * time.Second

// regroupFlowsOn rebuilds the striped group of every promoted flow that had a leg
// on dead, a session that ended without being retired first. Only a group with
// parity to spare (mux_stripe_parity) is still running then, on fewer legs than
// configured: left like that, the next lost leg could be one too many, so it is
// moved onto a full group at once. (A flow on a plain stream is not handled
// here: with replay state it is resumed by driveResume, without it it has ended.)
func (s *WsMuxTransport) regroupFlowsOn(ctx context.Context, dead *smux.Session) {
	var striped []*resumableFlow
	for _, f := range s.flowsOn(dead) {
		if f.isStriped() {
			striped = append(striped, f)
		}
	}
	ctx, cancel := context.WithTimeout(ctx, regroupWindow)
	defer cancel()
	s.moveFlowsOff(ctx, dead, striped, 0, "lost session")
}

// moveFlowsOff moves flows off the session old, a few at a time, trying again the
// ones that could not be started: attempts times each, or until ctx ends when
// attempts is 0. It returns how many it had to leave, and why the last of them.
//
// A flow that keeps replay state is not left: when it cannot be moved the
// orderly way - which needs both ends to stop at a write boundary, and an end
// whose reader has stopped reading (a paused download) never gets to one - it is
// taken off the session the way a cut would take it, suspended here and resumed
// on another by driveResume, replaying what was in flight.
func (s *WsMuxTransport) moveFlowsOff(ctx context.Context, old *smux.Session, flows []*resumableFlow, attempts int, what string) (int, error) {
	if len(flows) == 0 {
		return 0, nil
	}
	var moved, failed, finished, resumed int
	var why error
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, migrateParallel)
	for _, f := range flows {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			mu.Lock()
			failed++
			why = ctx.Err()
			mu.Unlock()
			continue
		}
		wg.Add(1)
		go func(f *resumableFlow) {
			defer wg.Done()
			defer func() { <-sem }()
			var err error
			for attempt := 1; ; attempt++ {
				// A flow that is suspended or being swapped right now is not ours to
				// move; it may be by the next look.
				if err = f.sw.Unswappable(); f.sw.Swappable() || !f.on(old) {
					err = s.migrateFlow(ctx, f, old)
				}
				if err == nil || attempt == attempts {
					break
				}
				select {
				case <-f.sw.DoneWait():
				case <-ctx.Done():
				case <-time.After(migrateRetry):
					continue
				}
				break
			}
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				select {
				case <-f.sw.DoneWait():
					// It ended on its own while the move was in flight: nothing was
					// left to move, and nothing was lost.
					finished++
					return
				default:
				}
				s.logger.Debugf("%s: moving flow %d off it: %v", what, f.id, err)
				// With nowhere to resume it, it is better off where it is.
				if !errors.Is(err, errNoOtherSession) && !f.isStriped() && f.session() == old && f.sw.Suspend() {
					resumed++
					return
				}
				failed++
				why = err
				return
			}
			moved++
		}(f)
	}
	wg.Wait()
	s.logger.Debugf("%s: %d flow(s) moved, %d resumed elsewhere, %d finished meanwhile, %d left on it", what, moved, resumed, finished, failed)
	return failed, why
}
