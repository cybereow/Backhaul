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

	"github.com/musix/backhaul/internal/utils"
	"github.com/musix/backhaul/internal/utils/handlers"
	"github.com/xtaci/smux"
)

// Resumable flows (docs/resumable-flows-plan.md, level A).
//
// A plain flow normally lives and dies on one smux stream, which lives and dies
// with one pool session, which the CDN cuts at its max connection age. With
// cdn_max_age set, plain flows are opened as resumable instead: their payload is
// wrapped in the half-close envelope from the first byte and run through a
// PumpSwapper, so that when the session carrying them is retired the flow is moved
// to a stream on another session, byte for byte, instead of being cut.

var (
	errNoOtherSession = errors.New("no other live pool session to move the flow to")
	errAttachRefused  = errors.New("the client refused to move the flow")
)

// resumableFlow is one running resumable flow and the pool session carrying it.
type resumableFlow struct {
	id uint64
	sw *handlers.PumpSwapper

	mu   sync.Mutex
	sess *smux.Session
}

func (f *resumableFlow) session() *smux.Session {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sess
}

func (f *resumableFlow) setSession(s *smux.Session) {
	f.mu.Lock()
	f.sess = s
	f.mu.Unlock()
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

// resumable reports whether plain flows are opened as resumable. Promotion (a
// plain flow moving onto a striped group) takes precedence: the two use the same
// swapper for different purposes and are not combined.
func (s *WsMuxTransport) resumable(promotable bool) bool {
	return s.config.MuxVersion >= 2 && s.config.MaxConnAge > 0 && !promotable
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
		if f.session() == sess {
			out = append(out, f)
		}
	}
	return out
}

// dispatchResumable runs one resumable flow until it ends. The swapper is
// registered before its pumps start, so a move can never find a half-built flow.
// With replayLimit > 0 the flow also keeps what it sent until acknowledged and is
// resumed on another session if its own is cut without warning.
func (s *WsMuxTransport) dispatchResumable(g *wsGeneration, appConn net.Conn, stream *smux.Stream, ps *pooledSession, flowID uint64, grant *handlers.ReplayGrant) {
	defer grant.Release()
	sw := handlers.NewPromotablePump(g.ctx, s.config.ProxyProtocol, appConn, handlers.NewHalfCloseConn(stream), s.logger, s.usageMonitor, appConn.LocalAddr().(*net.TCPAddr).Port, s.config.Sniffer)
	if sw == nil {
		return // failed proxy protocol
	}
	if grant != nil {
		// The client was told (by the flow's kind byte) that this flow keeps replay
		// state, so a flow that cannot is not an option: end it.
		sw.SetReplayGrower(grant.Grow)
		if err := sw.EnableReplay(grant.Limit()); err != nil {
			s.logger.Errorf("resumable flow %d: %v", flowID, err)
			sw.Abort()
			return
		}
		sw.SetResumeWindow(s.config.ResumeWindow)
	}
	f := &resumableFlow{id: flowID, sw: sw, sess: ps.session}
	s.registerFlow(f)
	defer s.unregisterFlow(flowID)
	sw.Start()
	if grant != nil {
		go s.driveResume(g.ctx, f)
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
		select {
		case <-f.sw.SuspendedCh():
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

	err = f.sw.Resume(ctx, stream, func() (net.Conn, error) {
		return handlers.NewHalfCloseConn(stream), nil
	})
	if err != nil {
		return err
	}
	f.setSession(ps.session)
	return nil
}

// migrateFlow moves one flow onto a fresh stream on another live session. The
// client may refuse before anything freezes (unknown or finished flow, swap
// already running), in which case the flow is untouched. A failure after both ends
// froze aborts the flow, which is no worse than the cut it was avoiding.
func (s *WsMuxTransport) migrateFlow(ctx context.Context, f *resumableFlow) error {
	// The retiring session has already left the registry, so this picks among the
	// others: lowest score wins, and the age bias prefers the youngest.
	stream, ps, err := s.openPlainLegPS()
	if err != nil {
		return fmt.Errorf("%w: %v", errNoOtherSession, err)
	}
	if ps.session == f.session() {
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
	prev := f.session()
	f.setSession(ps.session)
	err = f.sw.Promote(ctx, []net.Conn{stream}, func() (net.Conn, error) {
		return handlers.NewHalfCloseConn(stream), nil
	})
	if err != nil {
		f.setSession(prev)
		return err
	}
	return nil
}

// migrateParallel bounds how many flows are moved at once, so the handshakes of a
// busy session do not all compete for the same few streams and time out.
const migrateParallel = 8

// migrateFlowsOff moves every resumable flow off a session that is being retired.
// It returns when all of them have been moved or given up on; ctx bounds it.
func (s *WsMuxTransport) migrateFlowsOff(ctx context.Context, old *smux.Session) {
	var flows []*resumableFlow
	for _, f := range s.flowsOn(old) {
		if f.sw.Swappable() { // a flow already suspended or being moved is not ours to move
			flows = append(flows, f)
		}
	}
	if len(flows) == 0 {
		return
	}
	var moved, failed, finished int
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
			mu.Unlock()
			continue
		}
		wg.Add(1)
		go func(f *resumableFlow) {
			defer wg.Done()
			defer func() { <-sem }()
			err := s.migrateFlow(ctx, f)
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
				failed++
				s.logger.Debugf("moving flow %d off a retiring session: %v", f.id, err)
				return
			}
			moved++
		}(f)
	}
	wg.Wait()
	s.logger.Debugf("retiring session: %d flow(s) moved, %d finished meanwhile, %d left on it", moved, finished, failed)
	if failed > 0 {
		s.recordEvent("flows_not_moved", fmt.Sprintf("%d of %d flow(s) could not be moved off a retiring session; they stay on it until it drains or is cut", failed, len(flows)))
	}
}
