package transport

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/musix/backhaul/internal/utils/handlers"
	"github.com/musix/backhaul/internal/utils/network"
)

// newRawFlowID returns a random identifier for a flow that can be moved.
func newRawFlowID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// errNoTunnel: no idle connection able to carry the flow turned up in time.
var errNoTunnel = errors.New("no idle raw connection to move the flow to")

// moveRetry is how long a flow waits before trying again after a move that did
// not happen (no idle connection, or the other end gave up).
const moveRetry = 2 * time.Second

// resumeLoop moves one raw flow to a fresh connection each time the one it is on
// gets near the age at which a CDN would cut it (the rotation age, see
// RotationPlan), so a flow outlives that limit. It runs for the life of the flow.
func (s *WsTransport) resumeLoop(ctx context.Context, f *handlers.RawFlow) {
	for {
		due := f.CurrentBorn().Add(rotateAge(s.config.MaxConnAge))
		for {
			wait := time.Until(due)
			if wait < 0 {
				wait = 0
			}
			select {
			case <-ctx.Done():
				return
			case <-f.Done():
				return
			case <-time.After(wait):
			}
			err := s.moveFlow(ctx, f)
			if err == nil {
				s.moves.Add(1)
				break // scheduled again from the new connection's age
			}
			if errors.Is(err, handlers.ErrFlowCut) {
				s.logger.Debugf("raw flow %s was cut while moving: %v", f.ID(), err)
				return
			}
			s.logger.Tracef("raw flow %s not moved: %v", f.ID(), err)
			due = time.Now().Add(moveRetry)
		}
	}
}

// moveFlow takes an idle connection from the pool, tells the client that it now
// carries flow f, and runs the move on it.
func (s *WsTransport) moveFlow(ctx context.Context, f *handlers.RawFlow) error {
	tc, err := s.claimRawTunnel(ctx, 3*time.Second)
	if err != nil {
		return err
	}
	if err := tc.conn.WriteMessage(network.TextMessage, []byte(handlers.RawFlowResumePrefix+f.ID())); err != nil {
		tc.conn.Close()
		return err
	}
	left, raw := tc.conn.Detach()
	return f.Swap(&handlers.RawTunnel{Conn: raw, Left: left, Born: tc.born})
}

// claimRawTunnel takes an idle pool connection that can carry raw flows, the way
// a new flow does (the hand-over lock, never released), and asks the client for a
// replacement, since one has left the pool.
func (s *WsTransport) claimRawTunnel(ctx context.Context, wait time.Duration) (*TunnelChannel, error) {
	deadline := time.Now().Add(wait)
	timer := time.NewTimer(wait)
	defer timer.Stop()
	// Entries that are no use to a move are put back; look at each only once.
	skipped := 0
	// The client is asked for a replacement up front: the pool may be empty.
	select {
	case s.reqNewConnChan <- struct{}{}:
	default:
	}
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, errNoTunnel
		case tc := <-s.tunnelChannel:
			if tc.isRetired() {
				continue
			}
			if !tc.rawOK {
				select {
				case s.tunnelChannel <- tc:
				default:
					tc.conn.Close()
				}
				skipped++
				if skipped > cap(s.tunnelChannel) {
					return nil, errNoTunnel
				}
				time.Sleep(5 * time.Millisecond)
				continue
			}
			close(tc.ping)
			_ = tc.conn.NetConn().SetDeadline(deadline)
			if !tc.acquire(ctx, deadline) {
				if tc.isRetired() {
					continue
				}
				_ = tc.conn.NetConn().SetDeadline(time.Time{})
				tc.conn.Close()
				continue
			}
			_ = tc.conn.NetConn().SetDeadline(time.Time{})
			return &tc, nil
		}
	}
}
