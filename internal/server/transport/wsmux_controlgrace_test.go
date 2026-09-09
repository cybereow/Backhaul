package transport

import (
	"io"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
)

// TestControlGraceHoldsWhilePoolAlive verifies the core fix: when the control
// channel has not reattached but the pool still carries live sessions, the grace
// handler holds (re-arms) instead of restarting the whole transport - which
// would drop every in-flight flow just because a CDN was slow to reconnect the
// side channel.
func TestControlGraceHoldsWhilePoolAlive(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	s := &WsMuxTransport{logger: logger}

	atomic.StoreInt32(&s.sessionCounter, 2) // pool still carrying flows
	// controlChannel is nil (lost) and not reattached.

	s.onControlGraceExpired()

	// It must have recorded a hold (not a restart) and re-armed the timer.
	ev := s.snapshotEvents()
	if len(ev) == 0 {
		t.Fatal("expected a recorded event")
	}
	last := ev[len(ev)-1]
	if last.Kind != "control_hold" {
		t.Fatalf("expected a control_hold event, got kind %q (%+v)", last.Kind, ev)
	}
	for _, e := range ev {
		if e.Kind == "restart" {
			t.Errorf("must not restart while the pool is alive, but recorded: %+v", e)
		}
	}

	s.controlMu.Lock()
	armed := s.graceTimer != nil
	if s.graceTimer != nil {
		s.graceTimer.Stop() // don't leave a 30s timer running in the test process
	}
	s.controlMu.Unlock()
	if !armed {
		t.Error("grace timer should be re-armed while holding")
	}
}
