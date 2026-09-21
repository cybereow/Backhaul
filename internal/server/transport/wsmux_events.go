package transport

import (
	"sort"
	"sync/atomic"
	"time"

	"github.com/xtaci/smux"
)

// transportEvent is one recorded disruption: what happened, when, and any
// detail. Kept deliberately small - this is a diagnostic breadcrumb trail, not
// a metrics system.
type transportEvent struct {
	Time   time.Time `json:"time"`
	Kind   string    `json:"kind"`
	Detail string    `json:"detail,omitempty"`
}

// maxRecordedEvents caps the ring buffer; old events are dropped once full.
const maxRecordedEvents = 128

// recordEvent appends a disruption event, dropping the oldest once the ring is
// full. Safe to call from any goroutine.
func (s *WsMuxTransport) recordEvent(kind, detail string) {
	s.eventsMu.Lock()
	s.events = append(s.events, transportEvent{Time: time.Now(), Kind: kind, Detail: detail})
	if len(s.events) > maxRecordedEvents {
		s.events = s.events[len(s.events)-maxRecordedEvents:]
	}
	s.eventsMu.Unlock()
}

// snapshotEvents returns a copy of the recorded events, newest last.
func (s *WsMuxTransport) snapshotEvents() []transportEvent {
	s.eventsMu.Lock()
	defer s.eventsMu.Unlock()
	out := make([]transportEvent, len(s.events))
	copy(out, s.events)
	return out
}

// poolDiagnostics is the additive "diagnostics" object of /pool and /diag. It
// reports ownership state that already exists (plans 007/012/013/014/015); it
// keeps no counters or state of its own. Every value is a best-effort read of an
// independently locked subsystem taken one after the other, never nested, so the
// fields are NOT a transactionally consistent global view: expect momentary
// disagreement between them while the pool is changing, never a negative count.
// It does no network I/O, retains no reference to live slices/maps, and carries
// no token, header, secret path, payload or connection destination.
//
// Fields (name, type, source, meaning):
//
//	control_state              string  controlMu: controlChannel/handlersStarted/
//	                                   restartClaim + generation stop. "connected";
//	                                   "reconnecting" (control lost, grace running);
//	                                   "restarting" (restart claimed or generation
//	                                   stopping); "waiting" (no control channel yet).
//	control_grace_elapsed_ms   *int64  time.Since(graceStart), monotonic; only while
//	                                   reconnecting/restarting with a recorded loss.
//	sessions_owned             *int    open smux sessions the generation owns (queued,
//	                                   admitted and draining alike); omitted when the
//	                                   transport has no tracked generation.
//	sessions_eligible          int     registered pool sessions that are open: the
//	                                   ones a new flow may be placed on.
//	sessions_queued            int     len(tunnelChannel): upgraded, awaiting admission.
//	sessions_draining          *int    owned, open, no longer registered, and not
//	                                   queued: max(0, unregistered-open - queued).
//	                                   Derived, hence approximate; omitted like owned.
//	setups_active              int     setupsActive: setups a worker owns right now.
//	setups_queued              int     len(localChannel): accepted user connections
//	                                   still waiting for a setup permit.
//	setup_limit                int     cap(setupSlots): the setup-permit ceiling.
//	setups_saturated           bool    setups_active >= setup_limit.
//	pending_opens              int     sum of plain-leg OpenStream reservations
//	                                   (pooledSession.pendingOpens, plainSelectMu)
//	                                   over registered sessions.
//	replacement_waiting        *bool   the rotation decision gate is held: one aging
//	                                   session is waiting for its replacement;
//	                                   omitted when the generation is untracked.
//
// Not reported because no delivered state carries it: a generation ID (a
// wsGeneration has none and one is not invented here), the number of rotations
// queued behind the gate, and setup expired/rejected totals (no counter exists;
// per-connection events would flood the 128-entry ring).
type poolDiagnostics struct {
	ControlState          string `json:"control_state"`
	ControlGraceElapsedMS *int64 `json:"control_grace_elapsed_ms,omitempty"`
	SessionsOwned         *int   `json:"sessions_owned,omitempty"`
	SessionsEligible      int    `json:"sessions_eligible"`
	SessionsQueued        int    `json:"sessions_queued"`
	SessionsDraining      *int   `json:"sessions_draining,omitempty"`
	SetupsActive          int    `json:"setups_active"`
	SetupsQueued          int    `json:"setups_queued"`
	SetupLimit            int    `json:"setup_limit"`
	SetupsSaturated       bool   `json:"setups_saturated"`
	PendingOpens          int    `json:"pending_opens"`
	ReplacementWaiting    *bool  `json:"replacement_waiting,omitempty"`
}

// poolViews are the generation-scoped fields a snapshot reads. Restart republishes
// them without a lock, once every worker of the old generation has returned, so
// the tunnel listener (a worker of the generation, started after that publish)
// captures them once instead of the HTTP handler rereading them mid-restart.
type poolViews struct {
	g       *wsGeneration
	tunnelQ chan tunnelSession
	localQ  chan LocalTCPConn
	slots   chan struct{}
}

// poolViewsOf captures the current generation's views. Call it only from a
// worker of g, or while no Restart can run.
func (s *WsMuxTransport) poolViewsOf(g *wsGeneration) poolViews {
	return poolViews{g: g, tunnelQ: s.tunnelChannel, localQ: s.localChannel, slots: s.setupSlots}
}

// poolDiagnostics takes the snapshot described on poolDiagnostics. Each lock is
// held only to copy pointers or read a field; nothing here nests two of them, so
// it cannot add an edge to the lock order.
func (s *WsMuxTransport) poolDiagnostics(v poolViews) poolDiagnostics {
	var d poolDiagnostics
	stopped := v.g.isStopped()

	s.controlMu.Lock()
	switch {
	case s.controlChannel != nil:
		d.ControlState = "connected"
	case stopped || (s.restartClaim != nil && s.restartClaim == v.g):
		d.ControlState = "restarting"
	case s.handlersStarted:
		d.ControlState = "reconnecting"
	default:
		d.ControlState = "waiting"
	}
	if d.ControlState != "connected" && d.ControlState != "waiting" && !s.graceStart.IsZero() {
		ms := time.Since(s.graceStart).Milliseconds()
		d.ControlGraceElapsedMS = &ms
	}
	s.controlMu.Unlock()

	owned := v.g.ownedSessions() // nil for an untracked generation

	s.sessionsMu.Lock()
	registered := make([]*pooledSession, len(s.sessions))
	copy(registered, s.sessions)
	s.sessionsMu.Unlock()

	isRegistered := make(map[*smux.Session]struct{}, len(registered))
	for _, ps := range registered {
		isRegistered[ps.session] = struct{}{}
		if ps.session != nil && !ps.session.IsClosed() {
			d.SessionsEligible++
		}
	}

	d.SessionsQueued = len(v.tunnelQ)
	if v.g != nil {
		open, unregistered := 0, 0
		for _, sess := range owned {
			if sess.IsClosed() {
				continue
			}
			open++
			if _, ok := isRegistered[sess]; !ok {
				unregistered++
			}
		}
		draining := unregistered - d.SessionsQueued
		if draining < 0 {
			draining = 0
		}
		waiting := len(v.g.rotate) > 0
		d.SessionsOwned, d.SessionsDraining, d.ReplacementWaiting = &open, &draining, &waiting
	}

	d.SetupsActive = int(atomic.LoadInt32(&s.setupsActive))
	d.SetupsQueued = len(v.localQ)
	d.SetupLimit = cap(v.slots)
	d.SetupsSaturated = d.SetupLimit > 0 && d.SetupsActive >= d.SetupLimit

	s.plainSelectMu.Lock()
	for _, ps := range registered {
		d.PendingOpens += ps.pendingOpens
	}
	s.plainSelectMu.Unlock()
	return d
}

// poolSnapshot reports the live pool grouped by CDN: how many sessions each CDN
// has and how many streams (flows) are currently running on them. Run during a
// transfer, it shows whether flows spread across the CDNs (good aggregation) or
// concentrate on one (the reason a many-flow workload wouldn't aggregate). The
// legacy keys are unchanged; "diagnostics" is additive (see poolDiagnostics).
//
// The cdn value is the remote IP a pool connection arrived from, not a proven
// configured endpoint or provider: two providers behind one IP group together,
// one provider behind several IPs splits.
func (s *WsMuxTransport) poolSnapshot(v poolViews) map[string]interface{} {
	type cdnStat struct {
		CDN      string  `json:"cdn"`
		Sessions int     `json:"sessions"`
		Streams  int     `json:"streams"`
		RTTms    float64 `json:"rtt_ms,omitempty"`
	}

	s.sessionsMu.Lock()
	sessions := make([]*pooledSession, len(s.sessions))
	copy(sessions, s.sessions)
	s.sessionsMu.Unlock()

	byCDN := map[string]*cdnStat{}
	order := []string{}
	totalStreams := 0
	for _, ps := range sessions {
		st, ok := byCDN[ps.cdn]
		if !ok {
			st = &cdnStat{CDN: ps.cdn}
			byCDN[ps.cdn] = st
			order = append(order, ps.cdn)
		}
		st.Sessions++
		n := ps.session.NumStreams()
		st.Streams += n
		totalStreams += n
		if rtt := ps.rtt.Load(); rtt > 0 {
			st.RTTms = float64(rtt) / float64(time.Millisecond)
		}
	}

	// Sort CDNs by stream count, busiest first, so concentration is obvious.
	sort.SliceStable(order, func(i, j int) bool {
		return byCDN[order[i]].Streams > byCDN[order[j]].Streams
	})
	cdns := make([]*cdnStat, 0, len(order))
	for _, c := range order {
		cdns = append(cdns, byCDN[c])
	}

	return map[string]interface{}{
		"now":            time.Now(),
		"total_sessions": len(sessions),
		"distinct_cdns":  len(byCDN),
		"total_streams":  totalStreams,
		"per_cdn":        cdns,
		"diagnostics":    s.poolDiagnostics(v),
	}
}
