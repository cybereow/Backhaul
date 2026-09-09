package transport

import (
	"sort"
	"time"
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

// poolSnapshot reports the live pool grouped by CDN: how many sessions each CDN
// has and how many streams (flows) are currently running on them. Run during a
// transfer, it shows whether flows spread across the CDNs (good aggregation) or
// concentrate on one (the reason a many-flow workload wouldn't aggregate).
func (s *WsMuxTransport) poolSnapshot() map[string]interface{} {
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
	}
}
