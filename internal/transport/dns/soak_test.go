package dnsx

import (
	"testing"
	"time"
)

func stat(evs ...event) *streamStat {
	st := &streamStat{}
	st.events = evs
	return st
}

func TestSummarizeStalls(t *testing.T) {
	now := time.Now()
	at := func(secAgo float64) time.Time { return now.Add(-time.Duration(secAgo * float64(time.Second))) }

	// run lasted 30s; nothing worked for the first 20s, then one success, then silence
	r := summarize(SoakStream{}, stat(event{at: at(10), ok: true}), 30)
	if r.LongestStallMs < 19_000 {
		t.Errorf("initial outage not counted: LongestStallMs=%d, want >= ~20000", r.LongestStallMs)
	}

	// never succeeded: the whole run is one stall
	r = summarize(SoakStream{}, stat(event{at: at(5)}), 30)
	if r.LongestStallMs < 29_000 {
		t.Errorf("all-failure run: LongestStallMs=%d, want ~30000", r.LongestStallMs)
	}

	// terminal outage: last success 15s ago
	r = summarize(SoakStream{}, stat(event{at: at(29), ok: true}, event{at: at(15), ok: true}), 30)
	if r.LongestStallMs < 14_000 {
		t.Errorf("terminal outage not counted: LongestStallMs=%d, want ~15000", r.LongestStallMs)
	}
}
