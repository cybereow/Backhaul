package transport

import (
	"fmt"
	"testing"
)

func TestRecordEventRingBuffer(t *testing.T) {
	s := &WsMuxTransport{}

	// Fewer than the cap: all kept, in order.
	s.recordEvent("control_lost", "a")
	s.recordEvent("restart", "b")
	got := s.snapshotEvents()
	if len(got) != 2 {
		t.Fatalf("want 2 events, got %d", len(got))
	}
	if got[0].Kind != "control_lost" || got[1].Kind != "restart" {
		t.Errorf("events out of order: %+v", got)
	}
	if got[0].Time.IsZero() {
		t.Error("event timestamp not set")
	}

	// Overflow the ring: only the newest maxRecordedEvents survive, newest last.
	for i := 0; i < maxRecordedEvents+50; i++ {
		s.recordEvent("restart", fmt.Sprintf("n%d", i))
	}
	got = s.snapshotEvents()
	if len(got) != maxRecordedEvents {
		t.Fatalf("ring should cap at %d, got %d", maxRecordedEvents, len(got))
	}
	// The very last recorded detail must be present as the newest entry.
	last := got[len(got)-1]
	wantLast := fmt.Sprintf("n%d", maxRecordedEvents+50-1)
	if last.Detail != wantLast {
		t.Errorf("newest event detail = %q, want %q", last.Detail, wantLast)
	}
}

func TestSnapshotEventsIsCopy(t *testing.T) {
	s := &WsMuxTransport{}
	s.recordEvent("control_replaced", "x")
	snap := s.snapshotEvents()
	snap[0].Detail = "mutated"
	if again := s.snapshotEvents(); again[0].Detail != "x" {
		t.Errorf("snapshot should be a copy; internal state was mutated to %q", again[0].Detail)
	}
}
