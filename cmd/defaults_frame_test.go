package cmd

import (
	"testing"

	"github.com/musix/backhaul/internal/utils/handlers"
)

// A full half-close record must fit one smux frame by default, or every 32 KiB of
// an enveloped flow costs a second, 3-byte frame (and a second write).
func TestDefaultFrameFitsHalfCloseRecord(t *testing.T) {
	if defaultMaxFrameSize < handlers.HalfCloseRecordSize {
		t.Fatalf("default mux_framesize %d is smaller than a half-close record (%d)", defaultMaxFrameSize, handlers.HalfCloseRecordSize)
	}
}
