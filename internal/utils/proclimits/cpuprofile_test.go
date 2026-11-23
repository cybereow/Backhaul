package proclimits

import (
	"strings"
	"testing"
	"time"
)

func burn(stop <-chan struct{}) {
	x := 0
	for {
		select {
		case <-stop:
			return
		default:
			for i := 0; i < 1e5; i++ {
				x += i * i
			}
		}
	}
}

func TestTopCPUNamesWhatBurnsTheCPU(t *testing.T) {
	stop := make(chan struct{})
	defer close(stop)
	go burn(stop)
	line, err := TopCPU(500*time.Millisecond, 3)
	if err != nil {
		t.Skipf("no CPU profile here: %v", err)
	}
	if !strings.Contains(line, "burn") {
		t.Fatalf("the busy function is not in the summary: %s", line)
	}
	if !strings.HasPrefix(line, "cpu profile") {
		t.Fatalf("unexpected summary: %s", line)
	}
}
