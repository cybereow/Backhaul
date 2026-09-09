package utils

import (
	"net"
	"testing"
	"time"
)

func TestSpeedtestHeaderRoundTrip(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()

	go func() {
		_ = SendFlowSpeedtest(a, SpeedtestUpload, 7)
	}()

	// The kind byte is consumed by ReadFlowKind on the real path; consume it here.
	kind, err := ReadFlowKind(b)
	if err != nil {
		t.Fatalf("read flow kind: %v", err)
	}
	if kind != FlowSpeedtest {
		t.Fatalf("kind = %#x, want FlowSpeedtest", kind)
	}
	mode, seconds, err := ReceiveFlowSpeedtest(b)
	if err != nil {
		t.Fatalf("receive header: %v", err)
	}
	if mode != SpeedtestUpload || seconds != 7 {
		t.Fatalf("got mode=%d seconds=%d, want mode=%d seconds=7", mode, seconds, SpeedtestUpload)
	}
}

// TestSpeedtestSourceSinkAndReport drives the download shape end to end over an
// in-memory pipe: one side sources for a short window, the other sinks and
// measures, then reports its numbers back to the source side unchanged.
func TestSpeedtestSourceSinkAndReport(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()

	const window = 200 * time.Millisecond

	type report struct {
		bytes   int64
		elapsed time.Duration
		err     error
	}
	sinkDone := make(chan report, 1)
	go func() {
		bytes, el, err := SpeedtestSink(b)
		if err == nil {
			err = WriteSpeedtestReport(b, bytes, el)
		}
		sinkDone <- report{bytes, el, err}
	}()

	if err := SpeedtestSource(a, window); err != nil {
		t.Fatalf("source: %v", err)
	}
	gotBytes, gotElapsed, err := ReadSpeedtestReport(a)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}

	sink := <-sinkDone
	if sink.err != nil {
		t.Fatalf("sink: %v", sink.err)
	}
	if sink.bytes == 0 {
		t.Fatal("sink received zero bytes")
	}
	if gotBytes != sink.bytes {
		t.Errorf("reported bytes %d != sink-measured %d", gotBytes, sink.bytes)
	}
	if gotElapsed != sink.elapsed {
		t.Errorf("reported elapsed %v != sink-measured %v", gotElapsed, sink.elapsed)
	}
	if mbps := SpeedtestMbps(gotBytes, gotElapsed); mbps <= 0 {
		t.Errorf("computed throughput should be positive, got %v Mbps", mbps)
	}
}

func TestSpeedtestMbps(t *testing.T) {
	// 1,250,000 bytes in 1s = 10 Mbps.
	if got := SpeedtestMbps(1_250_000, time.Second); got < 9.99 || got > 10.01 {
		t.Errorf("SpeedtestMbps = %v, want ~10", got)
	}
	if got := SpeedtestMbps(100, 0); got != 0 {
		t.Errorf("zero duration should yield 0, got %v", got)
	}
}
