package handlers

import (
	"bytes"
	"io"
	"sync"
	"testing"
	"time"
)

// ackSink collects the offsets a handler receives.
type ackSink struct {
	mu   sync.Mutex
	got  []uint64
	wake chan struct{}
}

func newAckSink() *ackSink { return &ackSink{wake: make(chan struct{}, 64)} }

func (a *ackSink) fn(off uint64) {
	a.mu.Lock()
	a.got = append(a.got, off)
	a.mu.Unlock()
	select {
	case a.wake <- struct{}{}:
	default:
	}
}

func (a *ackSink) waitFor(t *testing.T, n int) []uint64 {
	t.Helper()
	deadline := time.After(hcTestTimeout)
	for {
		a.mu.Lock()
		if len(a.got) >= n {
			out := append([]uint64(nil), a.got...)
			a.mu.Unlock()
			return out
		}
		a.mu.Unlock()
		select {
		case <-a.wake:
		case <-deadline:
			t.Fatalf("expected %d acks, have fewer", n)
		}
	}
}

type acker interface {
	SendAck(uint64) error
	SetAckHandler(func(uint64))
	ServeAcks()
}

// ACK records interleave with data without disturbing it, and arrive in order.
func TestHalfCloseAckInterleavedWithData(t *testing.T) {
	sa, sb := streamPair(t)
	a, b := NewHalfCloseConn(sa), NewHalfCloseConn(sb)
	sink := newAckSink()
	b.(acker).SetAckHandler(sink.fn)

	payload := genPayload(200000)
	done := make(chan []byte, 1)
	go func() {
		got, _ := io.ReadAll(b) // b is the reader: it parses the ACKs a sends
		done <- got
	}()

	for i := 0; i < 10; i++ {
		if _, err := a.Write(payload[i*20000 : (i+1)*20000]); err != nil {
			t.Fatal(err)
		}
		if err := a.(acker).SendAck(uint64(1000 * (i + 1))); err != nil {
			t.Fatal(err)
		}
	}
	a.(interface{ CloseWrite() error }).CloseWrite()

	if got := <-done; !bytes.Equal(got, payload) {
		t.Fatalf("data corrupted by interleaved ACKs: %d bytes", len(got))
	}
	offs := sink.waitFor(t, 10)
	for i, o := range offs[:10] {
		if o != uint64(1000*(i+1)) {
			t.Fatalf("ack %d = %d, want %d", i, o, 1000*(i+1))
		}
	}
}

// After the peer's END its data direction is over but it still acks ours: the
// stream must keep being parsed for those, with no reader calling Read.
func TestHalfCloseAckAfterEnd(t *testing.T) {
	sa, sb := streamPair(t)
	a, b := NewHalfCloseConn(sa), NewHalfCloseConn(sb)
	sink := newAckSink()
	a.(acker).SetAckHandler(sink.fn)

	// b ends its direction; a reads to EOF.
	b.(interface{ CloseWrite() error }).CloseWrite()
	if _, err := a.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("read after the peer's END: %v, want io.EOF", err)
	}
	// Nobody Reads a any more, yet b's later acks must reach the handler.
	for i := uint64(1); i <= 3; i++ {
		if err := b.(acker).SendAck(i * 777); err != nil {
			t.Fatalf("SendAck after our own END: %v", err)
		}
	}
	offs := sink.waitFor(t, 3)
	if offs[0] != 777 || offs[2] != 3*777 {
		t.Fatalf("acks after END: %v", offs)
	}
}

// A stream whose receive side is not read by anyone (the download already
// ended) is served explicitly.
func TestHalfCloseServeAcks(t *testing.T) {
	sa, sb := streamPair(t)
	a, b := NewHalfCloseConn(sa), NewHalfCloseConn(sb)
	sink := newAckSink()
	a.(acker).SetAckHandler(sink.fn)
	a.(acker).ServeAcks()

	b.(acker).SendAck(42)
	b.(acker).SendAck(43)
	offs := sink.waitFor(t, 2)
	if offs[0] != 42 || offs[1] != 43 {
		t.Fatalf("acks: %v", offs)
	}
}

// Without a handler an ACK is still a protocol error, as before this record
// existed.
func TestHalfCloseAckWithoutHandlerIsAnError(t *testing.T) {
	sa, sb := streamPair(t)
	a, b := NewHalfCloseConn(sa), NewHalfCloseConn(sb)
	b.(acker).SendAck(1)
	if _, err := a.Read(make([]byte, 8)); err == nil || err == io.EOF {
		t.Fatalf("read over an unexpected ACK: %v, want a protocol error", err)
	}
}
