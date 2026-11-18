package handlers

import (
	"bytes"
	"context"
	"math/rand"
	"testing"
	"time"
)

// The ring is checked against a plain model: everything appended, with how much of
// its prefix has been acknowledged.
func TestReplayRingAgainstModel(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		rng := rand.New(rand.NewSource(seed))
		limit := 64<<10 + rng.Intn(256<<10)
		r := newReplayRing(limit)
		var model []byte
		var acked uint64

		for step := 0; step < 400; step++ {
			switch rng.Intn(3) {
			case 0, 1: // append, when it fits
				n := 1 + rng.Intn(20000)
				if r.free() < n {
					continue
				}
				p := make([]byte, n)
				rng.Read(p)
				r.append(p)
				model = append(model, p...)
			case 2: // ack a random prefix (sometimes stale or too far)
				var off uint64
				switch rng.Intn(5) {
				case 0:
					off = acked // stale
				case 1:
					off = uint64(len(model)) + uint64(1+rng.Intn(100)) // beyond what was sent
				default:
					off = acked + uint64(rng.Intn(len(model)-int(acked)+1))
				}
				r.ackTo(off)
				if off > acked && off <= uint64(len(model)) {
					acked = off
				}
			}
			if r.end() != uint64(len(model)) || r.len() != len(model)-int(acked) {
				t.Fatalf("seed %d step %d: end=%d len=%d, model end=%d retained=%d", seed, step, r.end(), r.len(), len(model), len(model)-int(acked))
			}
			// readAt over the retained span returns exactly the model's bytes.
			if r.len() > 0 {
				off := acked + uint64(rng.Intn(r.len()))
				got := make([]byte, 1+rng.Intn(30000))
				n := r.readAt(off, got)
				if !bytes.Equal(got[:n], model[off:int(off)+n]) {
					t.Fatalf("seed %d step %d: readAt(%d) differs", seed, step, off)
				}
				if want := min(len(got), len(model)-int(off)); n != want {
					t.Fatalf("seed %d step %d: readAt returned %d, want %d", seed, step, n, want)
				}
			}
			if r.readAt(acked, make([]byte, 8)) != 0 && r.len() == 0 {
				t.Fatalf("readAt on an empty ring returned data")
			}
		}
	}
}

func TestReplayRingBackpressure(t *testing.T) {
	r := newReplayRing(64 << 10)
	r.append(make([]byte, 64<<10))
	if r.free() != 0 {
		t.Fatalf("free = %d, want 0", r.free())
	}

	ctx := context.Background()
	got := make(chan error, 1)
	go func() { got <- r.waitFree(ctx, nil, nil, nil, 10000) }()
	select {
	case err := <-got:
		t.Fatalf("waitFree returned %v on a full ring", err)
	case <-time.After(100 * time.Millisecond):
	}
	r.ackTo(20000)
	select {
	case err := <-got:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(hcTestTimeout):
		t.Fatal("an ACK did not wake the waiting sender")
	}

	// A waiter is released by abort and by ctx.
	r.append(make([]byte, r.free()))
	abort := make(chan struct{})
	go func() { got <- r.waitFree(ctx, abort, nil, nil, 1) }()
	close(abort)
	if err := <-got; err == nil {
		t.Fatal("abort did not release the waiter")
	}
	cctx, cancel := context.WithCancel(ctx)
	go func() { got <- r.waitFree(cctx, nil, nil, nil, 1) }()
	cancel()
	if err := <-got; err == nil {
		t.Fatal("ctx did not release the waiter")
	}
	if err := r.waitFree(ctx, nil, nil, nil, 1<<20); err == nil {
		t.Fatal("a write larger than the limit must fail, not wait forever")
	}
	wake := make(chan struct{}, 1)
	go func() { got <- r.waitFree(ctx, nil, wake, nil, 1) }()
	wake <- struct{}{}
	if err := <-got; err != errReplayWake {
		t.Fatalf("a wake signal: %v, want errReplayWake", err)
	}
}

// A ring that has been fully acknowledged gives its memory back.
func TestReplayRingReleasesMemory(t *testing.T) {
	r := newReplayRing(4 << 20)
	r.append(make([]byte, 1<<20))
	if len(r.chunks) < (1<<20)/copyBufferSize {
		t.Fatalf("ring holds %d chunks for 1 MiB", len(r.chunks))
	}
	r.ackTo(1 << 20)
	if r.chunks != nil {
		t.Fatalf("an empty ring still holds %d chunks", len(r.chunks))
	}
	r.append([]byte("again")) // and works afterwards
	if r.len() != 5 || r.end() != 1<<20+5 {
		t.Fatalf("len=%d end=%d", r.len(), r.end())
	}
}

// Bytes handed over with appendOwned are kept as they are, in order with copied
// ones, read back across chunk boundaries, and released only when acknowledged.
func TestReplayRingKeepsOwnedBuffers(t *testing.T) {
	r := newReplayRing(8 << 20)
	var want []byte
	put := func(n int, own bool) {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(len(want) + i)
		}
		want = append(want, b...)
		if own {
			bp := copyBufferPool.Get().(*[]byte)
			copy(*bp, b)
			r.appendOwned(bp, n)
		} else {
			r.append(b)
		}
	}
	put(1000, false)
	put(copyBufferSize, true)
	put(70000, false) // spans two chunks
	put(copyBufferSize-5, true)
	put(3, false)
	if r.len() != len(want) || r.end() != uint64(len(want)) {
		t.Fatalf("len=%d end=%d want %d", r.len(), r.end(), len(want))
	}
	got := make([]byte, len(want))
	for off := 0; off < len(want); {
		n := r.readAt(uint64(off), got[off:min(len(got), off+9000)])
		if n == 0 {
			t.Fatalf("nothing at %d", off)
		}
		off += n
	}
	if !bytes.Equal(got, want) {
		t.Fatal("what was read back differs from what was stored")
	}
	// Acknowledge in odd steps, reading what is left each time.
	acked := 0
	for _, step := range []int{1, 999, 1, copyBufferSize, 33333, 40000} {
		acked += step
		r.ackTo(uint64(acked))
		rest := make([]byte, len(want)-acked)
		for off := 0; off < len(rest); {
			n := r.readAt(uint64(acked+off), rest[off:])
			if n == 0 {
				t.Fatalf("after ack %d: nothing at %d", acked, acked+off)
			}
			off += n
		}
		if !bytes.Equal(rest, want[acked:]) {
			t.Fatalf("after ack %d the rest differs", acked)
		}
	}
	r.ackTo(uint64(len(want)))
	if r.chunks != nil || r.len() != 0 {
		t.Fatal("not empty after everything was acknowledged")
	}
}
