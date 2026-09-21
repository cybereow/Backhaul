package striping

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFECRoundTrip(t *testing.T) {
	cases := []struct {
		data, parity int
	}{
		{2, 1},
		{4, 2},
		{6, 1},
	}
	for _, tc := range cases {
		tc := tc
		t.Run("", func(t *testing.T) {
			legs, peers := pipePair(tc.data + tc.parity)
			client, err := NewFEC(legs, 991, tc.data, tc.parity) // deliberately not a round chunk size
			if err != nil {
				t.Fatalf("NewFEC client: %v", err)
			}
			server, err := NewFEC(peers, 991, tc.data, tc.parity)
			if err != nil {
				t.Fatalf("NewFEC server: %v", err)
			}
			defer client.Close()
			defer server.Close()

			payload := make([]byte, 3*1024*1024+53) // not a multiple of row size
			if _, err := rand.Read(payload); err != nil {
				t.Fatalf("rand.Read: %v", err)
			}

			var wg sync.WaitGroup
			wg.Add(1)
			var writeErr error
			go func() {
				defer wg.Done()
				_, writeErr = client.Write(payload)
				writeErr2 := client.Close()
				if writeErr == nil {
					writeErr = writeErr2
				}
			}()

			got, err := io.ReadAll(io.LimitReader(server, int64(len(payload))))
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			wg.Wait()

			if !bytes.Equal(got, payload) {
				t.Fatalf("data=%d parity=%d: reassembled payload mismatch (got %d bytes, want %d)", tc.data, tc.parity, len(got), len(payload))
			}
		})
	}
}

// TestFECTeleratesLegFailure kills exactly parityShards legs before the
// transfer starts (simulating them dying, e.g. a CDN resetting the
// underlying WebSocket connection) and checks the full payload still
// arrives intact - the whole point of the parity shards.
func TestFECToleratesLegFailure(t *testing.T) {
	const dataShards, parityShards = 4, 2
	legs, peers := pipePair(dataShards + parityShards)
	client, err := NewFEC(legs, 4096, dataShards, parityShards)
	if err != nil {
		t.Fatalf("NewFEC client: %v", err)
	}
	server, err := NewFEC(peers, 4096, dataShards, parityShards)
	if err != nil {
		t.Fatalf("NewFEC server: %v", err)
	}
	defer client.Close()
	defer server.Close()

	// Kill 2 legs (== parityShards) up front: one data leg, one parity leg.
	legs[1].Close()
	legs[dataShards].Close()

	payload := make([]byte, 2*1024*1024+11)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	var writeErr error
	go func() {
		defer wg.Done()
		_, writeErr = client.Write(payload)
		writeErr2 := client.Close()
		if writeErr == nil {
			writeErr = writeErr2
		}
	}()

	got, err := io.ReadAll(io.LimitReader(server, int64(len(payload))))
	if err != nil {
		t.Fatalf("read after tolerated leg failure: %v", err)
	}
	wg.Wait()
	if writeErr != nil {
		t.Fatalf("write: %v", writeErr)
	}

	if !bytes.Equal(got, payload) {
		t.Fatalf("reassembled payload mismatch after tolerated leg failure (got %d bytes, want %d)", len(got), len(payload))
	}
}

// TestFECAbortWriteIsLoud mirrors TestStripedAbortWriteIsLoud for the FEC
// path: a truncated upstream write must never be reported to the peer as a
// clean end of stream.
func TestFECAbortWriteIsLoud(t *testing.T) {
	const dataShards, parityShards = 3, 1
	for iter := 0; iter < 20; iter++ {
		legs, peers := pipePair(dataShards + parityShards)
		sender, err := NewFEC(legs, 4096, dataShards, parityShards)
		if err != nil {
			t.Fatalf("NewFEC sender: %v", err)
		}
		receiver, err := NewFEC(peers, 4096, dataShards, parityShards)
		if err != nil {
			t.Fatalf("NewFEC receiver: %v", err)
		}

		payload := make([]byte, 40*dataShards*4096+17)
		if _, err := rand.Read(payload); err != nil {
			t.Fatalf("rand: %v", err)
		}

		go func() {
			sender.Write(payload)
			sender.AbortWrite()
			sender.Close()
		}()

		_, err = io.ReadAll(receiver)
		if err == nil {
			t.Fatalf("iter %d: truncated FEC stream reported a clean EOF - silent data loss", iter)
		}
		receiver.Close()
	}
}

// TestFECFailsBeyondTolerance kills more legs than parityShards can cover
// and expects a loud error - never a silent hang or, worse, corrupted data.
func TestFECFailsBeyondTolerance(t *testing.T) {
	const dataShards, parityShards = 4, 2
	legs, peers := pipePair(dataShards + parityShards)
	client, err := NewFEC(legs, 4096, dataShards, parityShards)
	if err != nil {
		t.Fatalf("NewFEC client: %v", err)
	}
	server, err := NewFEC(peers, 4096, dataShards, parityShards)
	if err != nil {
		t.Fatalf("NewFEC server: %v", err)
	}
	defer client.Close()
	defer server.Close()

	// Kill 3 legs (> parityShards=2): the stream can no longer be guaranteed.
	legs[0].Close()
	legs[1].Close()
	legs[2].Close()

	payload := make([]byte, 512*1024)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		client.Write(payload)
		client.Close()
	}()

	_, readErr := io.ReadAll(server)
	<-done

	if readErr == nil {
		t.Fatalf("expected an error reading past the point where alive legs dropped below dataShards, got nil")
	}
}

// TestFECPartialWriteProgress verifies that a short Write (less than one full
// row) is delivered to the peer without requiring CloseWrite/Close. This is the
// liveness property needed for request/response protocols: the request must
// reach the peer before the requester blocks waiting for a reply.
func TestFECPartialWriteProgress(t *testing.T) {
	// deadline guards against the pre-fix hang; well under the 20-second idle
	// timeout so this test isolates partial-write delivery, not stall teardown.
	t.Helper()

	cases := []struct {
		data, parity int
	}{
		{2, 1},
		{4, 2},
	}
	for _, tc := range cases {
		tc := tc
		t.Run("", func(t *testing.T) {
			legs, peers := pipePair(tc.data + tc.parity)
			client, err := NewFEC(legs, 1024, tc.data, tc.parity)
			if err != nil {
				t.Fatalf("NewFEC client: %v", err)
			}
			server, err := NewFEC(peers, 1024, tc.data, tc.parity)
			if err != nil {
				t.Fatalf("NewFEC server: %v", err)
			}
			defer client.Close()
			defer server.Close()

			rowSize := tc.data * 1024 // dataShards * chunkSize
			// payload is smaller than one full row
			payload := make([]byte, rowSize/2+7)
			if _, err := rand.Read(payload); err != nil {
				t.Fatalf("rand.Read: %v", err)
			}

			// Sub-test 1: partial write reaches peer without close.
			received := make(chan []byte, 1)
			go func() {
				buf := make([]byte, len(payload))
				_, err := io.ReadFull(server, buf)
				if err != nil {
					received <- nil
					return
				}
				received <- buf
			}()

			if _, err := client.Write(payload); err != nil {
				t.Fatalf("Write: %v", err)
			}
			// Expect delivery within 5 s — far below the 20-second idle timeout.
			select {
			case got := <-received:
				if got == nil {
					t.Fatal("server ReadFull failed before client closed")
				}
				if !bytes.Equal(got, payload) {
					t.Fatal("partial-write payload mismatch (zero-padding leaked or bytes reordered)")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("peer did not receive partial write within 5s (pre-fix: would hang until CloseWrite)")
			}

			// Sub-test 2: request/response — reply arrives before either side closes.
			// Send a request; server echoes it back; client reads the reply without
			// either side having called Close.
			reqPayload := []byte("ping-partial-row")
			replyCh := make(chan []byte, 1)
			go func() {
				buf := make([]byte, len(reqPayload))
				if _, err := io.ReadFull(server, buf); err != nil {
					replyCh <- nil
					return
				}
				// Echo back — also a partial write from server side.
				server.Write(buf)
				replyCh <- buf
			}()

			if _, err := client.Write(reqPayload); err != nil {
				t.Fatalf("Write request: %v", err)
			}
			// Read the echo.
			echoBuf := make([]byte, len(reqPayload))
			readDone := make(chan error, 1)
			go func() {
				_, err := io.ReadFull(client, echoBuf)
				readDone <- err
			}()
			select {
			case err := <-readDone:
				if err != nil {
					t.Fatalf("ReadFull echo: %v", err)
				}
				if !bytes.Equal(echoBuf, reqPayload) {
					t.Fatalf("echo mismatch: got %q, want %q", echoBuf, reqPayload)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("request/response round-trip did not complete within 5s without Close")
			}
			<-replyCh

			// Sub-test 3: sequence of short writes preserves order and no spurious
			// zero bytes between messages.
			const nMsgs = 8
			var msgs [nMsgs][]byte
			for i := range msgs {
				msgs[i] = make([]byte, 13+i*7)
				if _, err := rand.Read(msgs[i]); err != nil {
					t.Fatalf("rand.Read msg %d: %v", i, err)
				}
			}
			var collected []byte
			seqDone := make(chan error, 1)
			total := 0
			for _, m := range msgs {
				total += len(m)
			}
			go func() {
				buf := make([]byte, total)
				_, err := io.ReadFull(server, buf)
				if err != nil {
					seqDone <- err
					return
				}
				collected = buf
				seqDone <- nil
			}()
			for i, m := range msgs {
				if _, err := client.Write(m); err != nil {
					t.Fatalf("Write msg %d: %v", i, err)
				}
			}
			select {
			case err := <-seqDone:
				if err != nil {
					t.Fatalf("sequential short writes: ReadFull error: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("sequential short writes did not arrive within 5s")
			}
			var want []byte
			for _, m := range msgs {
				want = append(want, m...)
			}
			if !bytes.Equal(collected, want) {
				t.Fatal("sequential short writes: byte order or zero-padding leakage detected")
			}
		})
	}
}

// BenchmarkFECWriteSizes compares throughput for full-row, fragmented-row, and
// tiny-write patterns. No CI speed threshold is asserted; results document the
// padding overhead trade-off introduced by the per-Write flush in Write.
func BenchmarkFECWriteSizes(b *testing.B) {
	const chunkSize = 4096
	const dataShards, parityShards = 4, 2
	rowSize := dataShards * chunkSize

	sizes := []struct {
		name string
		size int
	}{
		{"full-row", rowSize},
		{"half-row", rowSize / 2},
		{"tiny-64B", 64},
	}

	for _, sz := range sizes {
		sz := sz
		b.Run(sz.name, func(b *testing.B) {
			legs, peers := pipePair(dataShards + parityShards)
			client, err := NewFEC(legs, chunkSize, dataShards, parityShards)
			if err != nil {
				b.Fatalf("NewFEC client: %v", err)
			}
			server, err := NewFEC(peers, chunkSize, dataShards, parityShards)
			if err != nil {
				b.Fatalf("NewFEC server: %v", err)
			}
			defer client.Close()

			payload := make([]byte, sz.size)
			rand.Read(payload)

			totalBytes := int64(0)
			// Drain server in background so pipes don't block.
			go func() { io.Copy(io.Discard, server) }()

			b.SetBytes(int64(sz.size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				n, err := client.Write(payload)
				if err != nil {
					b.Fatalf("Write: %v", err)
				}
				totalBytes += int64(n)
			}
			b.ReportMetric(float64(totalBytes), "input-bytes")
		})
	}
}

// ---- Plan 004: FEC idle vs proven gaps, and the reassembly budget ----

const (
	testFECChunk  = 8
	testFECData   = 2
	testFECParity = 1
	testFECRow    = testFECChunk * testFECData
)

// fecShardFrame is one FEC wire frame: rowSeq(4) shardIndex(1) rowLen(4) then
// exactly testFECChunk payload bytes.
func fecShardFrame(rowSeq uint32, shard byte, rowLen uint32, payload []byte) []byte {
	b := make([]byte, fecHeaderSize, fecHeaderSize+testFECChunk)
	binary.BigEndian.PutUint32(b[0:4], rowSeq)
	b[4] = shard
	binary.BigEndian.PutUint32(b[5:9], rowLen)
	p := make([]byte, testFECChunk)
	copy(p, payload)
	return append(b, p...)
}

// fecRowFrames returns the two data shards of one complete row (no parity
// needed to decode). data must fit in one row.
func fecRowFrames(rowSeq uint32, data string) []byte {
	padded := make([]byte, testFECRow)
	copy(padded, data)
	out := fecShardFrame(rowSeq, 0, uint32(len(data)), padded[:testFECChunk])
	return append(out, fecShardFrame(rowSeq, 1, uint32(len(data)), padded[testFECChunk:])...)
}

// fecEnd is the END marker announcing total rows.
func fecEnd(total uint32) []byte {
	b := make([]byte, fecHeaderSize)
	binary.BigEndian.PutUint32(b[0:4], fecEndMarkerSeq)
	binary.BigEndian.PutUint32(b[5:9], total)
	return b
}

func newFECServer(t *testing.T, stall time.Duration) (*FECConn, []net.Conn) {
	t.Helper()
	legs, peers := pipePair(testFECData + testFECParity)
	s, err := NewFEC(legs, testFECChunk, testFECData, testFECParity)
	if err != nil {
		t.Fatalf("NewFEC: %v", err)
	}
	if stall > 0 {
		s.stallTimeout = stall // before any I/O
	}
	drainPeers(peers)
	t.Cleanup(func() {
		s.Close()
		for _, p := range peers {
			p.Close()
		}
	})
	return s, peers
}

// TestFECTruncatedShardIsLoud: legs that close right after a shard header (before
// any payload byte) must fail Read with a non-EOF error, with or without the END
// marker having been seen.
func TestFECTruncatedShardIsLoud(t *testing.T) {
	for _, withEnd := range []bool{false, true} {
		name := "noEnd"
		if withEnd {
			name = "afterEnd"
		}
		t.Run(name, func(t *testing.T) {
			s, peers := newFECServer(t, 2*time.Second)
			ev := readEvents(s)
			for _, p := range peers {
				go func(p net.Conn) {
					if withEnd {
						p.Write(fecEnd(1))
					}
					p.Write(fecShardFrame(0, 0, testFECRow, nil)[:fecHeaderSize])
					p.Close()
				}(p)
			}
			expectErr(t, ev, "", 3*time.Second)
		})
	}
}

// TestFECIdle mirrors TestStripedIdle: nothing missing means idle, never a
// timeout.
func TestFECIdle(t *testing.T) {
	const stall = 100 * time.Millisecond

	t.Run("initial", func(t *testing.T) {
		s, peers := newFECServer(t, stall)
		ev := readEvents(s)
		expectQuiet(t, ev, 5*stall, "healthy initial idle")
		peers[0].Write(fecRowFrames(0, "hello"))
		expectData(t, ev, "hello")
	})

	t.Run("afterDeliveredData", func(t *testing.T) {
		s, peers := newFECServer(t, stall)
		ev := readEvents(s)
		peers[0].Write(fecRowFrames(0, "hello"))
		expectData(t, ev, "hello")
		expectQuiet(t, ev, 5*stall, "idle after delivered data")
		peers[1].Write(fecRowFrames(1, "world"))
		expectData(t, ev, "world")
	})

	t.Run("silentReverseDirection", func(t *testing.T) {
		n := testFECData + testFECParity
		legs, peers := pipePair(n)
		a, err := NewFEC(legs, testFECChunk, testFECData, testFECParity)
		if err != nil {
			t.Fatal(err)
		}
		b, err := NewFEC(peers, testFECChunk, testFECData, testFECParity)
		if err != nil {
			t.Fatal(err)
		}
		a.stallTimeout, b.stallTimeout = stall, stall
		defer a.Close()
		defer b.Close()
		aRead := readEvents(a) // reverse direction: b writes nothing for a while
		bRead := readEvents(b)

		for i := 0; i < 8; i++ { // forward traffic outlasts several stall timeouts
			if _, err := a.Write([]byte("ping")); err != nil {
				t.Fatalf("write: %v", err)
			}
			time.Sleep(stall / 2)
		}
		got := ""
		for len(got) < 32 {
			select {
			case e := <-bRead:
				if e.err != nil {
					t.Fatalf("forward read: %v", e.err)
				}
				got += e.data
			case <-time.After(3 * time.Second):
				t.Fatalf("forward data incomplete: %q", got)
			}
		}
		expectQuiet(t, aRead, 10*time.Millisecond, "silent reverse direction during forward traffic")
		b.Write([]byte("pong"))
		expectData(t, aRead, "pong")
	})

	t.Run("zeroLengthRead", func(t *testing.T) {
		s, _ := newFECServer(t, stall)
		done := make(chan struct{})
		go func() {
			if n, err := s.Read(nil); n != 0 || err != nil {
				t.Errorf("Read(nil) = %d, %v", n, err)
			}
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("zero-length Read blocked")
		}
	})

	t.Run("closeUnblocksIdleRead", func(t *testing.T) {
		s, _ := newFECServer(t, stall)
		ev := readEvents(s)
		expectQuiet(t, ev, 2*stall, "idle")
		s.Close()
		expectErr(t, ev, "", 3*time.Second)
	})
}

// TestFECGapDeadline mirrors TestStripedGapDeadline for rows.
func TestFECGapDeadline(t *testing.T) {
	t.Run("endBeforeMissingData", func(t *testing.T) {
		const stall = 200 * time.Millisecond
		s, peers := newFECServer(t, stall)
		ev := readEvents(s)
		peers[0].Write(fecRowFrames(0, "aa"))
		expectData(t, ev, "aa")
		start := time.Now()
		peers[0].Write(fecEnd(2)) // row 1 announced, never sent
		at := expectErr(t, ev, "stalled", 3*time.Second)
		if d := at.Sub(start); d < stall-10*time.Millisecond {
			t.Fatalf("gap failed after %s, before its %s deadline", d, stall)
		}
	})

	t.Run("gapClockStartsAtEvidenceNotIdle", func(t *testing.T) {
		const stall = 100 * time.Millisecond
		s, peers := newFECServer(t, stall)
		ev := readEvents(s)
		expectQuiet(t, ev, 4*stall, "idle before END")
		start := time.Now()
		peers[0].Write(fecEnd(1))
		at := expectErr(t, ev, "stalled", 3*time.Second)
		if d := at.Sub(start); d < stall-10*time.Millisecond {
			t.Fatalf("idle time counted toward the gap: failed %s after END, want >= %s", d, stall)
		}
	})

	t.Run("laterRowsDoNotRenew", func(t *testing.T) {
		const stall = 300 * time.Millisecond
		s, peers := newFECServer(t, stall)
		ev := readEvents(s)
		start := time.Now()
		peers[0].Write(fecRowFrames(1, "x")) // row 0 is now provably missing
		go func() {
			for seq := uint32(2); seq < 60; seq++ { // sustained later rows
				time.Sleep(30 * time.Millisecond)
				if _, err := peers[1].Write(fecRowFrames(seq, "x")); err != nil {
					return
				}
			}
		}()
		at := expectErr(t, ev, "stalled", 3*time.Second)
		d := at.Sub(start)
		if d < stall-10*time.Millisecond || d > 2*stall {
			t.Fatalf("known gap failed after %s; want about %s, not postponed by later arrivals", d, stall)
		}
	})

	t.Run("followingGapNotGivenFreshTimeout", func(t *testing.T) {
		const stall = 400 * time.Millisecond
		s, peers := newFECServer(t, stall)
		ev := readEvents(s)
		start := time.Now()
		peers[0].Write(fecRowFrames(3, "d")) // rows 0, 1, 2 missing; gap first observed now
		time.Sleep(stall * 5 / 8)
		peers[0].Write(fecRowFrames(0, "a"))
		peers[0].Write(fecRowFrames(1, "b")) // gap at row 2 was already observable
		expectData(t, ev, "a")
		expectData(t, ev, "b")
		at := expectErr(t, ev, "stalled", 3*time.Second)
		d := at.Sub(start)
		if d < stall-10*time.Millisecond || d > stall+stall/4 {
			t.Fatalf("following gap failed after %s; want about %s from first observation, not a fresh %s", d, stall, stall)
		}
	})
}

// TestFECReassemblyBudget: incomplete shards and decoded out-of-order rows
// both count; overflow fails loudly and promptly; charges release exactly.
func TestFECReassemblyBudget(t *testing.T) {
	// Charges for chunk=8, data=2, parity=1: a row's first shard costs
	// 8 + rowMeta (2*64 + 24*3 = 200); a decoded row of 16 bytes costs 16+64.
	newServer := func(t *testing.T, limit int64) (*FECConn, []net.Conn) {
		s, peers := newFECServer(t, 0)
		s.budget.limit = limit // before any I/O
		return s, peers
	}

	t.Run("incompleteRowsOverflow", func(t *testing.T) {
		const limit = 1000
		s, peers := newServer(t, limit)
		ev := readEvents(s)
		go func() {
			for seq := uint32(0); seq < 100; seq++ { // one shard each: none completes
				if _, err := peers[0].Write(fecShardFrame(seq, 0, testFECRow, nil)); err != nil {
					return
				}
			}
		}()
		expectErr(t, ev, "reassembly budget", 2*time.Second)
		if peak := atomic.LoadInt64(&s.budget.peak); peak > limit {
			t.Fatalf("peak retained %d exceeded the %d budget", peak, limit)
		}
	})

	t.Run("decodedRowsBehindGapOverflow", func(t *testing.T) {
		const limit = 700
		s, peers := newServer(t, limit)
		ev := readEvents(s)
		go func() {
			for seq := uint32(1); seq < 100; seq++ { // row 0 never arrives
				if _, err := peers[0].Write(fecRowFrames(seq, "x")); err != nil {
					return
				}
			}
		}()
		expectErr(t, ev, "reassembly budget", 2*time.Second) // default stall is 20s: this is the budget
		if peak := atomic.LoadInt64(&s.budget.peak); peak > limit {
			t.Fatalf("peak retained %d exceeded the %d budget", peak, limit)
		}
	})

	t.Run("singleShardOverBudgetRejectedPromptly", func(t *testing.T) {
		s, peers := newServer(t, testFECChunk-1)
		ev := readEvents(s)
		go peers[0].Write(fecShardFrame(0, 0, testFECRow, nil))
		expectErr(t, ev, "reassembly budget", 2*time.Second)
	})

	t.Run("outOfOrderWithinBudgetReleasesExactly", func(t *testing.T) {
		const limit = 4000
		s, peers := newServer(t, limit)
		ev := readEvents(s)
		want := ""
		for _, seq := range []uint32{2, 1, 0} {
			peers[0].Write(fecRowFrames(seq, strings.Repeat(string(rune('a'+seq)), testFECRow)))
		}
		for seq := 0; seq < 3; seq++ {
			want += strings.Repeat(string(rune('a'+seq)), testFECRow)
		}
		got := ""
		for len(got) < len(want) {
			select {
			case e := <-ev:
				if e.err != nil {
					t.Fatalf("read: %v", e.err)
				}
				got += e.data
			case <-time.After(3 * time.Second):
				t.Fatalf("only %d/%d bytes", len(got), len(want))
			}
		}
		if got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
		if used := atomic.LoadInt64(&s.budget.used); used != 0 {
			t.Fatalf("retained bytes after full delivery = %d, want 0", used)
		}
		if peak := atomic.LoadInt64(&s.budget.peak); peak == 0 || peak > limit {
			t.Fatalf("peak retained = %d, want within (0, %d]", peak, limit)
		}
		s.rowsMu.Lock()
		defer s.rowsMu.Unlock()
		if len(s.rows) != 0 || len(s.done) != 0 || s.doneBase != 3 {
			t.Fatalf("row bookkeeping leaked: rows=%d done=%d doneBase=%d", len(s.rows), len(s.done), s.doneBase)
		}
	})
}

// TestFECDuplicateRows: shards straggling in after their row was decoded (in
// order or out of order), and duplicate shards, must neither redeliver the row
// nor leak budget or bookkeeping.
func TestFECDuplicateRows(t *testing.T) {
	r0 := strings.Repeat("A", testFECRow)
	r1 := strings.Repeat("B", testFECRow)

	run := func(t *testing.T, frames [][]byte) {
		s, peers := newFECServer(t, 0)
		ev := readEvents(s)
		go func() {
			for _, f := range frames { // one writer per leg keeps frame order
				if _, err := peers[0].Write(f); err != nil {
					return
				}
			}
		}()
		want := r0 + r1
		got := ""
		for len(got) < len(want) {
			select {
			case e := <-ev:
				if e.err != nil {
					t.Fatalf("read: %v", e.err)
				}
				got += e.data
			case <-time.After(3 * time.Second):
				t.Fatalf("only %d/%d bytes", len(got), len(want))
			}
		}
		if got != want {
			t.Fatalf("got %q, want %q (a decoded row was redelivered or lost)", got, want)
		}
		// Everything was sent on one leg in order, so the stragglers were already
		// processed before the last row completed.
		if used := atomic.LoadInt64(&s.budget.used); used != 0 {
			t.Fatalf("retained bytes = %d, want 0: a straggler or duplicate leaked its charge", used)
		}
		s.rowsMu.Lock()
		defer s.rowsMu.Unlock()
		if len(s.rows) != 0 || len(s.done) != 0 || s.doneBase != 2 {
			t.Fatalf("straggler recreated state: rows=%d done=%d doneBase=%d", len(s.rows), len(s.done), s.doneBase)
		}
	}

	parity := func(seq uint32) []byte { return fecShardFrame(seq, 2, testFECRow, nil) }
	dupData := func(seq uint32) []byte { return fecShardFrame(seq, 0, testFECRow, []byte(r0[:testFECChunk])) }

	t.Run("lateShardsAfterInOrderRow", func(t *testing.T) {
		run(t, [][]byte{fecRowFrames(0, r0), parity(0), dupData(0), fecRowFrames(1, r1)})
	})
	t.Run("lateShardsAfterOutOfOrderRow", func(t *testing.T) {
		run(t, [][]byte{fecRowFrames(1, r1), parity(1), dupData(1), fecRowFrames(0, r0)})
	})
	t.Run("duplicateShardOfIncompleteRow", func(t *testing.T) {
		half := fecShardFrame(0, 0, testFECRow, []byte(r0[:testFECChunk]))
		rest := fecShardFrame(0, 1, testFECRow, []byte(r0[testFECChunk:]))
		run(t, [][]byte{half, half, rest, fecRowFrames(1, r1)})
	})
}
