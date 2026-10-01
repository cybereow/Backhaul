package rel

import (
	"bytes"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"
)

// --- discrete-event simulator on a virtual clock (no real sleeping) ---------

type ev struct {
	at time.Time
	n  int
	fn func()
}

type sim struct {
	now                time.Time
	evs                []ev
	n                  int
	rng                *rand.Rand
	blackFrom, blackTo time.Time // total outage window (both directions)
}

func newSim(seed int64) *sim {
	return &sim{now: time.Unix(1_000_000, 0), rng: rand.New(rand.NewSource(seed))}
}

func (s *sim) after(d time.Duration, fn func()) {
	s.n++
	e := ev{s.now.Add(d), s.n, fn}
	i := sort.Search(len(s.evs), func(i int) bool {
		a := s.evs[i]
		return a.at.After(e.at) || (a.at.Equal(e.at) && a.n > e.n)
	})
	s.evs = append(s.evs, ev{})
	copy(s.evs[i+1:], s.evs[i:])
	s.evs[i] = e
}

func (s *sim) run(limit time.Duration, done func() bool) bool {
	end := s.now.Add(limit)
	for len(s.evs) > 0 && !done() {
		e := s.evs[0]
		s.evs = s.evs[1:]
		if e.at.After(end) {
			return false
		}
		s.now = e.at
		e.fn()
	}
	return done()
}

// link models one direction of the resolver path: drop, duplicate, and a
// base+random delay (the random part is what reorders packets).
type link struct {
	loss, dup float64
	base, jit time.Duration
}

func (s *sim) send(l link, fn func()) {
	if !s.blackFrom.IsZero() && !s.now.Before(s.blackFrom) && s.now.Before(s.blackTo) {
		return
	}
	if s.rng.Float64() < l.loss {
		return
	}
	n := 1
	if s.rng.Float64() < l.dup {
		n = 2
	}
	for ; n > 0; n-- {
		s.after(l.base+time.Duration(s.rng.Int63n(int64(l.jit)+1)), fn)
	}
}

// --- harness: outside polls, inside answers, both directions carry data -----

type result struct {
	gotUp, gotDown []byte
	peakSrvBuf     int
	ok             bool
	elapsed        time.Duration
}

// runXfer moves up (client->server) and down (server->client) through l. The
// client polls every poll interval, like the prober; the server can only answer
// an arriving poll. srvRead>0 makes the server app read only that many bytes per
// arrival (to exercise flow control); maxBuf checks the receive buffer bound.
func runXfer(t *testing.T, s *sim, cli, srv *Endpoint, up, down []byte, l link,
	poll time.Duration, srvRead int, srvRecvBuf int, limit time.Duration) result {
	t.Helper()
	var r result
	start := s.now
	upOff, downOff := 0, 0
	buf := make([]byte, 4096)

	var tick func()
	tick = func() {
		upOff += cli.Write(up[upOff:])
		for {
			n := cli.Read(buf)
			if n == 0 {
				break
			}
			r.gotDown = append(r.gotDown, buf[:n]...)
		}
		pk := cli.Next(s.now).Marshal()
		s.send(l, func() {
			p, err := Unmarshal(pk)
			if err != nil {
				t.Errorf("unmarshal: %v", err)
				return
			}
			srv.Recv(s.now, p)
			if len(srv.rcvBuf) > r.peakSrvBuf {
				r.peakSrvBuf = len(srv.rcvBuf)
			}
			if srvRecvBuf > 0 && len(srv.rcvBuf) > srvRecvBuf {
				t.Errorf("server rcvBuf %d exceeds RecvBuf %d", len(srv.rcvBuf), srvRecvBuf)
			}
			downOff += srv.Write(down[downOff:])
			rb := buf
			if srvRead > 0 {
				rb = buf[:srvRead]
			}
			for {
				n := srv.Read(rb)
				if n == 0 || srvRead > 0 {
					if n > 0 {
						r.gotUp = append(r.gotUp, rb[:n]...)
					}
					break
				}
				r.gotUp = append(r.gotUp, rb[:n]...)
			}
			resp := srv.Next(s.now).Marshal()
			s.send(l, func() {
				q, err := Unmarshal(resp)
				if err != nil {
					t.Errorf("unmarshal: %v", err)
					return
				}
				cli.Recv(s.now, q)
			})
		})
		s.after(poll, tick)
	}
	s.after(0, tick)

	done := func() bool { return len(r.gotUp) >= len(up) && len(r.gotDown) >= len(down) }
	r.ok = s.run(limit, done)
	r.elapsed = s.now.Sub(start)
	return r
}

func payload(seed int64, n int) []byte {
	b := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(b)
	return b
}

func pair(isn uint32, srvRecvBuf int) (*Endpoint, *Endpoint) {
	// Uplink capacity is tiny (data rides in the QNAME); downlink is fat (RDATA):
	// the asymmetry measured on the real path.
	cli := New(Config{MSS: 100, ISN: isn})
	srv := New(Config{MSS: 400, ISN: isn, RecvBuf: srvRecvBuf})
	return cli, srv
}

func check(t *testing.T, name string, r result, up, down []byte, cli, srv *Endpoint) {
	t.Helper()
	if !r.ok {
		t.Fatalf("%s: transfer did not complete: up %d/%d down %d/%d after %s",
			name, len(r.gotUp), len(up), len(r.gotDown), len(down), r.elapsed)
	}
	if !bytes.Equal(r.gotUp, up) {
		t.Fatalf("%s: uplink bytes corrupted/reordered/duplicated (got %d want %d)", name, len(r.gotUp), len(up))
	}
	if !bytes.Equal(r.gotDown, down) {
		t.Fatalf("%s: downlink bytes corrupted/reordered/duplicated (got %d want %d)", name, len(r.gotDown), len(down))
	}
	t.Logf("%s: ok in %s virtual; up %d B (%.0f B/s) down %d B (%.0f B/s); retx cli %d/%d srv %d/%d",
		name, r.elapsed.Round(time.Second),
		len(up), float64(len(up))/r.elapsed.Seconds(),
		len(down), float64(len(down))/r.elapsed.Seconds(),
		cli.Retx, cli.Sent, srv.Retx, srv.Sent)
}

// --- tests ------------------------------------------------------------------

func TestSetMSS(t *testing.T) {
	e := New(Config{MSS: 100})
	e.Write(make([]byte, 300))

	// First packet should be capped at original MSS 100
	p1 := e.Next(time.Now())
	if len(p1.Data) != 100 {
		t.Fatalf("expected 100 bytes, got %d", len(p1.Data))
	}

	// Let the peer ack the first segment so it has window room
	e.Recv(time.Now(), Packet{Wnd: 1000, Ack: e.sndNxt})

	// Changing MSS to 50 should affect new packets
	e.SetMSS(50)
	p2 := e.Next(time.Now())
	if len(p2.Data) != 50 {
		t.Fatalf("expected 50 bytes, got %d", len(p2.Data))
	}
}

// A segment cut under a large MSS must still get through after the MSS shrinks
// (selector failover to a smaller record type): its retransmission is split.
func TestRetransmitSplitsOversizeSegment(t *testing.T) {
	now := time.Now()
	sender := New(Config{MSS: 400, MinRTO: 10 * time.Millisecond, MaxRTO: 50 * time.Millisecond})
	recv := New(Config{MSS: 400})
	data := payload(9, 400)
	sender.Write(data)

	first := sender.Next(now) // 400-byte segment, "lost"
	if len(first.Data) != 400 {
		t.Fatalf("first segment %d bytes, want 400", len(first.Data))
	}
	sender.SetMSS(100)

	var got []byte
	for i := 0; i < 50 && len(got) < len(data); i++ {
		now = now.Add(time.Second) // past any RTO
		pk := sender.Next(now)
		if len(pk.Data) > 100 {
			t.Fatalf("retransmission of %d bytes exceeds MSS 100", len(pk.Data))
		}
		recv.Recv(now, pk)
		sender.Recv(now, recv.Next(now))
		buf := make([]byte, 400)
		got = append(got, buf[:recv.Read(buf)]...)
		_ = buf
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("stream not delivered intact: got %d bytes", len(got))
	}
}

// Segments the receiver holds behind a gap are reported and never resent; only
// the hole is retransmitted.
func TestSackSkipsHeldSegments(t *testing.T) {
	now := time.Now()
	snd := New(Config{MSS: 100, MinRTO: 10 * time.Millisecond, MaxRTO: 50 * time.Millisecond})
	rcv := New(Config{MSS: 100})
	snd.Write(payload(3, 300))
	snd.Recv(now, Packet{Wnd: 5000}) // peer window: room for all three segments

	var pk [3]Packet
	for i := range pk {
		pk[i] = snd.Next(now)
	}
	// segment 0 is lost; 1 and 2 arrive
	rcv.Recv(now, pk[1])
	rcv.Recv(now, pk[2])
	ack := rcv.Next(now)
	if ack.Ack != 0 || ack.SackOff != 100 || ack.SackLen != 200 {
		t.Fatalf("receiver reported ack=%d sack=[+%d,%d), want ack=0 sack=[+100,200)", ack.Ack, ack.SackOff, ack.SackLen)
	}
	snd.Recv(now, ack)

	now = now.Add(time.Second) // everything unacked is past its RTO
	first := snd.Next(now)
	if first.Seq != 0 || len(first.Data) != 100 {
		t.Fatalf("retransmission was seq=%d len=%d, want the lost segment seq=0 len=100", first.Seq, len(first.Data))
	}
	if again := snd.Next(now); len(again.Data) != 0 {
		t.Fatalf("sacked segment resent: seq=%d len=%d", again.Seq, len(again.Data))
	}
	rcv.Recv(now, first)
	if rcv.rcvNxt != 300 {
		t.Fatalf("receiver at %d after the hole was filled, want 300", rcv.rcvNxt)
	}
}

func TestPacketRoundTrip(t *testing.T) {
	in := Packet{Seq: 0xfffffff0, Ack: 7, Wnd: 1232, SackOff: 300, SackLen: 500, Data: []byte("hello")}
	out, err := Unmarshal(in.Marshal())
	if err != nil || out.Seq != in.Seq || out.Ack != in.Ack || out.Wnd != in.Wnd || out.SackOff != in.SackOff || out.SackLen != in.SackLen || string(out.Data) != "hello" {
		t.Fatalf("round-trip mismatch: %+v err=%v", out, err)
	}
	if _, err := Unmarshal(make([]byte, HeaderLen-1)); err != ErrShort {
		t.Fatalf("want ErrShort, got %v", err)
	}
}

// Clean path: everything arrives, nothing needs resending.
func TestCleanTransfer(t *testing.T) {
	s := newSim(1)
	cli, srv := pair(0, 0)
	up, down := payload(1, 20_000), payload(2, 100_000)
	r := runXfer(t, s, cli, srv, up, down, link{base: 250 * time.Millisecond, jit: 50 * time.Millisecond}, 150*time.Millisecond, 0, 0, 30*time.Minute)
	check(t, "clean", r, up, down, cli, srv)
}

// The measured-path profile: 10% loss each way, duplicates, and jitter larger
// than the poll interval so packets reorder. Sequence numbers start just below
// the uint32 wrap so the modular comparisons are exercised too.
func TestLossyReorderedDuplicatedWrap(t *testing.T) {
	for seed := int64(1); seed <= 5; seed++ {
		s := newSim(seed)
		cli, srv := pair(math.MaxUint32-3000, 0)
		up, down := payload(seed, 20_000), payload(seed+100, 100_000)
		l := link{loss: 0.10, dup: 0.05, base: 250 * time.Millisecond, jit: 600 * time.Millisecond}
		r := runXfer(t, s, cli, srv, up, down, l, 150*time.Millisecond, 0, 0, 60*time.Minute)
		check(t, "lossy", r, up, down, cli, srv)
	}
}

// A 6 s total blackout mid-transfer (both directions dead): the stream must
// stall, back off, and then resume without loss, reorder or duplication.
func TestBlackoutRecovery(t *testing.T) {
	s := newSim(7)
	s.blackFrom, s.blackTo = s.now.Add(10*time.Second), s.now.Add(16*time.Second)
	cli, srv := pair(0, 0)
	up, down := payload(3, 20_000), payload(4, 100_000)
	l := link{loss: 0.05, base: 250 * time.Millisecond, jit: 300 * time.Millisecond}
	r := runXfer(t, s, cli, srv, up, down, l, 150*time.Millisecond, 0, 0, 60*time.Minute)
	check(t, "blackout", r, up, down, cli, srv)
}

// Flow control: a receiver with a tiny buffer that reads slowly must never see
// its buffer exceed RecvBuf, and still gets every byte exactly once.
func TestFlowControlSlowReader(t *testing.T) {
	s := newSim(9)
	const recvBuf = 1200
	cli, srv := pair(0, recvBuf)
	up, down := payload(5, 20_000), payload(6, 5_000)
	l := link{loss: 0.05, base: 200 * time.Millisecond, jit: 200 * time.Millisecond}
	r := runXfer(t, s, cli, srv, up, down, l, 150*time.Millisecond, 30, recvBuf, 60*time.Minute)
	check(t, "flowctl", r, up, down, cli, srv)
	if r.peakSrvBuf < recvBuf/2 {
		t.Fatalf("test is vacuous: receive buffer peaked at %d of %d, window never closed", r.peakSrvBuf, recvBuf)
	}
	if srv.WindowDrops != 0 {
		t.Fatalf("honest sender overran the advertised window %d times: sender-side flow control is not honoring Wnd", srv.WindowDrops)
	}
	t.Logf("flowctl: server rcvBuf peaked at %d of %d", r.peakSrvBuf, recvBuf)
}

// The receive bound is a safety net against a peer that ignores our window: an
// oversized segment must be dropped (not buffered), and a fitting one accepted.
func TestWindowEnforcedAgainstMisbehavingPeer(t *testing.T) {
	e := New(Config{MSS: 100, RecvBuf: 200})
	now := time.Unix(1_000_000, 0)
	e.Recv(now, Packet{Seq: 0, Data: make([]byte, 300)})
	if len(e.rcvBuf) != 0 || e.WindowDrops != 1 {
		t.Fatalf("oversized segment must be dropped: buffered %d, drops %d", len(e.rcvBuf), e.WindowDrops)
	}
	e.Recv(now, Packet{Seq: 0, Data: make([]byte, 200)})
	if len(e.rcvBuf) != 200 || e.WindowDrops != 1 {
		t.Fatalf("fitting segment must be accepted: buffered %d, drops %d", len(e.rcvBuf), e.WindowDrops)
	}
}

// The path measured by the 25-min soak: ~94% exchange success (~3% loss per
// leg), one-way delay ~150-500 ms (RTT p50 ~350 ms, p90 ~800 ms).
func TestMeasuredProfile(t *testing.T) {
	var sent, retx int
	for seed := int64(1); seed <= 5; seed++ {
		s := newSim(seed)
		cli, srv := pair(0, 0)
		up, down := payload(seed, 20_000), payload(seed+50, 100_000)
		l := link{loss: 0.03, dup: 0.01, base: 150 * time.Millisecond, jit: 350 * time.Millisecond}
		r := runXfer(t, s, cli, srv, up, down, l, 150*time.Millisecond, 0, 0, 60*time.Minute)
		check(t, "measured", r, up, down, cli, srv)
		sent += cli.Sent + srv.Sent
		retx += cli.Retx + srv.Retx
	}
	t.Logf("measured profile: %d/%d segments were retransmits (%.1f%%)", retx, sent, 100*float64(retx)/float64(sent))
	// Without SACK this profile measured 11.2%; selective acks must keep it lower.
	if pct := 100 * float64(retx) / float64(sent); pct > 10 {
		t.Errorf("retransmit overhead %.1f%%, want < 10%% with SACK", pct)
	}
}
