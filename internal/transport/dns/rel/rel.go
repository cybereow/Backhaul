// Package rel is a sans-I/O reliable, ordered byte stream for the DNS carrier.
//
// The carrier only offers unreliable, reorderable, duplicable, high-latency
// exchanges (outside polls, inside answers), and only the outside can start
// one. rel adds what smux needs underneath it: sequence numbers, cumulative
// ACK, per-segment retransmit with adaptive RTO, dedup, in-order delivery and
// receive-window flow control. It performs no I/O and reads no clock: the
// caller feeds packets to Recv, asks Next for the packet to put in the next
// exchange (always one, possibly an empty ACK/poll), and passes time in.
//
// Endpoint is NOT goroutine-safe; wrap it in a mutex if used concurrently.
//
// ponytail ceilings: no SACK (every timed-out segment is resent, so a burst
// loss costs duplicates) and no congestion control (rate is bounded by the
// poller and MaxInflight). Add SACK / AIMD if measurements show they matter.
package rel

import (
	"encoding/binary"
	"errors"
	"time"
)

// HeaderLen is the fixed per-packet overhead: seq(4) ack(4) wnd(2).
const HeaderLen = 10

// Packet is one exchange payload in one direction.
type Packet struct {
	Seq  uint32 // stream offset of Data[0] in the sender's byte stream
	Ack  uint32 // next byte the sender expects from the peer (cumulative)
	Wnd  uint16 // sender's free receive space in bytes, clamped to 65535
	Data []byte
}

func (p Packet) Marshal() []byte {
	b := make([]byte, HeaderLen+len(p.Data))
	binary.BigEndian.PutUint32(b[0:], p.Seq)
	binary.BigEndian.PutUint32(b[4:], p.Ack)
	binary.BigEndian.PutUint16(b[8:], p.Wnd)
	copy(b[HeaderLen:], p.Data)
	return b
}

var ErrShort = errors.New("rel: packet shorter than header")

func Unmarshal(b []byte) (Packet, error) {
	if len(b) < HeaderLen {
		return Packet{}, ErrShort
	}
	return Packet{
		Seq:  binary.BigEndian.Uint32(b[0:]),
		Ack:  binary.BigEndian.Uint32(b[4:]),
		Wnd:  binary.BigEndian.Uint16(b[8:]),
		Data: append([]byte(nil), b[HeaderLen:]...),
	}, nil
}

// Config sizes one endpoint. Zero values take the defaults below.
type Config struct {
	MSS         int           // max data bytes per segment: this side's TX capacity per exchange minus HeaderLen
	MaxInflight int           // max unacked bytes in flight (default 8*MSS)
	RecvBuf     int           // receive buffer bytes; source of the advertised window (default 32768)
	SendBuf     int           // max buffered unsent+unacked bytes accepted by Write (default 65536)
	MinRTO      time.Duration // default 300ms
	MaxRTO      time.Duration // default 8s
	ISN         uint32        // initial sequence number; both ends must agree (0 in production, non-zero to test wraparound)
}

func (c *Config) defaults() {
	if c.MSS <= 0 {
		c.MSS = 100
	}
	if c.MaxInflight <= 0 {
		c.MaxInflight = 8 * c.MSS
	}
	if c.RecvBuf <= 0 {
		c.RecvBuf = 32768
	}
	if c.SendBuf <= 0 {
		c.SendBuf = 65536
	}
	if c.MinRTO <= 0 {
		c.MinRTO = 300 * time.Millisecond
	}
	if c.MaxRTO <= 0 {
		c.MaxRTO = 8 * time.Second
	}
}

type seg struct {
	seq      uint32
	data     []byte
	sentAt   time.Time
	deadline time.Time
	retx     int
}

// Endpoint is one side of the reliable stream.
type Endpoint struct {
	cfg Config

	// send side
	sndBuf   []byte // accepted by Write, not yet segmented
	sndUna   uint32 // oldest unacked byte
	sndNxt   uint32 // next new byte to segment
	inflight []*seg // sent, unacked, in seq order
	peerWnd  int

	srtt, rttvar time.Duration
	backoff      int
	lastBackoff  time.Time

	// receive side
	rcvNxt uint32
	rcvBuf []byte            // in-order, not yet Read
	ooo    map[uint32][]byte // out-of-order segments by seq

	Sent int // segments transmitted (incl. retransmits)
	Retx int // retransmitted segments

	WindowDrops int // segments dropped for exceeding our advertised window (peer ignoring flow control)
}

func New(cfg Config) *Endpoint {
	cfg.defaults()
	return &Endpoint{
		cfg:     cfg,
		sndUna:  cfg.ISN,
		sndNxt:  cfg.ISN,
		rcvNxt:  cfg.ISN,
		peerWnd: cfg.MSS, // until the peer advertises: enough to get the first segment out
		ooo:     map[uint32][]byte{},
	}
}

// SetMSS changes the max data bytes per segment. Affects only newly cut segments;
// in-flight segments keep their size.
func (e *Endpoint) SetMSS(n int) {
	if n > 0 {
		e.cfg.MSS = n
	}
}

// after reports a > b in modular uint32 sequence space.
func after(a, b uint32) bool { return int32(a-b) > 0 }

// Write queues bytes for sending and returns how many were accepted.
func (e *Endpoint) Write(p []byte) int {
	room := e.cfg.SendBuf - e.Pending()
	if room <= 0 {
		return 0
	}
	if len(p) > room {
		p = p[:room]
	}
	e.sndBuf = append(e.sndBuf, p...)
	return len(p)
}

// Read copies in-order delivered bytes into p and returns the count.
func (e *Endpoint) Read(p []byte) int {
	n := copy(p, e.rcvBuf)
	e.rcvBuf = e.rcvBuf[n:]
	return n
}

// Pending is the bytes written but not yet acked by the peer.
func (e *Endpoint) Pending() int {
	return len(e.sndBuf) + e.inflightBytes()
}

// PeerWnd returns the peer's last advertised receive window.
func (e *Endpoint) PeerWnd() int {
	return e.peerWnd
}

func (e *Endpoint) inflightBytes() int {
	n := 0
	for _, s := range e.inflight {
		n += len(s.data)
	}
	return n
}

func (e *Endpoint) window() uint16 {
	w := e.cfg.RecvBuf - len(e.rcvBuf)
	if w < 0 {
		w = 0
	}
	if w > 65535 {
		w = 65535
	}
	return uint16(w)
}

func (e *Endpoint) rto() time.Duration {
	r := time.Second // before the first RTT sample
	if e.srtt > 0 {
		r = e.srtt + 4*e.rttvar
	}
	if r < e.cfg.MinRTO {
		r = e.cfg.MinRTO
	}
	r <<= e.backoff
	if r > e.cfg.MaxRTO {
		r = e.cfg.MaxRTO
	}
	return r
}

func (e *Endpoint) sample(r time.Duration) {
	if e.srtt == 0 {
		e.srtt, e.rttvar = r, r/2
		return
	}
	d := e.srtt - r
	if d < 0 {
		d = -d
	}
	e.rttvar = (3*e.rttvar + d) / 4
	e.srtt = (7*e.srtt + r) / 8
}

// Next returns the packet for the next exchange. Priority: resend the oldest
// timed-out segment; else send new data if the peer window and MaxInflight
// allow; else an empty packet that still carries our ACK and window (it also
// serves as the poll that lets the peer answer).
func (e *Endpoint) Next(now time.Time) Packet {
	pk := Packet{Seq: e.sndNxt, Ack: e.rcvNxt, Wnd: e.window()}

	for _, s := range e.inflight {
		if now.Before(s.deadline) {
			continue
		}
		// Grow the backoff at most once per RTO so a burst of timeouts is one event.
		if now.Sub(e.lastBackoff) >= e.rto() && e.backoff < 6 {
			e.backoff++
			e.lastBackoff = now
		}
		// A segment cut under a larger MSS (the server's reply budget shrinks when
		// the selector fails over to a smaller record type) may no longer fit one
		// exchange: split it so the retransmission can get through.
		if len(s.data) > e.cfg.MSS {
			e.split(s)
		}
		s.retx++
		s.sentAt = now
		s.deadline = now.Add(e.rto())
		e.Sent++
		e.Retx++
		pk.Seq, pk.Data = s.seq, s.data
		return pk
	}

	room := e.peerWnd
	if room > e.cfg.MaxInflight {
		room = e.cfg.MaxInflight
	}
	room -= e.inflightBytes()
	if room > 0 && len(e.sndBuf) > 0 {
		n := len(e.sndBuf)
		if n > e.cfg.MSS {
			n = e.cfg.MSS
		}
		if n > room {
			n = room
		}
		s := &seg{seq: e.sndNxt, data: append([]byte(nil), e.sndBuf[:n]...), sentAt: now}
		s.deadline = now.Add(e.rto())
		e.sndBuf = e.sndBuf[n:]
		e.sndNxt += uint32(n)
		e.inflight = append(e.inflight, s)
		e.Sent++
		pk.Seq, pk.Data = s.seq, s.data
	}
	return pk
}

// split cuts s down to the current MSS and queues the remainder right after it
// as an already-expired segment, so the next Next() retransmits it too.
func (e *Endpoint) split(s *seg) {
	tail := &seg{seq: s.seq + uint32(e.cfg.MSS), data: s.data[e.cfg.MSS:], sentAt: s.sentAt, deadline: s.deadline, retx: s.retx}
	s.data = s.data[:e.cfg.MSS]
	for i, x := range e.inflight {
		if x == s {
			e.inflight = append(e.inflight[:i+1], append([]*seg{tail}, e.inflight[i+1:]...)...)
			return
		}
	}
}

// Recv processes a packet from the peer: its ACK/window first, then its data.
func (e *Endpoint) Recv(now time.Time, p Packet) {
	e.peerWnd = int(p.Wnd)

	if after(p.Ack, e.sndUna) && !after(p.Ack, e.sndNxt) {
		e.sndUna = p.Ack
		sample := time.Duration(-1)
		keep := e.inflight[:0]
		for _, s := range e.inflight {
			if !after(s.seq+uint32(len(s.data)), p.Ack) {
				if s.retx == 0 { // Karn: never sample a retransmitted segment
					sample = now.Sub(s.sentAt)
				}
				continue
			}
			keep = append(keep, s)
		}
		e.inflight = keep
		e.backoff = 0
		if sample >= 0 {
			e.sample(sample)
		}
	}

	if len(p.Data) > 0 {
		e.rx(p.Seq, p.Data)
	}
}

func (e *Endpoint) rx(seq uint32, d []byte) {
	end := seq + uint32(len(d))
	if !after(end, e.rcvNxt) {
		return // entirely old: duplicate
	}
	if after(e.rcvNxt, seq) { // partial overlap: keep only the new tail
		d = d[e.rcvNxt-seq:]
		seq = e.rcvNxt
	}
	free := e.cfg.RecvBuf - len(e.rcvBuf)
	if int(seq-e.rcvNxt)+len(d) > free {
		e.WindowDrops++
		return // beyond our window; the sender will retransmit
	}
	if seq != e.rcvNxt {
		if _, dup := e.ooo[seq]; !dup {
			e.ooo[seq] = append([]byte(nil), d...)
		}
		return
	}
	e.rcvBuf = append(e.rcvBuf, d...)
	e.rcvNxt += uint32(len(d))

	// Drain buffered segments that are now contiguous (or overlap what we have).
	for progress := true; progress; {
		progress = false
		for k, v := range e.ooo {
			if after(k, e.rcvNxt) {
				continue
			}
			delete(e.ooo, k)
			if kend := k + uint32(len(v)); after(kend, e.rcvNxt) {
				e.rcvBuf = append(e.rcvBuf, v[e.rcvNxt-k:]...)
				e.rcvNxt = kend
				progress = true
			}
		}
	}
}
