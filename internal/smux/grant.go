package smux

import (
	"encoding/binary"
	"sync/atomic"
	"time"
)

// Sharing the session's receive budget among its streams. BACKHAUL PATCH.
//
// A session may buffer MaxReceiveBuffer bytes in all (the bucket), and each stream
// may have MaxStreamBuffer bytes outstanding. Nothing ties the two together: with
// a 64 MiB session and 16 MiB streams, four streams whose readers fall behind use
// the whole bucket. recvLoop then stops reading the connection - for every stream
// on it, and for the frames that are not data at all: stream openings, window
// updates, the echo of an RTT probe. From outside, the connection has stopped
// getting through; it stays that way until a slow reader catches up, which under
// load can be tens of seconds.
//
// So what a stream is told it may have outstanding (its window) is granted out of
// the budget instead of being a constant:
//
//   - a window is only raised out of what is left of grantShare of the bucket, so
//     streams whose readers fall behind cannot use the bucket up between them and
//     recvLoop keeps reading;
//   - a stream gets its configured window when there is room, and an equal share
//     of the budget when the streams that are carrying data would not all fit;
//   - a stream that has gone quiet gives its window back (idleReclaim).
//
// The accounting is exact for a peer that honours its window: for each stream it
// keeps the highest position the peer was ever allowed to send up to, so the
// grant of a stream whose window was reduced only shrinks as its reader consumes
// what was already on its way.
//
// Protocol version 2 only (version 1 has no windows), and only the receiving side
// changes: the peer sees ordinary window updates.
const (
	// grantShareNum/grantShareDen of the bucket may be granted. The rest is for
	// what cannot be refused: every stream starts on initialPeerWindow, which its
	// peer assumes before the first update, however many streams there already
	// are. Half the bucket leaves room for that many new streams at once - 128 on
	// a 64 MiB session - on top of everything already granted.
	grantShareNum, grantShareDen = 1, 2
	// grantFloor is the least a stream is ever granted, so that it always moves.
	grantFloor = 64 << 10
	// grantTick is how often a session counts the streams carrying data and
	// takes back the windows of idle ones.
	grantTick = 500 * time.Millisecond

	// What may be granted also follows what the connection delivers.
	//
	// Whatever a peer is allowed to have outstanding it sends, and what the path
	// cannot carry at once waits in it: in the peer's socket, and in every proxy
	// between the two ends, each of which takes megabytes before it pushes back.
	// Everything else on the connection waits behind that - a new flow's first
	// bytes, a window update, a probe's echo - for (bytes outstanding) / (rate of
	// the connection). 16 MiB outstanding on a connection that delivers 2.5 MB/s
	// is six seconds, on every flow that shares it. Windows sized in bytes are, on
	// a slow connection, a queue measured in seconds.
	//
	// So the budget is what the connection was seen to deliver in rateHorizon: a
	// fast connection is granted as much as before, a slow one only what it can
	// carry in that time. Twice what arrives leaves room to speed up - the rate
	// can double every round trip up to rateHorizon/2 long - and bounds what waits
	// in the path to about rateHorizon of delay.
	rateTick    = 100 * time.Millisecond
	rateSamples = 10 // the rate is the highest of this many ticks: a pause does not shrink it at once
	rateHorizon = 500 * time.Millisecond
	// rateFloor is the least a session's budget falls to, so that a connection
	// which has carried nothing can show what it delivers.
	rateFloor = 1 << 20
	// idleReclaim: a stream with nothing buffered that has received no data for
	// this long is taken to have nothing on its way either, and is put back on
	// the initial window.
	idleReclaim = 3 * time.Second
)

// grantOpen accounts for a new stream: until its first update the peer assumes
// initialPeerWindow.
func (s *Session) grantOpen(st *Stream) {
	if s.config.Version != 2 {
		return
	}
	s.grantMu.Lock()
	st.limit = initialPeerWindow
	st.grant = initialPeerWindow
	s.grantSum += st.grant
	s.grantMu.Unlock()
	atomic.StoreUint32(&st.announced, initialPeerWindow)
}

// grantClose gives a closed stream's grant back.
func (s *Session) grantClose(st *Stream) {
	s.grantMu.Lock()
	s.grantSum -= st.grant
	st.grant = 0
	s.grantMu.Unlock()
}

// grantWindow decides the window to announce to st's peer now that st has consumed
// up to consumed, given the most st would take (limit: its configured or overridden
// window).
func (s *Session) grantWindow(st *Stream, consumed uint32, limit uint32) uint32 {
	budget := s.grantBudget()
	active := int64(max(1, s.activeStreams.Load()))

	s.grantMu.Lock()
	defer s.grantMu.Unlock()

	st.consumed64 += uint64(consumed - st.updConsumed) // wrap-safe
	st.updConsumed = consumed
	held := int64(st.limit - st.consumed64) // the most the peer can still have outstanding

	want := min(int64(limit), max(grantFloor, budget/active))
	if want > held {
		// More than it has now: only out of what the other streams leave.
		free := budget - (s.grantSum - st.grant)
		want = max(held, min(want, free))
	}
	want = max(want, min(grantFloor, int64(limit)))

	if pos := st.consumed64 + uint64(want); pos > st.limit {
		st.limit = pos
	}
	grant := int64(st.limit - st.consumed64)
	s.grantSum += grant - st.grant
	st.grant = grant
	atomic.StoreUint32(&st.announced, uint32(want))
	return uint32(want)
}

// grantBudget is what the streams may be granted in all right now: the share of
// the bucket set aside for it, or less on a connection that delivers less.
func (s *Session) grantBudget() int64 {
	most := int64(s.config.MaxReceiveBuffer) * grantShareNum / grantShareDen
	// Nothing measured yet is nothing delivered yet: a connection starts on the
	// floor and is granted more as it shows what it carries.
	return min(most, max(rateFloor, s.rateBudget.Load()))
}

// grantLoop keeps the delivered rate and activeStreams current and takes idle
// streams' windows back.
func (s *Session) grantLoop() {
	t := time.NewTicker(rateTick)
	defer t.Stop()
	var (
		streams []*Stream
		samples [rateSamples]int64
		tick    int
		seen    = s.rxData.Load()
		last    = time.Now()
	)
	for {
		select {
		case <-s.die:
			return
		case now := <-t.C:
			if dt := now.Sub(last); dt > 0 {
				got := s.rxData.Load()
				samples[tick%rateSamples] = int64(float64(got-seen) / dt.Seconds())
				seen, last = got, now
				rate := int64(0)
				for _, v := range samples {
					rate = max(rate, v)
				}
				s.rateBudget.Store(max(rateFloor, int64(float64(rate)*rateHorizon.Seconds())))
			}
			tick++
			if tick%int(grantTick/rateTick) != 0 {
				continue
			}
			streams = streams[:0]
			s.streamLock.Lock()
			for _, st := range s.streams {
				streams = append(streams, st)
			}
			s.streamLock.Unlock()

			active := int32(0)
			for _, st := range streams {
				st.bufferLock.Lock()
				buffered := len(st.buffers) > 0
				st.bufferLock.Unlock()
				quiet := now.UnixNano()-st.lastData.Load() >= int64(idleReclaim)
				if buffered || !quiet {
					active++
					continue
				}
				st.reclaimWindow()
			}
			s.activeStreams.Store(active)
		}
	}
}

// reclaimWindow puts an idle stream back on the initial window and tells the
// peer. Nothing is buffered and nothing has arrived for idleReclaim, so nothing
// is taken to be on its way: the stream's grant is the new window alone.
func (s *Stream) reclaimWindow() {
	s.updLock.Lock()
	defer s.updLock.Unlock()

	sess := s.sess
	sess.grantMu.Lock()
	if s.grant <= initialPeerWindow {
		sess.grantMu.Unlock()
		return
	}
	s.bufferLock.Lock()
	consumed := s.numRead
	s.bufferLock.Unlock()
	s.consumed64 += uint64(consumed - s.updConsumed)
	s.updConsumed = consumed
	window := min(uint32(initialPeerWindow), s.receiveWindow())
	s.limit = s.consumed64 + uint64(window)
	sess.grantSum += int64(window) - s.grant
	s.grant = int64(window)
	sess.grantMu.Unlock()
	atomic.StoreUint32(&s.announced, window)

	frame := newFrame(byte(sess.config.Version), cmdUPD, s.id)
	var hdr updHeader
	binary.LittleEndian.PutUint32(hdr[:], consumed)
	binary.LittleEndian.PutUint32(hdr[4:], window)
	frame.data = hdr[:]
	deadline := time.NewTimer(grantTick)
	defer deadline.Stop()
	_, _ = sess.writeFrameInternal(frame, deadline.C, CLSCTRL)
}

// updateQuantum is how many consumed bytes pass between two window updates: a
// fraction of the window the peer currently has.
func (s *Stream) updateQuantum() uint32 {
	return max(atomic.LoadUint32(&s.announced)/windowUpdateDivisor, 1)
}
