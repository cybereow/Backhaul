package smux

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// liveSessions are the sessions that have not been closed, for StallReport.
var liveSessions sync.Map

// StallReport describes, for a log line, the streams whose writers have been
// waiting for the peer's window for at least minWait, and what this end knows
// about each: how much it has outstanding, the window the peer last granted
// and how long ago that arrived, and what this end is itself granting the
// streams that are receiving. It returns "" when nothing waits that long.
//
// Read together with the same report from the other end it says where a window
// is lost: granted small by the receiver, granted large but arriving late, or
// not granted at all.
func StallReport(minWait time.Duration, most int) string {
	now := time.Now()
	var stuck, recv []string
	liveSessions.Range(func(k, _ any) bool {
		s := k.(*Session)
		s.streamLock.Lock()
		streams := make([]*Stream, 0, len(s.streams))
		for _, st := range s.streams {
			streams = append(streams, st)
		}
		n := len(s.streams)
		s.streamLock.Unlock()
		for _, st := range streams {
			if w := st.waitSince.Load(); w != 0 && now.Sub(time.Unix(0, w)) >= minWait {
				infl := int64(int32(st.numWrittenNow() - st.peerConsumedNow()))
				upd := "never"
				if u := st.lastUpd.Load(); u != 0 {
					upd = now.Sub(time.Unix(0, u)).Round(time.Millisecond).String() + " ago"
				}
				stuck = append(stuck, fmt.Sprintf("stream %d waiting for %v now, %d KB outstanding of the %d KB window it was last granted (update %s; its session carries %d streams, %d active, budget %d KB)",
					st.id, now.Sub(time.Unix(0, w)).Round(time.Millisecond), infl>>10, st.peerWindowNow()>>10, upd, n, s.activeStreams.Load(), s.grantBudget()>>10))
			}
			if d := now.UnixNano() - st.lastData.Load(); st.lastData.Load() != 0 && d < int64(time.Second) {
				s.grantMu.Lock()
				g := st.grant
				s.grantMu.Unlock()
				st.bufferLock.Lock()
				var buffered int
				for _, b := range st.buffers {
					buffered += len(b)
				}
				st.bufferLock.Unlock()
				recv = append(recv, fmt.Sprintf("stream %d is receiving: told the peer %d KB, granted %d KB, %d KB unread (session budget %d KB, rate %d KB/s)",
					st.id, st.announcedNow()>>10, g>>10, buffered>>10, s.grantBudget()>>10, s.rateNow()>>10))
			}
		}
		return true
	})
	if len(stuck) == 0 {
		return ""
	}
	sort.Strings(stuck)
	sort.Strings(recv)
	if len(stuck) > most {
		stuck = stuck[:most]
	}
	if len(recv) > most {
		recv = recv[:most]
	}
	return "writers held up: " + strings.Join(stuck, "; ") + " | receivers: " + strings.Join(recv, "; ")
}

func (s *Stream) numWrittenNow() uint32   { return atomic.LoadUint32(&s.numWritten) }
func (s *Stream) peerConsumedNow() uint32 { return atomic.LoadUint32(&s.peerConsumed) }
func (s *Stream) peerWindowNow() uint32   { return atomic.LoadUint32(&s.peerWindow) }
func (s *Stream) announcedNow() uint32    { return atomic.LoadUint32(&s.announced) }

// rateNow is what the session was seen to deliver, in bytes a second.
func (s *Session) rateNow() int64 { return int64(float64(s.rateBudget.Load()) / rateHorizon.Seconds()) }
