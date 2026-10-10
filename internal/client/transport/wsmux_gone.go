package transport

import "time"

// Flows this client no longer runs, or is about to run again.
//
// A flow's opening can be held up on a connection that has stopped getting
// through, while the server, which cannot tell, asks on another connection for
// the flow to be resumed. A client that has never seen the flow says so
// (AttachRejectNeverSeen) and the server opens it again from its first byte.
// Two things must not happen then:
//
//   - a flow that ran here and finished is taken for one never seen: opened
//     again, its request would reach the target twice. So finished flows are
//     remembered for goneFor, far longer than the server takes to ask;
//   - the opening that was held up arrives after all, once its connection gets
//     through, and starts the flow a second time. So an id the server was told
//     to open again is remembered for reopenedFor, longer than any pool
//     connection lives, and admits exactly one opening.
const (
	goneFor     = 2 * time.Minute
	reopenedFor = 15 * time.Minute
)

type goneFlow struct {
	until  time.Time
	reopen bool // told to the server as never seen: one opening is still expected
}

// goneFlows is not safe for concurrent use; its owner locks. The zero value is
// ready.
type goneFlows struct {
	m    map[uint64]goneFlow
	adds int
}

func (g *goneFlows) put(id uint64, f goneFlow, now time.Time) {
	if g.m == nil {
		g.m = make(map[uint64]goneFlow)
	}
	if old, ok := g.m[id]; ok && old.until.After(f.until) {
		f.until = old.until
	}
	g.m[id] = f
	if g.adds++; g.adds >= 1024 { // ponytail: a sweep every so many adds, a timer if it ever matters
		g.adds = 0
		for k, v := range g.m {
			if now.After(v.until) {
				delete(g.m, k)
			}
		}
	}
}

func (g *goneFlows) get(id uint64, now time.Time) (goneFlow, bool) {
	f, ok := g.m[id]
	if ok && now.After(f.until) {
		delete(g.m, id)
		return goneFlow{}, false
	}
	return f, ok
}

// finished remembers a flow that has ended here.
func (g *goneFlows) finished(id uint64, now time.Time) {
	g.put(id, goneFlow{until: now.Add(goneFor)}, now)
}

// neverSeen answers a resume of a flow that is not running here: true when
// nothing is remembered of it either, in which case one opening of it is now
// expected.
func (g *goneFlows) neverSeen(id uint64, now time.Time) bool {
	if f, ok := g.get(id, now); ok && !f.reopen {
		return false
	}
	g.put(id, goneFlow{until: now.Add(reopenedFor), reopen: true}, now)
	return true
}

// admit answers the opening of a flow that is not running here: false for one
// that is remembered, unless it is the one opening that was asked for.
func (g *goneFlows) admit(id uint64, now time.Time) bool {
	f, ok := g.get(id, now)
	if !ok {
		return true
	}
	if !f.reopen {
		return false
	}
	g.put(id, goneFlow{until: now.Add(reopenedFor)}, now)
	return true
}
