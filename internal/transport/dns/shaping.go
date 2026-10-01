package dnsx

import "time"

// Traffic shaping is automatic too: nobody can tune a client that is deployed
// out of reach, so the number of exchanges kept in flight follows what the
// resolvers tolerate. Every shapeInterval the failure ratio of the exchanges in
// that window decides: many failures (throttling, rate limits) halve the worker
// count, a clean window adds a couple back, up to the configured maximum.

const (
	shapeInterval = 3 * time.Second
	shapeMinObs   = 12 // exchanges a window needs before it says anything
	minWorkers    = 4
)

// nextWorkerTarget returns the worker count for the next window.
func nextWorkerTarget(cur, maxW, ok, fail int) int {
	total := ok + fail
	if total < shapeMinObs {
		return cur
	}
	ratio := float64(fail) / float64(total)
	switch {
	case ratio > 0.25:
		cur /= 2
	case ratio < 0.05:
		cur += 2
	}
	return min(max(cur, min(minWorkers, maxW)), maxW)
}

// shape runs the controller until the carrier ends.
func (c *clientConn) shape(maxW int) {
	defer c.wg.Done()
	t := time.NewTicker(shapeInterval)
	defer t.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-t.C:
			ok, fail := int(c.winOK.Swap(0)), int(c.winFail.Swap(0))
			cur := int(c.target.Load())
			if next := nextWorkerTarget(cur, maxW, ok, fail); next != cur {
				c.target.Store(int32(next))
				if c.logf != nil {
					c.logf("dns shaping: %d/%d exchanges failed in the last window, workers %d -> %d", fail, ok+fail, cur, next)
				}
			}
		}
	}
}
