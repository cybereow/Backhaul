package transport

import (
	"sync/atomic"
	"time"
)

// shrinkTokenTTL is how long a pool shrink may absorb a new-connection request.
// The token exists so a request already in flight when the pool shrinks does not
// immediately regrow it. It must not outlive that moment: a stale token would eat
// a later request - above all the server's rotation request for a replacement
// connection - and delay the rotation until its slow re-ask, by which time the
// CDN may have cut the aging connection.
const shrinkTokenTTL = 5 * time.Second

// markShrink records that a pool shrink just queued a controlFlow token.
func markShrink(lastShrink *int64) {
	atomic.StoreInt64(lastShrink, time.Now().UnixNano())
}

// swallowShrinkToken reports whether an SG_Chan should be ignored because a
// recent shrink queued a token for it. Tokens older than shrinkTokenTTL are
// discarded, with every other leftover one, and the request is served.
func swallowShrinkToken(flow chan struct{}, lastShrink *int64) bool {
	select {
	case <-flow:
	default:
		return false
	}
	if time.Since(time.Unix(0, atomic.LoadInt64(lastShrink))) <= shrinkTokenTTL {
		return true
	}
	for {
		select {
		case <-flow:
		default:
			return false
		}
	}
}
