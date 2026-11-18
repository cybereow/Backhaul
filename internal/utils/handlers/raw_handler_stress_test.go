package handlers

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	mrand "math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Two raw handlers back to back over a loopback "tunnel", with four flows each
// moving data both ways at once and half-closing at the end - the shape of the
// server/client pair, without the WebSocket plumbing.
func TestRawHandlerBackToBackBidirectional(t *testing.T) {
	const per = 16 << 20
	stream := func(seed int64) io.Reader { return io.LimitReader(mrand.New(mrand.NewSource(seed)), per) }
	sum := func(seed int64) string {
		h := sha256.New()
		io.Copy(h, stream(seed))
		return fmt.Sprintf("%x", h.Sum(nil))
	}
	var wg sync.WaitGroup
	start := time.Now()
	for f := int64(1); f <= 4; f++ {
		f := f
		wg.Add(1)
		go func() {
			defer wg.Done()
			userPeer, userEnd := tcpPair(t) // the user, and the server's socket to it
			tunS, tunC := tcpPair(t)        // the tunnel: server end, client end
			destEnd, destPeer := tcpPair(t) // the client's socket to the service, and the service
			ctx := context.Background()
			go RawConnectionHandler(ctx, false, tunS, nil, userEnd, quietLogger(), nil, 0, false)
			go RawConnectionHandler(ctx, false, tunC, nil, destEnd, quietLogger(), nil, 0, false)

			var inner sync.WaitGroup
			inner.Add(4)
			var upGot, downGot string
			go func() { defer inner.Done(); io.Copy(userPeer, stream(f)); userPeer.CloseWrite() }()
			go func() { defer inner.Done(); io.Copy(destPeer, stream(f+100)); destPeer.CloseWrite() }()
			go func() {
				defer inner.Done()
				h := sha256.New()
				io.Copy(h, destPeer)
				upGot = fmt.Sprintf("%x", h.Sum(nil))
			}()
			go func() {
				defer inner.Done()
				h := sha256.New()
				io.Copy(h, userPeer)
				downGot = fmt.Sprintf("%x", h.Sum(nil))
			}()
			inner.Wait()
			require.Equal(t, sum(f), upGot, "flow %d upload", f)
			require.Equal(t, sum(f+100), downGot, "flow %d download", f)
			t.Logf("flow %d done at %v", f, time.Since(start))
		}()
	}
	wg.Wait()
}
