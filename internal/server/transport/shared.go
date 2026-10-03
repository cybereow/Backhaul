package transport

import (
	"context"
	"crypto/subtle"
	"errors"
	"github.com/musix/backhaul/internal/utils/network"
	"net"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type TunnelChannel struct { // for websocket
	conn *network.WebSocketConn
	ping chan struct{}
	mu   *sync.Mutex
}

type LocalTCPConn struct {
	conn        net.Conn
	remoteAddr  string
	timeCreated int64
}

// Accept backoff bounds, same values http.Server.Serve uses.
const (
	acceptMinDelay = 5 * time.Millisecond
	acceptMaxDelay = 1 * time.Second
)

// acceptWithBackoff accepts one connection, absorbing transient failures. It
// returns nil once the listener is dead or ctx is done, which means the caller
// should leave its accept loop.
//
// Every accept loop here used to log the error and continue, which is a busy
// spin for the two ways Accept fails repeatedly: a closed listener returns
// net.ErrClosed immediately and forever, and a descriptor limit (EMFILE when
// the process runs out, ENFILE when the box does) returns immediately for as
// long as the limit holds. Either one pinned a core per listener - so a tunnel
// forwarding many ports would pin the whole box exactly when it was already out
// of resources, and burn the CPU that was needed to recover.
//
// So: give up on a dead listener, and back off 5ms -> 1s doubling on anything
// else, which lets the loop idle until the condition clears. The delay is local
// to the call, so it resets on every successful accept.
func acceptWithBackoff(ctx context.Context, listener net.Listener, logger *logrus.Logger) net.Conn {
	var delay time.Duration

	for {
		conn, err := listener.Accept()
		if err == nil {
			return conn
		}

		if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
			logger.Debugf("accept loop on %s stopping: %v", listener.Addr().String(), err)
			return nil
		}

		if delay == 0 {
			delay = acceptMinDelay
		} else if delay *= 2; delay > acceptMaxDelay {
			delay = acceptMaxDelay
		}

		logger.Debugf("failed to accept connection on %s: %v (retrying in %v)", listener.Addr().String(), err, delay)

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
	}
}

// authorizedToken compares an Authorization header against the expected value
// in constant time.
//
// Plain string != stops at the first differing byte, so the time it takes to
// reject a request reveals how much of the token the guess got right - which
// turns a brute force from "guess the whole secret" into "guess it one byte at
// a time". This check is the only thing between the internet and the tunnel, so
// it is worth the constant-time compare even though a network round trip hides
// most of the signal.
func authorizedToken(authHeader, expected string) bool {
	return subtle.ConstantTimeCompare([]byte(authHeader), []byte(expected)) == 1
}
