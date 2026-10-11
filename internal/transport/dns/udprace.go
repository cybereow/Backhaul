package dnsx

import (
	"context"
	"net"
	"time"

	"github.com/miekg/dns"
)

// raceGrace is how long exchangeUDPRace keeps listening after a reply that did
// not validate. A forged answer from an on-path hijacker lands within a few
// milliseconds; the genuine one needs the resolver's full recursion, so the
// window is generous but bounded, since it also delays a plain failure.
const raceGrace = 800 * time.Millisecond

// exchangeUDPRace sends msg once over UDP and returns the first reply accept
// likes. A reply that does not validate (same ID, but forged or foreign) does not
// end the exchange: reading continues for raceGrace or until the timeout, and
// that first reply is only returned if nothing valid shows up. Replies with
// another ID are ignored. REFUSED and SERVFAIL are the resolver's own final word,
// not something a hijacker injects ahead of the real answer, so they end it at
// once.
func exchangeUDPRace(ctx context.Context, client *dns.Client, msg *dns.Msg, resolver string, accept func(*dns.Msg) bool) (*dns.Msg, time.Duration, error) {
	var d net.Dialer
	nc, err := d.DialContext(ctx, "udp", resolver)
	if err != nil {
		return nil, 0, err
	}
	co := &dns.Conn{Conn: nc, UDPSize: client.UDPSize}
	defer co.Close()
	// Unblock a pending read when the caller's context ends.
	stop := context.AfterFunc(ctx, func() { _ = co.SetReadDeadline(time.Unix(1, 0)) })
	defer stop()

	start := time.Now()
	deadline := start.Add(client.Timeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	_ = co.SetWriteDeadline(deadline)
	if err := co.WriteMsg(msg); err != nil {
		return nil, time.Since(start), err
	}

	var first *dns.Msg
	var firstRTT time.Duration
	for {
		_ = co.SetReadDeadline(deadline)
		r, err := co.ReadMsg()
		if err != nil {
			if first != nil {
				return first, firstRTT, nil
			}
			if ctx.Err() != nil {
				err = ctx.Err()
			}
			return nil, time.Since(start), err
		}
		if r.Id != msg.Id {
			continue
		}
		rtt := time.Since(start)
		if accept(r) {
			return r, rtt, nil
		}
		if first == nil {
			first, firstRTT = r, rtt
			if r.Rcode == dns.RcodeRefused || r.Rcode == dns.RcodeServerFailure {
				return first, firstRTT, nil
			}
			if g := time.Now().Add(raceGrace); g.Before(deadline) {
				deadline = g
			}
		}
	}
}
