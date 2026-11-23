package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"time"

	"github.com/musix/backhaul/internal/utils/handlers"
	"github.com/musix/backhaul/internal/utils/network"
)

// rawProbeLoop shows the server, over and over, that raw flows get through the
// path to it. The server sends a raw flow only after a probe has come back
// intact, so a path that reads the bytes after the upgrade as WebSocket frames
// (a CDN or a WAF) costs speed, not flows. It probes quickly while the answer is
// not yet good, then once an interval to keep the verdict fresh and to notice a
// path that stopped carrying raw bytes.
func (c *WsTransport) rawProbeLoop() {
	ctx := c.ctx
	interval := c.config.RawProbeInterval
	if interval < 0 {
		c.rawFirstDone()
		return
	}
	if interval == 0 {
		interval = time.Minute
	}
	retry := interval / 6
	if retry < 50*time.Millisecond {
		retry = 50 * time.Millisecond
	}
	unsupported := interval * 10 // a server that does not know the path: ask rarely

	var wait time.Duration // the first round runs at once: the pool waits for it
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		// Every entry point is probed: the verdict is per entry point, since two
		// of them can lead through different CDNs.
		allOK, anyUnsupported := true, false
		for idx := range c.endpoints {
			err := c.rawProbeOnce(ctx, idx)
			switch {
			case err == nil:
				c.setRawOK(idx, time.Now())
			case errors.Is(err, errRawProbeUnsupported):
				c.setRawOK(idx, time.Time{})
				c.logger.Debugf("the server does not answer raw probes: %v", err)
				allOK, anyUnsupported = false, true
			default:
				if ctx.Err() != nil {
					return
				}
				c.setRawOK(idx, time.Time{})
				c.logger.Debugf("raw probe via %s failed: %v", c.endpoints[idx].addr, err)
				allOK = false
			}
		}
		c.rawFirstDone()
		switch {
		case allOK:
			wait = interval + time.Duration(rand.Int63n(int64(interval/5)+1))
		case anyUnsupported:
			wait = unsupported
		default:
			wait = retry
		}
	}
}

// rawFirstDone marks the end of the first round of probes, whatever came of it.
func (c *WsTransport) rawFirstDone() {
	c.rawFirstOnce.Do(func() {
		if c.rawFirst != nil {
			close(c.rawFirst)
		}
	})
}

// awaitFirstRawProbe holds back the first pool fill until the first probe has
// an answer (or a few seconds have passed): a connection dialled before it is
// not marked as able to carry raw flows, and would carry framed ones for as long
// as it sits in the pool, at the front of the queue.
func (c *WsTransport) awaitFirstRawProbe(ctx context.Context) {
	if c.rawFirst == nil || c.config.RawProbeInterval < 0 {
		return
	}
	t := time.NewTimer(firstProbeWait)
	defer t.Stop()
	select {
	case <-c.rawFirst:
	case <-t.C:
	case <-ctx.Done():
	}
}

// firstProbeWait bounds how long the first pool fill waits for the first probe.
const firstProbeWait = 4 * time.Second

func (c *WsTransport) setRawOK(idx int, at time.Time) {
	if idx < 0 || idx >= len(c.rawOKAt) {
		return
	}
	if at.IsZero() {
		c.rawOKAt[idx].Store(0)
		return
	}
	c.rawOKAt[idx].Store(at.UnixNano())
}

// rawVerified reports whether entry point idx passed a probe recently enough to
// be trusted with raw flows. The window is three probe intervals, so a missed
// round does not cost it, but a path that stopped answering does.
func (c *WsTransport) rawVerified(idx int) bool {
	if idx < 0 || idx >= len(c.rawOKAt) {
		return false
	}
	ok := c.rawOKAt[idx].Load()
	if ok == 0 {
		return false
	}
	interval := c.config.RawProbeInterval
	if interval <= 0 {
		interval = time.Minute
	}
	return time.Since(time.Unix(0, ok)) < 3*interval
}

// errRawProbeUnsupported: the server does not know the probe path or message.
var errRawProbeUnsupported = errors.New("raw probe not supported by the server")

// rawProbeOnce runs one probe: upgrade, wait for the message that turns the
// connection raw, and echo the server's payload back, with this client's
// capabilities after it.
func (c *WsTransport) rawProbeOnce(ctx context.Context, idx int) error {
	ep := c.endpoints[idx]
	conn, err := network.WebSocketDialer(ctx, ep.addr, ep.edgeIP, network.NormalizeBasePath(c.config.Path)+handlers.RawProbeSuffix, c.config.DialTimeOut, c.config.KeepAlive, c.config.Nodelay, c.config.Token, c.userAgent, c.config.Mode, 1, c.config.SO_RCVBUF, c.config.SO_SNDBUF, c.config.MSS, c.config.TLSVerify)
	if err != nil {
		if strings.Contains(err.Error(), "unexpected HTTP response status") {
			return fmt.Errorf("%w: %v", errRawProbeUnsupported, err)
		}
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(handlers.RawProbeTimeout))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		return err
	}
	if string(msg) != handlers.RawProbeMsg {
		return fmt.Errorf("%w: unexpected message %q", errRawProbeUnsupported, msg)
	}
	left, raw := conn.Detach()
	echo := make([]byte, handlers.RawProbeLen+1)
	if _, err := io.ReadFull(io.MultiReader(bytes.NewReader(left), raw), echo[:handlers.RawProbeLen]); err != nil {
		return err
	}
	echo[handlers.RawProbeLen] = c.rawCaps()
	_, err = raw.Write(echo)
	return err
}

// rawCaps is what this client reports to the server with its probe.
func (c *WsTransport) rawCaps() byte { return handlers.CapRawResume }

// resumeRawFlow runs the client's side of a move: the server has handed this
// connection to the flow it names.
func (c *WsTransport) resumeRawFlow(conn *network.WebSocketConn, id string) {
	v, ok := c.rawFlows.Load(id)
	if !ok {
		c.logger.Debugf("a move was asked for flow %s, which is not running here", id)
		conn.Close()
		return
	}
	left, raw := conn.Detach()
	if err := v.(*handlers.RawFlow).Swap(&handlers.RawTunnel{Conn: raw, Left: left, Born: time.Now()}); err != nil {
		c.logger.Debugf("move of flow %s: %v", id, err)
	}
}
