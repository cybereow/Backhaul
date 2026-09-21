package handlers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/xtaci/smux"
)

// hcTestTimeout bounds every wait in the half-close tests; nothing here uses a
// sleep as synchronization, only channels/deadlines with this ceiling.
const hcTestTimeout = 10 * time.Second

// genPayload is a deterministic, non-repeating-looking byte pattern.
func genPayload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*131 + i/251)
	}
	return b
}

// newSmuxSessions returns a real smux Client/Server pair (v2, as backhaul
// runs it) over net.Pipe, which is a valid transport underneath smux.
func newSmuxSessions(t *testing.T) (client, server *smux.Session) {
	t.Helper()
	c1, c2 := net.Pipe()
	cfg := smux.DefaultConfig()
	cfg.Version = 2
	client, err := smux.Client(c1, cfg)
	if err != nil {
		t.Fatalf("smux.Client: %v", err)
	}
	server, err = smux.Server(c2, cfg)
	if err != nil {
		t.Fatalf("smux.Server: %v", err)
	}
	t.Cleanup(func() {
		client.Close()
		server.Close()
		c1.Close()
		c2.Close()
	})
	return client, server
}

// openStreamPair opens one stream from client and returns both its ends.
func openStreamPair(t *testing.T, client, server *smux.Session) (opened, accepted *smux.Stream) {
	t.Helper()
	opened, err := client.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	ch := make(chan *smux.Stream, 1)
	go func() {
		if s, err := server.AcceptStream(); err == nil {
			ch <- s
		}
	}()
	select {
	case accepted = <-ch:
	case <-time.After(hcTestTimeout):
		t.Fatal("AcceptStream timed out")
	}
	return opened, accepted
}

// streamPair is a fresh session pair with one stream on it.
func streamPair(t *testing.T) (a, b *smux.Stream) {
	t.Helper()
	c, s := newSmuxSessions(t)
	return openStreamPair(t, c, s)
}

func waitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(hcTestTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// dialPair returns the dialing and accepting ends of one real TCP connection.
func dialPair(t *testing.T) (dialed, accepted *net.TCPConn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	ch := make(chan net.Conn, 1)
	go func() {
		if c, err := ln.Accept(); err == nil {
			ch <- c
		}
	}()
	d, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	select {
	case c := <-ch:
		return d.(*net.TCPConn), c.(*net.TCPConn)
	case <-time.After(hcTestTimeout):
		d.Close()
		t.Fatal("accept timed out")
	}
	return nil, nil
}

// recordingHC keeps the first error the peer-facing Read returned, so a test
// can see whether the far side of a flow got a loud error or a clean EOF. It
// embeds *halfCloseConn, so CloseWrite/AbortWrite are still the real hooks.
type recordingHC struct {
	*halfCloseConn
	first chan error
}

func (r *recordingHC) Read(p []byte) (int, error) {
	n, err := r.halfCloseConn.Read(p)
	if err != nil {
		select {
		case r.first <- err:
		default:
		}
	}
	return n, err
}

// bridge is the plan's oracle topology: a real TCP application and a real TCP
// target joined by two unmodified TCPConnectionHandler instances (the backhaul
// server side and the client side) over one real smux Client/Server pair, plus
// a second, independent stream on the same session.
//
//	app --TCP-- [server handler] --smux stream-- [client handler] --TCP-- target
type bridge struct {
	t         *testing.T
	app, tgt  *net.TCPConn
	cancelSrv context.CancelFunc
	srvDone   chan struct{}
	cliDone   chan struct{}
	cliRec    *recordingHC   // nil on the legacy (plain) topology
	srvHC     *halfCloseConn // nil on the legacy (plain) topology
	ping1     net.Conn
	seq       int
}

func newBridge(t *testing.T, envelope bool) *bridge {
	t.Helper()
	client, server := newSmuxSessions(t)
	flowSrv, flowCli := openStreamPair(t, client, server)
	side, sideEcho := openStreamPair(t, client, server)
	go io.Copy(sideEcho, sideEcho)

	app, user := dialPair(t)
	toTarget, tgt := dialPair(t)

	var srvConn, cliConn net.Conn = flowSrv, flowCli
	b := &bridge{t: t, app: app, tgt: tgt, srvDone: make(chan struct{}), cliDone: make(chan struct{}), ping1: side}
	if envelope {
		b.srvHC = newHalfCloseConn(flowSrv)
		srvConn = b.srvHC
		b.cliRec = &recordingHC{halfCloseConn: newHalfCloseConn(flowCli), first: make(chan error, 1)}
		cliConn = b.cliRec
	}

	ctxS, cancelS := context.WithCancel(context.Background())
	ctxC, cancelC := context.WithCancel(context.Background())
	b.cancelSrv = cancelS
	go func() {
		defer close(b.srvDone)
		TCPConnectionHandler(ctxS, false, user, srvConn, testLogger(), nil, 0, false)
	}()
	go func() {
		defer close(b.cliDone)
		TCPConnectionHandler(ctxC, false, cliConn, toTarget, testLogger(), nil, 0, false)
	}()
	t.Cleanup(func() {
		cancelS()
		cancelC()
		app.Close()
		tgt.Close()
		side.Close()
		for name, ch := range map[string]chan struct{}{"server handler": b.srvDone, "client handler": b.cliDone} {
			select {
			case <-ch:
			case <-time.After(hcTestTimeout):
				t.Errorf("%s did not return after teardown", name)
			}
		}
	})
	return b
}

// ping proves the second stream on the shared session still round-trips.
func (b *bridge) ping() {
	b.t.Helper()
	b.seq++
	msg := []byte(fmt.Sprintf("second-stream-ping-%d", b.seq))
	b.ping1.SetDeadline(time.Now().Add(hcTestTimeout))
	if _, err := b.ping1.Write(msg); err != nil {
		b.t.Fatalf("second stream write: %v", err)
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(b.ping1, got); err != nil || !bytes.Equal(got, msg) {
		b.t.Fatalf("second stream echo = %q, %v; want %q", got, err, msg)
	}
}

type tgtResult struct {
	req []byte
	err error
}

// serveTarget scripts the real target. Default: read the request to EOF, report
// it, wait for release, then send reply and CloseWrite (a delayed reply after
// request EOF). replyFirst: send reply + CloseWrite immediately, then read the
// request to EOF (reply END before request EOF).
func (b *bridge) serveTarget(reply []byte, replyFirst bool) (res <-chan tgtResult, release chan struct{}) {
	out := make(chan tgtResult, 1)
	release = make(chan struct{})
	go func() {
		b.tgt.SetDeadline(time.Now().Add(2 * hcTestTimeout))
		sendReply := func() {
			b.tgt.Write(reply)
			b.tgt.CloseWrite()
		}
		if replyFirst {
			sendReply()
		}
		req, err := io.ReadAll(b.tgt)
		out <- tgtResult{req, err}
		if !replyFirst {
			<-release
			sendReply()
		}
	}()
	return out, release
}

func recvTarget(t *testing.T, res <-chan tgtResult) tgtResult {
	t.Helper()
	select {
	case r := <-res:
		return r
	case <-time.After(hcTestTimeout):
		t.Fatal("target never observed EOF on the request")
	}
	return tgtResult{}
}

// runDelayedReply is the oracle: request, application CloseWrite, target sees
// EOF, the second stream is used while the reply is still pending, then the
// delayed reply is released. It returns what the application received.
func (b *bridge) runDelayedReply(req, reply []byte) (got []byte, readErr error) {
	t := b.t
	t.Helper()
	b.ping()
	res, release := b.serveTarget(reply, false)
	if _, err := b.app.Write(req); err != nil {
		t.Fatalf("app write: %v", err)
	}
	if err := b.app.CloseWrite(); err != nil {
		t.Fatalf("app CloseWrite: %v", err)
	}
	r := recvTarget(t, res)
	if !bytes.Equal(r.req, req) {
		t.Fatalf("target request = %d bytes (%v), want %d bytes matching", len(r.req), r.err, len(req))
	}
	b.ping() // usable while the reply is still pending
	close(release)
	b.app.SetReadDeadline(time.Now().Add(hcTestTimeout))
	got, readErr = io.ReadAll(b.app)
	b.ping() // and afterwards
	return got, readErr
}

func (b *bridge) waitDone() {
	b.t.Helper()
	waitClosed(b.t, b.srvDone, "server handler")
	waitClosed(b.t, b.cliDone, "client handler")
}

// isLoudEnd: a torn-down flow must reach the peer as an error, never as io.EOF.
// Close skips its ABORT when a Write is still returning (TryLock, per the spec);
// the stream close that follows then reaches the peer as EOF-without-END, which
// is io.ErrUnexpectedEOF. Both are loud; only a clean io.EOF would be a defect.
func isLoudEnd(err error) bool {
	return err != nil && !errors.Is(err, io.EOF) &&
		(errors.Is(err, errHalfCloseAborted) || errors.Is(err, io.ErrUnexpectedEOF))
}

func (b *bridge) cliReadErr() error {
	b.t.Helper()
	select {
	case err := <-b.cliRec.first:
		return err
	case <-time.After(hcTestTimeout):
		b.t.Fatal("client-side envelope Read never returned an error")
	}
	return nil
}

// serveTargetPart reads exactly n request bytes (proving they crossed the whole
// path), signals it, then drains until the connection ends.
func (b *bridge) serveTargetPart(n int) (gotPart <-chan []byte, res <-chan tgtResult) {
	part := make(chan []byte, 1)
	out := make(chan tgtResult, 1)
	go func() {
		b.tgt.SetDeadline(time.Now().Add(2 * hcTestTimeout))
		buf := make([]byte, n)
		if _, err := io.ReadFull(b.tgt, buf); err != nil {
			out <- tgtResult{nil, err}
			return
		}
		part <- buf
		rest, err := io.ReadAll(b.tgt)
		out <- tgtResult{append(buf, rest...), err}
		b.tgt.Close() // a real target closes once it is done, which ends the reverse copy
	}()
	return part, out
}

// TestSMuxHalfCloseCharacterization records the LEGACY behaviour of a plain
// (non-envelope) smux stream under the oracle scenario: with no CloseWrite on
// smux.Stream, closeWrite falls back to a full Close, so the upload EOF also
// ends the reverse direction and the delayed reply is lost. It documents the
// limitation; it does not (and must not) claim directional-EOF support.
//
// What is asserted is deterministic by construction: by the time the target has
// observed EOF, the server-side stream was already closed locally (that Close is
// what produced the FIN the target's EOF derives from), so no reply byte can
// reach the application. The exact error the application's read ends with
// (clean EOF or a reset) is timing-dependent and is only logged.
func TestSMuxHalfCloseCharacterization(t *testing.T) {
	b := newBridge(t, false)
	req, reply := genPayload(70000), genPayload(50000)
	got, err := b.runDelayedReply(req, reply)
	t.Logf("legacy plain smux: application received %d of %d reply bytes, read error: %v", len(got), len(reply), err)
	if errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("application read hung instead of ending: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("legacy plain smux delivered %d reply bytes after request EOF; the recorded limitation is total loss", len(got))
	}
	b.waitDone()
}

// TestSMuxHalfCloseEnvelope is the same oracle through the half-close envelope.
func TestSMuxHalfCloseEnvelope(t *testing.T) {
	t.Run("delayed reply after request EOF", func(t *testing.T) {
		b := newBridge(t, true)
		req, reply := genPayload(100000), genPayload(90001)
		got, err := b.runDelayedReply(req, reply)
		if err != nil || !bytes.Equal(got, reply) {
			t.Fatalf("application got %d bytes, err %v; want the exact %d-byte reply", len(got), err, len(reply))
		}
		b.waitDone()
	})

	t.Run("empty request", func(t *testing.T) {
		b := newBridge(t, true)
		reply := genPayload(33000)
		got, err := b.runDelayedReply(nil, reply)
		if err != nil || !bytes.Equal(got, reply) {
			t.Fatalf("application got %d bytes, err %v; want the exact %d-byte reply", len(got), err, len(reply))
		}
		b.waitDone()
	})

	t.Run("reply END before request EOF", func(t *testing.T) {
		b := newBridge(t, true)
		req, reply := genPayload(40000), genPayload(70000)
		b.ping()
		res, _ := b.serveTarget(reply, true)
		if _, err := b.app.Write(req); err != nil {
			t.Fatalf("app write: %v", err)
		}
		// The whole reply and its EOF must arrive while the application's own
		// write side is still open.
		b.app.SetReadDeadline(time.Now().Add(hcTestTimeout))
		got, err := io.ReadAll(b.app)
		if err != nil || !bytes.Equal(got, reply) {
			t.Fatalf("application got %d bytes, err %v; want the exact %d-byte reply", len(got), err, len(reply))
		}
		b.ping()
		if err := b.app.CloseWrite(); err != nil {
			t.Fatalf("app CloseWrite: %v", err)
		}
		if r := recvTarget(t, res); !bytes.Equal(r.req, req) {
			t.Fatalf("target request = %d bytes (%v), want %d matching", len(r.req), r.err, len(req))
		}
		b.waitDone()
	})

	t.Run("application full close", func(t *testing.T) {
		b := newBridge(t, true)
		req, reply := genPayload(20000), genPayload(20000)
		b.ping()
		res, release := b.serveTarget(reply, false)
		if _, err := b.app.Write(req); err != nil {
			t.Fatalf("app write: %v", err)
		}
		b.app.Close() // the application goes away without reading the reply
		if r := recvTarget(t, res); !bytes.Equal(r.req, req) {
			t.Fatalf("target request = %d bytes (%v), want %d matching", len(r.req), r.err, len(req))
		}
		b.ping()
		close(release)
		b.waitDone() // both handlers finish; nothing hangs
		b.ping()
	})

	t.Run("closing the flow mid-stream aborts the peer loudly", func(t *testing.T) {
		b := newBridge(t, true)
		b.ping()
		part, res := b.serveTargetPart(1000)
		if _, err := b.app.Write(genPayload(1000)); err != nil {
			t.Fatalf("app write: %v", err)
		}
		select {
		case <-part:
		case r := <-res:
			t.Fatalf("target ended early: %v", r.err)
		case <-time.After(hcTestTimeout):
			t.Fatal("request part never reached the target")
		}
		// Cancel the flow's stream itself while the application is idle. Nothing
		// else writes to the stream, so the only record the peer can get is ABORT.
		b.srvHC.Close()
		if err := b.cliReadErr(); !isLoudEnd(err) {
			t.Fatalf("peer Read error = %v, want the abort error or io.ErrUnexpectedEOF, never io.EOF", err)
		}
		b.ping()
	})

	// Regression for the cancel-path decision: the handler's ctx watcher aborts
	// an envelope conn before closing its plain peer, so the request copy cannot
	// turn the cancel into a clean END. The peer must see a loud error, never
	// io.EOF, every time (this used to be a 29-in-30 clean EOF).
	t.Run("handler ctx cancel aborts the peer loudly", func(t *testing.T) {
		b := newBridge(t, true)
		b.ping()
		part, res := b.serveTargetPart(1000)
		if _, err := b.app.Write(genPayload(1000)); err != nil {
			t.Fatalf("app write: %v", err)
		}
		select {
		case <-part:
		case r := <-res:
			t.Fatalf("target ended early: %v", r.err)
		case <-time.After(hcTestTimeout):
			t.Fatal("request part never reached the target")
		}
		b.cancelSrv()
		if err := b.cliReadErr(); !isLoudEnd(err) {
			t.Fatalf("peer Read error after handler ctx cancel = %v, want the abort error or io.ErrUnexpectedEOF, never io.EOF", err)
		}
		b.app.SetReadDeadline(time.Now().Add(hcTestTimeout))
		if _, err := io.ReadAll(b.app); errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("application read hung after cancel: %v", err)
		}
		b.waitDone()
		b.ping()
	})

	t.Run("mid-payload error is never a clean end", func(t *testing.T) {
		b := newBridge(t, true)
		b.ping()
		part, res := b.serveTargetPart(1000)
		if _, err := b.app.Write(genPayload(1000)); err != nil {
			t.Fatalf("app write: %v", err)
		}
		select {
		case <-part:
		case r := <-res:
			t.Fatalf("target ended early: %v", r.err)
		case <-time.After(hcTestTimeout):
			t.Fatal("request part never reached the target")
		}
		// RST the application connection mid-upload: the server handler's copy
		// fails, transferData calls AbortWrite on the stream, and the far side
		// must see an error rather than a truncated-but-clean end.
		if err := b.app.SetLinger(0); err != nil {
			t.Fatalf("SetLinger: %v", err)
		}
		b.app.Close()
		if err := b.cliReadErr(); !isLoudEnd(err) {
			t.Fatalf("peer Read error = %v, want the abort error or io.ErrUnexpectedEOF, never io.EOF", err)
		}
		b.waitDone()
		b.ping()
	})
}

// TestSMuxHalfCloseCancelLegacyUnchanged pins the other half of the cancel-path
// decision: the ctx watcher's AbortWrite is scoped to *halfCloseConn, so a plain
// (non-envelope) smux stream keeps today's cancel behaviour, which is a full
// close the far end reads as an ordinary clean end. Both handlers still finish
// and the session's other stream stays usable.
func TestSMuxHalfCloseCancelLegacyUnchanged(t *testing.T) {
	b := newBridge(t, false)
	b.ping()
	part, res := b.serveTargetPart(1000)
	if _, err := b.app.Write(genPayload(1000)); err != nil {
		t.Fatalf("app write: %v", err)
	}
	select {
	case <-part:
	case r := <-res:
		t.Fatalf("target ended early: %v", r.err)
	case <-time.After(hcTestTimeout):
		t.Fatal("request part never reached the target")
	}
	b.cancelSrv()
	r := recvTarget(t, res)
	if r.err != nil || len(r.req) != 1000 {
		t.Fatalf("legacy cancel: target saw %d bytes, err %v; want the unchanged clean end after the 1000 bytes", len(r.req), r.err)
	}
	b.waitDone()
	b.ping()
}
