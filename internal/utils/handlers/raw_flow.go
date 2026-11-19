package handlers

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/musix/backhaul/internal/web"
	"github.com/sirupsen/logrus"
)

// A raw flow is one user connection carried over one tunnel connection with no
// framing (see RawFlowPrefix). A CDN or a load balancer in front of the tunnel
// may close a connection once it has lived long enough, and then the flow dies
// with it. RawFlow can move a live flow to another tunnel connection without
// losing, repeating or reordering a byte, and without keeping a copy of the data:
//
//   - Each direction is a pump (io.Copy, which is splice(2) between two TCP
//     sockets) that can be paused. Pausing sets a read deadline in the past on the
//     pump's source; the copy loops return between chunks, never inside one, so a
//     paused pump has moved exactly the bytes it reports.
//   - To move, the two ends pause their sending pumps, tell each other on the new
//     connection how many bytes they sent on the old one, drain the old one to
//     exactly the count the other reported, confirm that, and only then switch to
//     the new connection and close the old. TCP delivers the old connection's
//     bytes in order, so reading to the count is exact.
//   - Up to the confirmation either end may give up and both carry on over the
//     old connection, because pausing and draining only ever deliver bytes the
//     flow would have delivered anyway. A flow in which one direction has already
//     ended is never moved.
//
// One window cannot be closed: if the new connection dies between the two
// confirmations, one end may have switched and the other not, and the flow is
// cut. That is a failure of the connection the flow was moving to, the same as
// any connection dying under a flow.
const (
	// RawFlowResumePrefix opens the message that tells a client "this connection
	// is the new carrier of that flow": RAW2R|<flow id>.
	RawFlowResumePrefix = "RAW2R|"
	// RawFlowIDPrefix opens the flow-open message of a flow that can be moved:
	// RAW2|<flow id>|<address>.
	RawFlowIDPrefix = "RAW2|"

	swapAck = 0xA5
)

// swapTimeout bounds one move, from the pause to the confirmation.
var swapTimeout = 15 * time.Second

// RawTunnel is a tunnel connection that has turned raw, with the bytes that were
// read ahead of that moment.
type RawTunnel struct {
	Conn net.Conn
	Left []byte    // read ahead of the point the stream turned raw
	Born time.Time // when the connection was established, to schedule its replacement
}

// RawFlowConfig configures a RawFlow.
type RawFlowConfig struct {
	ID            string
	Tunnel        *RawTunnel
	App           net.Conn // the user's connection (server) or the local service's (client)
	ProxyProtocol bool
	Logger        *logrus.Logger
	Usage         *web.Usage
	RemotePort    int
	Sniffer       bool
}

// dir is the control block of one pump.
type dir struct {
	pauseReq atomic.Bool
	paused   chan int64 // pump -> controller: paused at this many bytes of the segment
	resume   chan resumeCmd
	finished chan struct{} // closed when the pump has ended
}

type resumeCmd struct {
	drain int64      // in pump: bytes still to read from the old tunnel first
	park  bool       // in pump: after draining, stop again and wait for the next command
	tun   *RawTunnel // the tunnel to carry on with; nil = stay on the current one
}

func newDir() *dir {
	return &dir{paused: make(chan int64, 1), resume: make(chan resumeCmd, 1), finished: make(chan struct{})}
}

// RawFlow runs one raw flow. See the comment at the top of the file.
type RawFlow struct {
	cfg    RawFlowConfig
	ctx    context.Context
	cancel context.CancelFunc
	app    net.Conn
	toTun  []byte // the user's bytes read before the flow started: the first thing sent

	mu  sync.Mutex
	cur *RawTunnel // the tunnel the flow is on

	out, in *dir
	swapMu  sync.Mutex // one move at a time

	closeOnce sync.Once
	done      chan struct{}
	wg        sync.WaitGroup

	swaps atomic.Int64 // completed moves
}

// NewRawFlow prepares a flow. Run starts it.
func NewRawFlow(ctx context.Context, cfg RawFlowConfig) *RawFlow {
	f := &RawFlow{cfg: cfg, app: cfg.App, cur: cfg.Tunnel, out: newDir(), in: newDir(), done: make(chan struct{})}
	if pc, ok := cfg.App.(PrefixedConn); ok {
		f.toTun, f.app = pc.TakePrefix()
	}
	f.ctx, f.cancel = context.WithCancel(ctx)
	return f
}

// ID is the flow's identifier (empty for a flow that cannot be moved).
func (f *RawFlow) ID() string { return f.cfg.ID }

// Done is closed when the flow has ended.
func (f *RawFlow) Done() <-chan struct{} { return f.done }

// Swaps reports how many times the flow has been moved to another connection.
func (f *RawFlow) Swaps() int64 { return f.swaps.Load() }

// CurrentBorn is when the connection the flow is on was established.
func (f *RawFlow) CurrentBorn() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cur.Born
}

func (f *RawFlow) curTun() *RawTunnel {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cur
}

func (f *RawFlow) setTun(t *RawTunnel) {
	f.mu.Lock()
	f.cur = t
	f.mu.Unlock()
}

func (f *RawFlow) logf() *logrus.Logger { return f.cfg.Logger }

// Close ends the flow at once, closing every connection it holds.
func (f *RawFlow) Close() { f.teardown() }

func (f *RawFlow) teardown() {
	f.closeOnce.Do(func() {
		f.cancel()
		f.curTun().Conn.Close()
		f.app.Close()
	})
}

func (f *RawFlow) account(n int64) {
	if f.cfg.Sniffer && n > 0 && f.cfg.Usage != nil {
		f.cfg.Usage.AddOrUpdatePort(f.cfg.RemotePort, uint64(n))
	}
}

// Run carries the flow until both directions have ended, then closes everything.
func (f *RawFlow) Run() {
	defer close(f.done)
	t := f.curTun()
	if f.cfg.ProxyProtocol {
		header, err := ProxyProtocolHeader(f.app.RemoteAddr(), t.Conn.RemoteAddr())
		if err != nil {
			f.cfg.Logger.Error(err)
			f.teardown()
			return
		}
		if _, err := t.Conn.Write(header); err != nil {
			f.cfg.Logger.Errorf("failed to send Proxy Protocol v2 header: %v", err)
			f.teardown()
			return
		}
	}
	go func() {
		select {
		case <-f.ctx.Done():
			f.teardown()
		case <-f.done:
		}
	}()
	f.wg.Add(2)
	go f.inPump(t)
	go f.outPump()
	f.wg.Wait()
	f.teardown()
}

// outPump copies the app's bytes to the tunnel.
func (f *RawFlow) outPump() {
	defer f.wg.Done()
	defer close(f.out.finished)
	t := f.curTun()
	if len(f.toTun) > 0 {
		if _, err := t.Conn.Write(f.toTun); err != nil {
			f.teardown()
			return
		}
		f.account(int64(len(f.toTun)))
	}
	var seg int64 // bytes written on the current tunnel
	for {
		n, err := io.Copy(t.Conn, f.app)
		seg += n
		f.account(n)
		switch {
		case err == nil: // the app is done sending: a clean half-close
			closeWrite(t.Conn)
			return
		case isTimeout(err) && f.out.pauseReq.Load():
			f.app.SetReadDeadline(time.Time{})
			f.out.pauseReq.Store(false)
			f.out.paused <- seg
			if cmd := <-f.out.resume; cmd.tun != nil {
				t, seg = cmd.tun, 0
			}
		default:
			f.logf().Trace("raw app -> tunnel: ", err)
			f.teardown()
			return
		}
	}
}

// inPump copies the tunnel's bytes to the app.
func (f *RawFlow) inPump(t *RawTunnel) {
	defer f.wg.Done()
	defer close(f.in.finished)
	var seg int64 // bytes delivered from the current tunnel
	if len(t.Left) > 0 {
		if _, err := f.app.Write(t.Left); err != nil {
			f.teardown()
			return
		}
		seg += int64(len(t.Left))
		f.account(int64(len(t.Left)))
		t.Left = nil
	}
	for {
		n, err := io.Copy(f.app, t.Conn)
		seg += n
		f.account(n)
		switch {
		case err == nil: // the peer is done sending: a clean half-close
			closeWrite(f.app)
			return
		case isTimeout(err) && f.in.pauseReq.Load():
			t.Conn.SetReadDeadline(time.Time{})
			f.in.pauseReq.Store(false)
			f.in.paused <- seg
			cmd := <-f.in.resume
			for {
				if cmd.drain > 0 { // what the peer sent on this tunnel that has not arrived yet
					m, err := io.CopyN(f.app, t.Conn, cmd.drain)
					seg += m
					f.account(m)
					if err != nil {
						f.logf().Trace("raw drain: ", err)
						f.teardown()
						return
					}
				}
				if !cmd.park {
					break
				}
				f.in.paused <- seg // drained: stop again until the move is decided
				cmd = <-f.in.resume
			}
			if cmd.tun != nil {
				t, seg = cmd.tun, 0
				if len(t.Left) > 0 {
					if _, err := f.app.Write(t.Left); err != nil {
						f.teardown()
						return
					}
					seg += int64(len(t.Left))
					f.account(int64(len(t.Left)))
					t.Left = nil
				}
			}
		default:
			f.logf().Trace("raw tunnel -> app: ", err)
			f.teardown()
			return
		}
	}
}

// pause asks a pump to stop at a byte boundary and returns how many bytes it has
// moved on its current tunnel. kick interrupts the pump's blocking read. ok is
// false if the pump ended instead (or did not stop in time; it is then resumed
// as soon as it does stop).
func (f *RawFlow) pause(d *dir, kick func(), deadline time.Time) (count int64, ok bool) {
	d.pauseReq.Store(true)
	kick()
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case n := <-d.paused:
		return n, true
	case <-d.finished:
		d.pauseReq.Store(false)
		return 0, false
	case <-timer.C:
		go func() { // give up, but never leave the pump parked
			select {
			case <-d.paused:
				d.resume <- resumeCmd{}
			case <-d.finished:
			}
		}()
		return 0, false
	}
}

// rawReader reads the bytes read ahead of a tunnel's turning raw first, then the
// connection itself. Used for the few bytes of the move; the data path reads the
// connection directly so that splice(2) applies.
type rawReader struct {
	left []byte
	conn net.Conn
}

func (r *rawReader) Read(p []byte) (int, error) {
	if len(r.left) > 0 {
		n := copy(p, r.left)
		r.left = r.left[n:]
		return n, nil
	}
	return r.conn.Read(p)
}

// Swap moves the flow to t2, a tunnel connection that has just turned raw and
// whose other end is Swap-ing the same flow at the same time. It returns nil once
// the flow is on t2 and the old connection is closed. On any other return the
// flow is still on its old connection (an error that is ErrFlowCut means it was
// cut), and t2 is closed.
func (f *RawFlow) Swap(t2 *RawTunnel) error {
	f.swapMu.Lock()
	defer f.swapMu.Unlock()
	err := f.swap(t2)
	if err != nil {
		t2.Conn.Close()
	}
	return err
}

// ErrFlowCut: the move failed in the one window that cannot be undone.
var ErrFlowCut = errors.New("raw flow cut during a move to another connection")

// errMoveAborted: the move was given up; the flow carries on where it was.
var errMoveAborted = errors.New("move of a raw flow given up")

func (f *RawFlow) swap(t2 *RawTunnel) error {
	t1 := f.curTun()
	deadline := time.Now().Add(swapTimeout)
	_ = t2.Conn.SetDeadline(deadline)
	rr := &rawReader{left: t2.Left, conn: t2.Conn}

	// 1. Stop sending on the old connection, at a boundary.
	outCount, outOK := f.pause(f.out, func() { f.app.SetReadDeadline(time.Now()) }, deadline)
	resumeOut := func() {
		if outOK {
			f.out.resume <- resumeCmd{}
		}
	}

	// 2. Tell the other end where that is, and learn where its sending stopped. A
	// failure up to here is safe to undo: nothing has changed on the old connection.
	var hdr [9]byte
	if !outOK {
		hdr[0] = 1
	}
	binary.BigEndian.PutUint64(hdr[1:], uint64(outCount))
	if _, err := t2.Conn.Write(hdr[:]); err != nil {
		resumeOut()
		return fmt.Errorf("%w: %v", errMoveAborted, err)
	}
	var peer [9]byte
	if _, err := io.ReadFull(rr, peer[:]); err != nil {
		resumeOut()
		return fmt.Errorf("%w: %v", errMoveAborted, err)
	}
	if !outOK || peer[0] != 0 {
		resumeOut()
		return errMoveAborted // one end has a direction that is already over
	}
	peerOut := int64(binary.BigEndian.Uint64(peer[1:]))

	// From here the other end is moving too: if this fails the flow is cut, on
	// both ends, rather than left half moved.
	cut := func(why string, err error) error {
		resumeOut()
		f.teardown()
		return fmt.Errorf("%w: %s: %v", ErrFlowCut, why, err)
	}

	// 3. Stop reading the old connection, at a boundary, and take in what the
	// other end sent on it, up to its count.
	inCount, inOK := f.pause(f.in, func() { t1.Conn.SetReadDeadline(time.Now()) }, deadline)
	if !inOK {
		return cut("reading the old connection", errors.New("did not stop"))
	}
	remaining := peerOut - inCount
	if remaining < 0 {
		return cut("counts", fmt.Errorf("peer sent %d bytes on the old connection, %d already arrived", peerOut, inCount))
	}
	f.in.resume <- resumeCmd{drain: remaining, park: true}
	timer := time.NewTimer(time.Until(deadline))
	select {
	case <-f.in.paused:
		timer.Stop()
	case <-f.in.finished:
		timer.Stop()
		return cut("draining the old connection", errors.New("the flow ended"))
	case <-timer.C:
		return cut("draining the old connection", errors.New("timed out"))
	}

	// 4. Confirm that the old connection is drained, and wait for the other end to
	// confirm the same.
	if _, err := t2.Conn.Write([]byte{swapAck}); err != nil {
		f.in.resume <- resumeCmd{}
		return cut("confirming", err)
	}
	var ack [1]byte
	if _, err := io.ReadFull(rr, ack[:]); err != nil || ack[0] != swapAck {
		f.in.resume <- resumeCmd{}
		return cut("waiting for the other end's confirmation", err)
	}

	// 5. Commit: both pumps carry on over t2, the old connection is closed.
	_ = t2.Conn.SetDeadline(time.Time{})
	t2.Left = rr.left
	f.setTun(t2)
	f.out.resume <- resumeCmd{tun: t2}
	f.in.resume <- resumeCmd{tun: t2}
	t1.Conn.Close()
	f.swaps.Add(1)
	return nil
}
