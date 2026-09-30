package dnsx

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/musix/backhaul/internal/transport/dns/rel"
	"github.com/sirupsen/logrus"
)

const (
	sessionIdle = 60 * time.Second
	gcInterval  = 10 * time.Second
	acceptQueue = 64
	// maxSegment is the largest server->client rel segment (data bytes): base32
	// of it plus a 255-byte question still fits a 1232-byte EDNS response.
	maxSegment   = 400
	errClosedStr = "dnsx: server closed"
)

var errServerClosed = errors.New(errClosedStr)

// Server is the inside (authoritative) end of the DNS carrier. It owns a
// Responder whose Handler demultiplexes queries into per-sid sessions; each new
// sid is handed out once via Accept as a net.Conn.
type Server struct {
	responder *Responder
	// relCfg seeds each session's rel.Endpoint (MSS is set per exchange). Set it
	// before the first query arrives.
	relCfg rel.Config

	mu       sync.Mutex
	sessions map[uint32]*serverSession
	acceptCh chan net.Conn
	done     chan struct{}
	once     sync.Once
}

// NewServer builds a carrier server for domain. relCfg zero values take the rel
// defaults (MSS is set per exchange).
func NewServer(domain, key string, logger *logrus.Logger) *Server {
	s := &Server{
		sessions: make(map[uint32]*serverSession),
		acceptCh: make(chan net.Conn, acceptQueue),
		done:     make(chan struct{}),
	}
	s.responder = NewResponder(domain, key, logger)
	s.responder.Handler = s.handle
	go s.gcLoop()
	return s
}

// SetRel sets the rel parameters (RTO bounds, buffers) for sessions created from
// now on; call it before Serve.
func (s *Server) SetRel(cfg rel.Config) { s.relCfg = cfg }

// Serve runs the DNS listener (UDP+TCP) until ctx ends.
func (s *Server) Serve(ctx context.Context, addr string) error {
	// Close must stop the listener too, not only Accept and the GC loop.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-s.done:
			cancel()
		case <-ctx.Done():
		}
	}()
	return s.responder.Serve(ctx, addr)
}

// Accept returns the next new session. The conn's RemoteAddr is the recursive
// resolver's, not the client's, and is not meaningful.
func (s *Server) Accept() (net.Conn, error) {
	select {
	case c := <-s.acceptCh:
		return c, nil
	case <-s.done:
		return nil, errServerClosed
	}
}

// Close stops the server and resets every session.
func (s *Server) Close() error {
	s.once.Do(func() {
		close(s.done)
		s.mu.Lock()
		for _, ss := range s.sessions {
			ss.fail(errServerClosed)
		}
		s.sessions = make(map[uint32]*serverSession)
		s.mu.Unlock()
	})
	return nil
}

func (s *Server) gcLoop() {
	t := time.NewTicker(gcInterval)
	defer t.Stop()
	for {
		select {
		case <-s.done:
			return
		case now := <-t.C:
			s.mu.Lock()
			for sid, ss := range s.sessions {
				if ss.idleSince(now) > sessionIdle {
					ss.fail(io.ErrUnexpectedEOF)
					delete(s.sessions, sid)
				}
			}
			s.mu.Unlock()
		}
	}
}

// handle is the Responder.Handler: one query in, one response packet out.
func (s *Server) handle(sid uint32, flags byte, in []byte, maxResp int) ([]byte, byte) {
	pk, err := rel.Unmarshal(in)
	if err != nil {
		return nil, 0
	}
	now := time.Now()
	s.responder.logger.Tracef("dns session %08x: flags=%d seq=%d ack=%d wnd=%d data=%d maxResp=%d", sid, flags, pk.Seq, pk.Ack, pk.Wnd, len(pk.Data), maxResp)

	select {
	case <-s.done:
		return nil, FlagRST // closed: create nothing new
	default:
	}

	s.mu.Lock()
	ss := s.sessions[sid]
	if ss == nil {
		// Only a SYN creates a session; anything else for an unknown sid means it
		// expired (or the server restarted): reset it.
		if flags&FlagSYN == 0 || flags&(FlagFIN|FlagRST) != 0 {
			s.mu.Unlock()
			return nil, FlagRST
		}
		ss = newServerSession(sid, now, s.relCfg)
		select {
		case s.acceptCh <- ss:
			s.sessions[sid] = ss
		default: // accept queue full: drop, the client retries
			s.mu.Unlock()
			return nil, 0
		}
	}
	s.mu.Unlock()

	return ss.exchange(now, flags, pk, maxResp)
}

// serverSession is one carrier connection on the inside; it implements net.Conn.
type serverSession struct {
	sid uint32

	mu   sync.Mutex
	cond *sync.Cond
	ep   *rel.Endpoint

	last      time.Time
	remoteFIN bool // peer closed its half
	closed    bool // local Close
	err       error

	readDeadline  time.Time
	writeDeadline time.Time
}

func newServerSession(sid uint32, now time.Time, cfg rel.Config) *serverSession {
	ss := &serverSession{sid: sid, ep: rel.New(cfg), last: now}
	ss.cond = sync.NewCond(&ss.mu)
	return ss
}

func (ss *serverSession) idleSince(now time.Time) time.Duration {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return now.Sub(ss.last)
}

// IdleFor is how long ago the client last queried this session. A live client
// polls at least every ~0.5s, so a long idle time means the client is gone.
func (ss *serverSession) IdleFor() time.Duration { return ss.idleSince(time.Now()) }

func (ss *serverSession) fail(err error) {
	ss.mu.Lock()
	if ss.err == nil {
		ss.err = err
	}
	ss.cond.Broadcast()
	ss.mu.Unlock()
}

func (ss *serverSession) exchange(now time.Time, flags byte, pk rel.Packet, maxResp int) ([]byte, byte) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.last = now

	ss.ep.Recv(now, pk)
	if flags&FlagFIN != 0 {
		ss.remoteFIN = true
	}
	// The client picks the RR type per query, so the response budget varies.
	// The reply echoes the query's QNAME in its question section, so a segment
	// that fits an empty poll can overflow the client's EDNS size when the query
	// carried data - and a retransmission can never be shrunk, so it would fail
	// forever. Cap every segment to what fits even beside a maximum-size QNAME.
	if mss := maxResp - rel.HeaderLen; mss > 0 {
		if mss > maxSegment {
			mss = maxSegment
		}
		ss.ep.SetMSS(mss)
	}
	out := ss.ep.Next(now)
	ss.cond.Broadcast()

	var outFlags byte
	if ss.closed && ss.ep.Pending() == 0 {
		outFlags |= FlagFIN
	}
	return out.Marshal(), outFlags
}

func (ss *serverSession) Read(b []byte) (int, error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	for {
		if n := ss.ep.Read(b); n > 0 {
			ss.cond.Broadcast()
			return n, nil
		}
		if ss.remoteFIN {
			return 0, io.EOF
		}
		if ss.err != nil {
			return 0, ss.err
		}
		if ss.closed {
			return 0, io.ErrClosedPipe
		}
		if err := ss.wait(ss.readDeadline); err != nil {
			return 0, err
		}
	}
}

func (ss *serverSession) Write(b []byte) (int, error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	written := 0
	for len(b) > 0 {
		if ss.err != nil {
			return written, ss.err
		}
		if ss.closed {
			return written, io.ErrClosedPipe
		}
		n := ss.ep.Write(b)
		b, written = b[n:], written+n
		if len(b) == 0 {
			break
		}
		if err := ss.wait(ss.writeDeadline); err != nil {
			return written, err
		}
	}
	return written, nil
}

// wait parks on the cond until signalled or the deadline passes. mu is held.
func (ss *serverSession) wait(deadline time.Time) error {
	var t *time.Timer
	if !deadline.IsZero() {
		d := time.Until(deadline)
		if d <= 0 {
			return timeoutError{}
		}
		t = time.AfterFunc(d, func() {
			ss.mu.Lock()
			ss.cond.Broadcast()
			ss.mu.Unlock()
		})
	}
	ss.cond.Wait()
	if t != nil {
		t.Stop()
	}
	return nil
}

// Close marks the local half closed; the FIN goes out on the next responses once
// everything written has been acked.
func (ss *serverSession) Close() error {
	ss.mu.Lock()
	ss.closed = true
	ss.cond.Broadcast()
	ss.mu.Unlock()
	return nil
}

func (ss *serverSession) LocalAddr() net.Addr  { return &net.IPAddr{IP: net.IPv4zero} }
func (ss *serverSession) RemoteAddr() net.Addr { return &net.IPAddr{IP: net.IPv4zero} }

func (ss *serverSession) SetDeadline(t time.Time) error {
	ss.SetReadDeadline(t)
	return ss.SetWriteDeadline(t)
}

func (ss *serverSession) SetReadDeadline(t time.Time) error {
	ss.mu.Lock()
	ss.readDeadline = t
	ss.cond.Broadcast()
	ss.mu.Unlock()
	return nil
}

func (ss *serverSession) SetWriteDeadline(t time.Time) error {
	ss.mu.Lock()
	ss.writeDeadline = t
	ss.cond.Broadcast()
	ss.mu.Unlock()
	return nil
}
