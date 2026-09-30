package transport

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	dnsx "github.com/musix/backhaul/internal/transport/dns"
	"github.com/musix/backhaul/internal/transport/dns/rel"
	"github.com/musix/backhaul/internal/utils"
	"github.com/musix/backhaul/internal/utils/handlers"
	"github.com/musix/backhaul/internal/web"

	"github.com/sirupsen/logrus"
	"github.com/xtaci/smux"
)

// DnsMuxConfig configures the dnsmux server: the authoritative end of the DNS
// carrier. The client dials through recursive resolvers; this side accepts the
// resulting tunnel conns, opens smux streams on them and forwards local ports,
// exactly like tcpmux.
type DnsMuxConfig struct {
	Domain           string
	Key              string
	Listen           string
	Token            string
	Ports            []string
	Nodelay          bool
	Sniffer          bool
	WebPort          int
	SnifferLog       string
	TunnelStatus     string
	ProxyProtocol    bool
	MuxVersion       int
	MaxFrameSize     int
	MaxReceiveBuffer int
	MaxStreamBuffer  int
	AuthTimeout      time.Duration // token handshake deadline (default 60s: a DNS round trip is slow)
}

// dnsSession is one registered tunnel: the smux session plus how long its
// carrier has been silent (nil when unknown).
type dnsSession struct {
	mux  *smux.Session
	idle func() time.Duration
}

// staleAfter: a carrier silent this long has lost its client (which polls at
// least every ~0.5s even on a bad path), so new streams must not go there.
const staleAfter = 20 * time.Second

type DnsMuxTransport struct {
	config       *DnsMuxConfig
	smuxConfig   *smux.Config
	ctx          context.Context
	cancel       context.CancelFunc
	logger       *logrus.Logger
	usageMonitor *web.Usage

	mu       sync.Mutex
	sessions []*dnsSession
	next     int
}

func NewDnsMuxServer(parentCtx context.Context, config *DnsMuxConfig, logger *logrus.Logger) *DnsMuxTransport {
	ctx, cancel := context.WithCancel(parentCtx)
	sc := smux.DefaultConfig()
	// One DNS round trip is slow and mux keepalive frames queue behind bulk data,
	// so the default 30s keepalive timeout would kill a healthy but busy session.
	// The carrier has its own liveness handling (RST / idle timeout).
	sc.KeepAliveInterval = 30 * time.Second
	sc.KeepAliveTimeout = 10 * time.Minute
	if config.MuxVersion > 0 {
		sc.Version = config.MuxVersion
	}
	if config.MaxFrameSize > 0 {
		sc.MaxFrameSize = config.MaxFrameSize
	}
	if config.MaxReceiveBuffer > 0 {
		sc.MaxReceiveBuffer = config.MaxReceiveBuffer
	}
	if config.MaxStreamBuffer > 0 {
		sc.MaxStreamBuffer = config.MaxStreamBuffer
	}
	// A tunnel round trip is at least one DNS exchange, so the default 30s
	// keepalive timeout is fine but must not be shortened.
	if config.AuthTimeout <= 0 {
		config.AuthTimeout = 60 * time.Second
	}
	return &DnsMuxTransport{
		config:       config,
		smuxConfig:   sc,
		ctx:          ctx,
		cancel:       cancel,
		logger:       logger,
		usageMonitor: web.NewDataStore(fmt.Sprintf(":%v", config.WebPort), ctx, config.SnifferLog, config.Sniffer, &config.TunnelStatus, logger),
	}
}

func (s *DnsMuxTransport) Start() {
	if s.config.WebPort > 0 {
		go s.usageMonitor.Monitor()
	}
	s.config.TunnelStatus = "Disconnected (DNSMUX)"

	srv := dnsx.NewServer(s.config.Domain, s.config.Key, s.logger)
	// Real DNS round trips take 0.1-1.5s and vary widely; a 300ms minimum RTO
	// would retransmit most segments spuriously and waste the scarce capacity.
	srv.SetRel(rel.Config{MinRTO: time.Second, MaxRTO: 4 * time.Second, MaxInflight: 32 * 1024})
	go func() {
		<-s.ctx.Done()
		srv.Close()
	}()
	go func() {
		if err := srv.Serve(s.ctx, s.config.Listen); err != nil && s.ctx.Err() == nil {
			s.logger.Fatalf("dns responder on %s: %v", s.config.Listen, err)
		}
	}()
	s.logger.Infof("dnsmux: authoritative responder for %s listening on %s", s.config.Domain, s.config.Listen)

	go s.acceptLoop(srv)
	s.startListeners()
}

// Close stops the transport.
func (s *DnsMuxTransport) Close() { s.cancel() }

func (s *DnsMuxTransport) acceptLoop(srv *dnsx.Server) {
	for {
		conn, err := srv.Accept()
		if err != nil {
			return
		}
		go s.handshake(conn)
	}
}

// handshake authenticates a fresh tunnel conn with the shared token and turns it
// into a smux session (this side opens streams, the client accepts them).
func (s *DnsMuxTransport) handshake(conn net.Conn) {
	_ = conn.SetDeadline(time.Now().Add(s.config.AuthTimeout))
	if err := dnsx.ServerAuth(conn, s.config.Token); err != nil {
		s.logger.Warnf("dnsmux: rejected tunnel conn: %v", err)
		conn.Close()
		return
	}
	_ = conn.SetDeadline(time.Time{})

	session, err := smux.Client(conn, s.smuxConfig)
	if err != nil {
		s.logger.Errorf("dnsmux: failed to create mux session: %v", err)
		conn.Close()
		return
	}

	ds := &dnsSession{mux: session}
	if ic, ok := conn.(interface{ IdleFor() time.Duration }); ok {
		ds.idle = ic.IdleFor
	}
	s.mu.Lock()
	s.sessions = append(s.sessions, ds)
	n := len(s.sessions)
	s.config.TunnelStatus = "Connected (DNSMUX)" // under mu: serialized with removal below
	s.mu.Unlock()
	s.logger.Infof("dnsmux: tunnel session established (%d active)", n)

	// Keep the session registered until it dies.
	select {
	case <-session.CloseChan():
	case <-s.ctx.Done():
	}
	session.Close()
	s.mu.Lock()
	for i, x := range s.sessions {
		if x == ds {
			s.sessions = append(s.sessions[:i], s.sessions[i+1:]...)
			break
		}
	}
	remaining := len(s.sessions)
	if remaining == 0 {
		s.config.TunnelStatus = "Disconnected (DNSMUX)"
	}
	s.mu.Unlock()
	s.logger.Warnf("dnsmux: tunnel session closed (%d active)", remaining)
}

// pick returns a live session, round-robin, skipping closed ones and ones whose
// client has gone silent (a dead tunnel lingers until the carrier's idle GC); nil
// when there is none, so the caller rejects the connection promptly.
func (s *DnsMuxTransport) pick() *smux.Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	for range s.sessions {
		s.next = (s.next + 1) % len(s.sessions)
		ds := s.sessions[s.next]
		if ds.mux.IsClosed() {
			continue
		}
		if ds.idle != nil && ds.idle() > staleAfter {
			continue
		}
		return ds.mux
	}
	return nil
}

type dnsPortMap struct{ local, remote string }

// expandPorts turns the tcpmux-style "ports" entries ("8080", "8080=host:80",
// "1000-1010", "ip:8080=remote") into listener/target pairs.
func expandPorts(ports []string) ([]dnsPortMap, error) {
	var out []dnsPortMap
	for _, entry := range ports {
		local, remote, hasRemote := strings.Cut(strings.TrimSpace(entry), "=")
		local, remote = strings.TrimSpace(local), strings.TrimSpace(remote)

		if lo, hi, isRange := strings.Cut(local, "-"); isRange {
			a, errA := strconv.Atoi(strings.TrimSpace(lo))
			b, errB := strconv.Atoi(strings.TrimSpace(hi))
			if errA != nil || errB != nil || a < 1 || b > 65535 || b < a {
				return nil, fmt.Errorf("invalid port range %q", entry)
			}
			for p := a; p <= b; p++ {
				r := remote
				if !hasRemote {
					r = strconv.Itoa(p)
				}
				out = append(out, dnsPortMap{fmt.Sprintf(":%d", p), r})
			}
			continue
		}

		if p, err := strconv.Atoi(local); err == nil {
			if p < 1 || p > 65535 {
				return nil, fmt.Errorf("invalid port %q", entry)
			}
			if !hasRemote {
				remote = local
			}
			out = append(out, dnsPortMap{fmt.Sprintf(":%d", p), remote})
			continue
		}
		if !hasRemote { // "ip:port" alone is not a valid mapping
			return nil, fmt.Errorf("invalid port mapping %q", entry)
		}
		out = append(out, dnsPortMap{local, remote})
	}
	return out, nil
}

func (s *DnsMuxTransport) startListeners() {
	maps, err := expandPorts(s.config.Ports)
	if err != nil {
		s.logger.Fatalf("dnsmux: %v", err)
	}
	for _, m := range maps {
		l, err := net.Listen("tcp", m.local)
		if err != nil {
			s.logger.Fatalf("dnsmux: failed to listen on %s: %v", m.local, err)
		}
		s.logger.Infof("dnsmux: forwarding %s -> %s", l.Addr(), m.remote)
		go func(l net.Listener, remote string) {
			<-s.ctx.Done()
			l.Close()
		}(l, m.remote)
		go s.acceptLocal(l, m.remote)
	}
}

func (s *DnsMuxTransport) acceptLocal(l net.Listener, remote string) {
	for {
		conn := acceptWithBackoff(s.ctx, l, s.logger)
		if conn == nil {
			return
		}
		go s.forward(conn, remote)
	}
}

func (s *DnsMuxTransport) forward(local net.Conn, remote string) {
	session := s.pick()
	if session == nil {
		s.logger.Debugf("dnsmux: no tunnel session, dropping connection from %s", local.RemoteAddr())
		local.Close()
		return
	}
	stream, err := session.OpenStream()
	if err != nil {
		s.logger.Debugf("dnsmux: open stream: %v", err)
		local.Close()
		return
	}
	if err := utils.SendBinaryString(stream, remote); err != nil {
		stream.Close()
		local.Close()
		return
	}
	port := 0
	if a, ok := local.LocalAddr().(*net.TCPAddr); ok {
		port = a.Port
	}
	handlers.TCPConnectionHandler(s.ctx, s.config.ProxyProtocol, local, stream, s.logger, s.usageMonitor, port, s.config.Sniffer)
}
