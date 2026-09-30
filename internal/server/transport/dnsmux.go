package transport

import (
	"context"
	"runtime"
	"time"

	dnsx "github.com/musix/backhaul/internal/transport/dns"
	"github.com/musix/backhaul/internal/transport/dns/rel"
	"github.com/musix/backhaul/internal/utils"
	"github.com/sirupsen/logrus"
	"github.com/xtaci/smux"
)

// DNSMuxConfig adds DNS carrier settings to the ordinary TCP mux settings.
type DNSMuxConfig struct {
	TcpMuxConfig
	Domain string
	Key    string
	Listen string
	Rel    rel.Config
}

// DNSMuxTransport runs the TCP mux forwarding machinery over reliable DNS
// carrier connections instead of TCP connections.
type DNSMuxTransport struct {
	*TcpMuxTransport
	dnsConfig *DNSMuxConfig
	carrier   *dnsx.Server
}

func NewDNSMuxServer(parent context.Context, cfg *DNSMuxConfig, logger *logrus.Logger) *DNSMuxTransport {
	if cfg.Rel.RecvBuf == 0 {
		cfg.Rel.RecvBuf = 256 << 10
	}
	if cfg.Rel.SendBuf == 0 {
		cfg.Rel.SendBuf = 256 << 10
	}
	if cfg.Rel.MaxInflight == 0 {
		cfg.Rel.MaxInflight = 65535
	}
	base := NewTcpMuxServer(parent, &cfg.TcpMuxConfig, logger)
	base.smuxConfig.KeepAliveTimeout = 2 * time.Minute
	carrier := dnsx.NewServer(cfg.Domain, cfg.Key, logger)
	carrier.SetRelConfig(cfg.Rel)
	return &DNSMuxTransport{TcpMuxTransport: base, dnsConfig: cfg, carrier: carrier}
}

func (s *DNSMuxTransport) Start() {
	if s.config.WebPort > 0 {
		go s.usageMonitor.Monitor()
	}
	s.config.TunnelStatus = "Disconnected (DNSMux)"
	go func() {
		if err := s.carrier.Serve(s.ctx, s.dnsConfig.Listen); err != nil && s.ctx.Err() == nil {
			s.logger.Errorf("DNS carrier listener: %v", err)
		}
		_ = s.carrier.Close()
	}()
	go s.acceptDNSConnections()

	s.dnsChannelHandshake()
	if s.controlChannel == nil {
		return
	}
	s.config.TunnelStatus = "Connected (DNSMux)"
	go s.parsePortMappings()
	go s.channelHandler()
	n := runtime.NumCPU()
	if n > 4 {
		n = 4
	}
	for i := 0; i < n; i++ {
		go s.handleLoop()
	}
}

func (s *DNSMuxTransport) acceptDNSConnections() {
	defer s.carrier.Close()
	for {
		conn, err := s.carrier.Accept()
		if err != nil {
			return
		}
		if s.controlChannel == nil {
			select {
			case s.handshakeChannel <- conn:
			default:
				conn.Close()
			}
			continue
		}
		session, err := smux.Client(conn, s.smuxConfig)
		if err != nil {
			conn.Close()
			continue
		}
		select {
		case s.tunnelChannel <- session:
		default:
			session.Close()
		}
	}
}

func (s *DNSMuxTransport) dnsChannelHandshake() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case conn := <-s.handshakeChannel:
			_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
			msg, signal, err := utils.ReceiveBinaryTransportString(conn)
			if err != nil || signal != utils.SG_Chan || msg != s.config.Token {
				conn.Close()
				continue
			}
			if err := utils.SendBinaryTransportString(conn, s.config.Token, utils.SG_Chan); err != nil {
				conn.Close()
				continue
			}
			_ = conn.SetReadDeadline(time.Time{})
			s.controlChannel = conn
			return
		}
	}
}
