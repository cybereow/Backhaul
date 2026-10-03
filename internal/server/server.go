// Package server wires a [server] configuration to the transport that serves it.
package server

import (
	"context"
	"net/http"
	_ "net/http/pprof"
	"time"

	"github.com/musix/backhaul/config"
	"github.com/musix/backhaul/internal/server/transport"
	"github.com/musix/backhaul/internal/utils"

	"github.com/sirupsen/logrus"
)

// pprofAddr is loopback-only: pprof serves heap dumps from a process holding the
// tunnel token and TLS keys, so it must never be reachable off-host. Reach it
// through an SSH tunnel if you need it remotely.
const pprofAddr = "127.0.0.1:6060"

// defaultDNSListen is where the authoritative DNS responder binds when
// dns_listen is not set.
const defaultDNSListen = "0.0.0.0:53"

type Server struct {
	config *config.ServerConfig
	ctx    context.Context
	cancel context.CancelFunc
	logger *logrus.Logger
}

func NewServer(cfg *config.ServerConfig, parentCtx context.Context) *Server {
	ctx, cancel := context.WithCancel(parentCtx)
	return &Server{
		config: cfg,
		ctx:    ctx,
		cancel: cancel,
		logger: utils.NewLogger(cfg.LogLevel),
	}
}

// Start launches the configured transport and blocks until the server is stopped.
func (s *Server) Start() {
	if s.config.PPROF {
		go func() {
			s.logger.Infof("pprof started at %s", pprofAddr)
			http.ListenAndServe(pprofAddr, nil)
		}()
	}

	t := s.config.Transport
	switch {
	case t == config.WS || t == config.WSS:
		s.startWS()
	case t == config.WSMUX || t == config.WSSMUX:
		s.startWSMux()
	case t.IsDNS():
		s.startDNS()
	default:
		s.logger.Fatal("invalid transport type: ", t)
	}

	<-s.ctx.Done()
	s.logger.Info("all workers stopped successfully")

	// suppress other logs
	s.logger.SetLevel(logrus.FatalLevel)
}

// Stop shuts down the server gracefully
func (s *Server) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
}

func (s *Server) seconds(n int) time.Duration { return time.Duration(n) * time.Second }

func (s *Server) startWS() {
	c := s.config
	maxConnAge, _ := transport.RotationPlan(s.seconds(c.CDNMaxAge))
	srv := transport.NewWSServer(s.ctx, &transport.WsConfig{
		MaxConnAge:    maxConnAge,
		BindAddr:      c.BindAddr,
		Nodelay:       c.Nodelay,
		KeepAlive:     s.seconds(c.Keepalive),
		Heartbeat:     s.seconds(c.Heartbeat),
		Token:         c.Token,
		ChannelSize:   c.ChannelSize,
		Ports:         c.Ports,
		Sniffer:       c.Sniffer,
		WebPort:       c.WebPort,
		SnifferLog:    c.SnifferLog,
		Mode:          c.Transport,
		TLSCertFile:   c.TLSCertFile,
		TLSKeyFile:    c.TLSKeyFile,
		TLSCerts:      c.TLSCerts,
		TLSKeys:       c.TLSKeys,
		Path:          c.Path,
		Fallback:      c.Fallback,
		ProxyProtocol: c.ProxyProtocol,
	}, s.logger)
	go srv.Start()
}

func (s *Server) startWSMux() {
	c := s.config
	maxConnAge, maxDrain := transport.RotationPlan(s.seconds(c.CDNMaxAge))
	srv := transport.NewWSMuxServer(s.ctx, &transport.WsMuxConfig{
		BindAddr:             c.BindAddr,
		Nodelay:              c.Nodelay,
		KeepAlive:            s.seconds(c.Keepalive),
		Heartbeat:            s.seconds(c.Heartbeat),
		Token:                c.Token,
		ChannelSize:          c.ChannelSize,
		Ports:                c.Ports,
		MuxCon:               c.MuxCon,
		AcceptUDP:            c.AcceptUDP,
		UDPBuffer:            c.UDPBuffer,
		Speedtest:            c.Speedtest,
		MuxVersion:           c.MuxVersion,
		MaxFrameSize:         c.MaxFrameSize,
		MaxReceiveBuffer:     c.MaxReceiveBuffer,
		MaxStreamBuffer:      c.MaxStreamBuffer,
		MuxKeepaliveDisabled: c.MuxKeepaliveDisabled,
		StripeFactor:         c.StripeFactor,
		StripeParity:         c.StripeParity,
		StripePorts:          c.StripePorts,
		PromoteBytes:         c.PromoteBytes,
		SO_RCVBUF:            c.SO_RCVBUF,
		SO_SNDBUF:            c.SO_SNDBUF,
		Sniffer:              c.Sniffer,
		WebPort:              c.WebPort,
		SnifferLog:           c.SnifferLog,
		Mode:                 c.Transport,
		TLSCertFile:          c.TLSCertFile,
		TLSKeyFile:           c.TLSKeyFile,
		TLSCerts:             c.TLSCerts,
		TLSKeys:              c.TLSKeys,
		ProxyProtocol:        c.ProxyProtocol,
		Path:                 c.Path,
		Fallback:             c.Fallback,
		MaxConnAge:           maxConnAge,
		MaxDrain:             maxDrain,
		ResumeWindow:         s.seconds(c.ResumeWindow),
		WSFraming:            c.MuxWSFraming,
		HalfClose:            c.MuxHalfClose,
	}, s.logger)
	go srv.Start()
}

func (s *Server) startDNS() {
	c := s.config
	key := c.DNSKey
	if key == "" {
		key = c.Token
	}
	listen := c.DNSListen
	if listen == "" {
		listen = defaultDNSListen
	}
	srv := transport.NewDnsMuxServer(s.ctx, &transport.DnsMuxConfig{
		Domain:           c.DNSDomain,
		Key:              key,
		Listen:           listen,
		Token:            c.Token,
		Ports:            c.Ports,
		Nodelay:          c.Nodelay,
		Sniffer:          c.Sniffer,
		WebPort:          c.WebPort,
		SnifferLog:       c.SnifferLog,
		ProxyProtocol:    c.ProxyProtocol,
		MuxVersion:       c.MuxVersion,
		MaxFrameSize:     c.MaxFrameSize,
		MaxReceiveBuffer: c.MaxReceiveBuffer,
		MaxStreamBuffer:  c.MaxStreamBuffer,
	}, s.logger)
	go srv.Start()
}
