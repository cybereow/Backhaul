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

func (s *Server) Start() {
	// for pprof and debugging. Bind to loopback only: pprof serves heap dumps
	// from a process holding the tunnel token and TLS keys, so it must never be
	// reachable off-host. Reach it via an SSH tunnel if you need it remotely.
	if s.config.PPROF {
		go func() {
			s.logger.Info("pprof started at 127.0.0.1:6060")
			http.ListenAndServe("127.0.0.1:6060", nil)
		}()
	}

	switch s.config.Transport {
	case config.TCP:
		tcpConfig := &transport.TcpConfig{
			BindAddr:      s.config.BindAddr,
			Nodelay:       s.config.Nodelay,
			KeepAlive:     time.Duration(s.config.Keepalive) * time.Second,
			Heartbeat:     time.Duration(s.config.Heartbeat) * time.Second,
			Token:         s.config.Token,
			ChannelSize:   s.config.ChannelSize,
			Ports:         s.config.Ports,
			Sniffer:       s.config.Sniffer,
			WebPort:       s.config.WebPort,
			SnifferLog:    s.config.SnifferLog,
			AcceptUDP:     s.config.AcceptUDP,
			MSS:           s.config.MSS,
			SO_RCVBUF:     s.config.SO_RCVBUF,
			SO_SNDBUF:     s.config.SO_SNDBUF,
			ProxyProtocol: s.config.ProxyProtocol,
		}

		tcpServer := transport.NewTCPServer(s.ctx, tcpConfig, s.logger)
		go tcpServer.Start()

	case config.TCPMUX:
		tcpMuxConfig := &transport.TcpMuxConfig{
			BindAddr:         s.config.BindAddr,
			Nodelay:          s.config.Nodelay,
			KeepAlive:        time.Duration(s.config.Keepalive) * time.Second,
			Heartbeat:        time.Duration(s.config.Heartbeat) * time.Second,
			Token:            s.config.Token,
			ChannelSize:      s.config.ChannelSize,
			Ports:            s.config.Ports,
			MuxCon:           s.config.MuxCon,
			MuxVersion:       s.config.MuxVersion,
			MaxFrameSize:     s.config.MaxFrameSize,
			MaxReceiveBuffer: s.config.MaxReceiveBuffer,
			MaxStreamBuffer:  s.config.MaxStreamBuffer,
			Sniffer:          s.config.Sniffer,
			WebPort:          s.config.WebPort,
			SnifferLog:       s.config.SnifferLog,
			MSS:              s.config.MSS,
			SO_RCVBUF:        s.config.SO_RCVBUF,
			SO_SNDBUF:        s.config.SO_SNDBUF,
			ProxyProtocol:    s.config.ProxyProtocol,
		}

		tcpMuxServer := transport.NewTcpMuxServer(s.ctx, tcpMuxConfig, s.logger)
		go tcpMuxServer.Start()

	case config.WS, config.WSS:
		wsConfig := &transport.WsConfig{
			BindAddr:      s.config.BindAddr,
			Nodelay:       s.config.Nodelay,
			KeepAlive:     time.Duration(s.config.Keepalive) * time.Second,
			Heartbeat:     time.Duration(s.config.Heartbeat) * time.Second,
			Token:         s.config.Token,
			ChannelSize:   s.config.ChannelSize,
			Ports:         s.config.Ports,
			Sniffer:       s.config.Sniffer,
			WebPort:       s.config.WebPort,
			SnifferLog:    s.config.SnifferLog,
			Mode:          s.config.Transport,
			TLSCertFile:   s.config.TLSCertFile,
			TLSKeyFile:    s.config.TLSKeyFile,
			TLSCerts:      s.config.TLSCerts,
			TLSKeys:       s.config.TLSKeys,
			Path:          s.config.Path,
			Fallback:      s.config.Fallback,
			TLSEngine:     s.config.TLSEngine,
			ProxyProtocol: s.config.ProxyProtocol,
		}

		wsServer := transport.NewWSServer(s.ctx, wsConfig, s.logger)
		go wsServer.Start()

	case config.WSMUX, config.WSSMUX:
		wsMuxConfig := &transport.WsMuxConfig{
			BindAddr:             s.config.BindAddr,
			Nodelay:              s.config.Nodelay,
			KeepAlive:            time.Duration(s.config.Keepalive) * time.Second,
			Heartbeat:            time.Duration(s.config.Heartbeat) * time.Second,
			Token:                s.config.Token,
			ChannelSize:          s.config.ChannelSize,
			Ports:                s.config.Ports,
			MuxCon:               s.config.MuxCon,
			AcceptUDP:            s.config.AcceptUDP,
			MuxVersion:           s.config.MuxVersion,
			MaxFrameSize:         s.config.MaxFrameSize,
			MaxReceiveBuffer:     s.config.MaxReceiveBuffer,
			MaxStreamBuffer:      s.config.MaxStreamBuffer,
			MuxKeepaliveDisabled: s.config.MuxKeepaliveDisabled,
			StripeFactor:         s.config.StripeFactor,
			StripeParity:         s.config.StripeParity,
			StripePorts:          s.config.StripePorts,
			PromoteBytes:         s.config.PromoteBytes,
			Sniffer:              s.config.Sniffer,
			WebPort:              s.config.WebPort,
			SnifferLog:           s.config.SnifferLog,
			Mode:                 s.config.Transport,
			TLSCertFile:          s.config.TLSCertFile,
			TLSKeyFile:           s.config.TLSKeyFile,
			TLSCerts:             s.config.TLSCerts,
			TLSKeys:              s.config.TLSKeys,
			ProxyProtocol:        s.config.ProxyProtocol,
			Path:                 s.config.Path,
			Fallback:             s.config.Fallback,
			TLSEngine:            s.config.TLSEngine,
			MaxConnAge:           time.Duration(s.config.MaxConnAge) * time.Second,
		}

		wsMuxServer := transport.NewWSMuxServer(s.ctx, wsMuxConfig, s.logger)
		go wsMuxServer.Start()

	case config.UDP:
		udpConfig := &transport.UdpConfig{
			BindAddr:    s.config.BindAddr,
			Heartbeat:   time.Duration(s.config.Heartbeat) * time.Second,
			Token:       s.config.Token,
			ChannelSize: s.config.ChannelSize,
			Ports:       s.config.Ports,
			Sniffer:     s.config.Sniffer,
			WebPort:     s.config.WebPort,
			SnifferLog:  s.config.SnifferLog,
		}

		udpServer := transport.NewUDPServer(s.ctx, udpConfig, s.logger)
		go udpServer.Start()

	default:
		s.logger.Fatal("invalid transport type: ", s.config.Transport)
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
