package client

import (
	"context"
	"strings"
	"time"

	"github.com/musix/backhaul/internal/utils"

	"github.com/musix/backhaul/config"

	"github.com/musix/backhaul/internal/client/transport"

	"net/http"
	_ "net/http/pprof"

	"github.com/sirupsen/logrus"
)

// Client encapsulates the client configuration and state
type Client struct {
	config *config.ClientConfig
	ctx    context.Context
	cancel context.CancelFunc
	logger *logrus.Logger
}

func NewClient(cfg *config.ClientConfig, parentCtx context.Context) *Client {
	ctx, cancel := context.WithCancel(parentCtx)
	return &Client{
		config: cfg,
		ctx:    ctx,
		cancel: cancel,
		logger: utils.NewLogger(cfg.LogLevel),
	}
}

// Run starts the client and begins dialing the tunnel server
func (c *Client) Start() {
	// for pprof. Bind to loopback only: pprof serves heap dumps from a process
	// holding the tunnel token and TLS keys, so it must never be reachable
	// off-host. Reach it via an SSH tunnel if you need it remotely.
	if c.config.PPROF {
		go func() {
			c.logger.Info("pprof started at 127.0.0.1:6061")
			http.ListenAndServe("127.0.0.1:6061", nil)
		}()
	}

	remoteDisplay := c.config.RemoteAddr
	if remoteDisplay == "" && len(c.config.RemoteAddrs) > 0 {
		remoteDisplay = strings.Join(c.config.RemoteAddrs, ", ")
	}
	c.logger.Infof("client with remote address %s started successfully", remoteDisplay)

	switch c.config.Transport {
	case config.TCP:
		tcpConfig := &transport.TcpConfig{
			RemoteAddr:     c.config.RemoteAddr,
			Nodelay:        c.config.Nodelay,
			KeepAlive:      time.Duration(c.config.Keepalive) * time.Second,
			RetryInterval:  time.Duration(c.config.RetryInterval) * time.Second,
			DialTimeOut:    time.Duration(c.config.DialTimeout) * time.Second,
			ConnPoolSize:   c.config.ConnectionPool,
			Token:          c.config.Token,
			Sniffer:        c.config.Sniffer,
			WebPort:        c.config.WebPort,
			SnifferLog:     c.config.SnifferLog,
			AggressivePool: c.config.AggressivePool,
			MSS:            c.config.MSS,
			SO_RCVBUF:      c.config.SO_RCVBUF,
			SO_SNDBUF:      c.config.SO_SNDBUF,
		}
		tcpClient := transport.NewTCPClient(c.ctx, tcpConfig, c.logger)
		go tcpClient.Start()

	case config.DNS:
		dnsConfig := &transport.DnsConfig{
			RemoteAddr:     c.config.RemoteAddr,
			DNSDomain:      c.config.DNSDomain,
			Nodelay:        c.config.Nodelay,
			KeepAlive:      time.Duration(c.config.Keepalive) * time.Second,
			RetryInterval:  time.Duration(c.config.RetryInterval) * time.Second,
			DialTimeOut:    time.Duration(c.config.DialTimeout) * time.Second,
			ConnPoolSize:   c.config.ConnectionPool,
			Token:          c.config.Token,
			Sniffer:        c.config.Sniffer,
			WebPort:        c.config.WebPort,
			SnifferLog:     c.config.SnifferLog,
			AggressivePool: c.config.AggressivePool,
			MSS:            c.config.MSS,
			SO_RCVBUF:      c.config.SO_RCVBUF,
			SO_SNDBUF:      c.config.SO_SNDBUF,
		}
		dnsClient := transport.NewDNSClient(c.ctx, dnsConfig, c.logger)
		go dnsClient.Start()

	case config.TCPMUX:
		tcpMuxConfig := &transport.TcpMuxConfig{
			RemoteAddr:       c.config.RemoteAddr,
			Nodelay:          c.config.Nodelay,
			KeepAlive:        time.Duration(c.config.Keepalive) * time.Second,
			RetryInterval:    time.Duration(c.config.RetryInterval) * time.Second,
			DialTimeOut:      time.Duration(c.config.DialTimeout) * time.Second,
			ConnPoolSize:     c.config.ConnectionPool,
			Token:            c.config.Token,
			MuxVersion:       c.config.MuxVersion,
			MaxFrameSize:     c.config.MaxFrameSize,
			MaxReceiveBuffer: c.config.MaxReceiveBuffer,
			MaxStreamBuffer:  c.config.MaxStreamBuffer,
			Sniffer:          c.config.Sniffer,
			WebPort:          c.config.WebPort,
			SnifferLog:       c.config.SnifferLog,
			AggressivePool:   c.config.AggressivePool,
			MSS:              c.config.MSS,
			SO_RCVBUF:        c.config.SO_RCVBUF,
			SO_SNDBUF:        c.config.SO_SNDBUF,
		}
		tcpMuxClient := transport.NewMuxClient(c.ctx, tcpMuxConfig, c.logger)
		go tcpMuxClient.Start()

	case config.WS, config.WSS:
		WsConfig := &transport.WsConfig{
			RemoteAddr:     c.config.RemoteAddr,
			RemoteAddrs:    c.config.RemoteAddrs,
			EdgeIPs:        c.config.EdgeIPs,
			Nodelay:        c.config.Nodelay,
			KeepAlive:      time.Duration(c.config.Keepalive) * time.Second,
			RetryInterval:  time.Duration(c.config.RetryInterval) * time.Second,
			DialTimeOut:    time.Duration(c.config.DialTimeout) * time.Second,
			ConnPoolSize:   c.config.ConnectionPool,
			Token:          c.config.Token,
			Sniffer:        c.config.Sniffer,
			WebPort:        c.config.WebPort,
			SnifferLog:     c.config.SnifferLog,
			Mode:           c.config.Transport,
			AggressivePool: c.config.AggressivePool,
			EdgeIP:         c.config.EdgeIP,
			Path:           c.config.Path,
			SO_RCVBUF:      c.config.SO_RCVBUF,
			SO_SNDBUF:      c.config.SO_SNDBUF,
			MSS:            c.config.MSS,
			TLSVerify:      c.config.TLSVerify,
		}
		if c.config.Transport == config.WSS && !c.config.TLSVerify {
			c.logger.Warn("SECURITY: wss server certificate verification is OFF (tls_verify=false); the auth token can be harvested by an on-path party via TLS MITM. Set tls_verify=true once the server presents a verifiable certificate.")
		}
		WsClient := transport.NewWSClient(c.ctx, WsConfig, c.logger)
		go WsClient.Start()

	case config.WSMUX, config.WSSMUX:
		wsMuxConfig := &transport.WsMuxConfig{
			RemoteAddr:           c.config.RemoteAddr,
			RemoteAddrs:          c.config.RemoteAddrs,
			EdgeIPs:              c.config.EdgeIPs,
			Nodelay:              c.config.Nodelay,
			KeepAlive:            time.Duration(c.config.Keepalive) * time.Second,
			RetryInterval:        time.Duration(c.config.RetryInterval) * time.Second,
			DialTimeOut:          time.Duration(c.config.DialTimeout) * time.Second,
			ConnPoolSize:         c.config.ConnectionPool,
			Token:                c.config.Token,
			MuxVersion:           c.config.MuxVersion,
			MaxFrameSize:         c.config.MaxFrameSize,
			MaxReceiveBuffer:     c.config.MaxReceiveBuffer,
			MaxStreamBuffer:      c.config.MaxStreamBuffer,
			MuxKeepaliveDisabled: c.config.MuxKeepaliveDisabled,
			StripeFactor:         c.config.StripeFactor,
			StripeParity:         c.config.StripeParity,
			SO_RCVBUF:            c.config.SO_RCVBUF,
			SO_SNDBUF:            c.config.SO_SNDBUF,
			MSS:                  c.config.MSS,
			Sniffer:              c.config.Sniffer,
			WebPort:              c.config.WebPort,
			SnifferLog:           c.config.SnifferLog,
			Mode:                 c.config.Transport,
			AggressivePool:       c.config.AggressivePool,
			EdgeIP:               c.config.EdgeIP,
			Path:                 c.config.Path,
			TLSVerify:            c.config.TLSVerify,
			WSFraming:            c.config.MuxWSFraming,
		}
		if c.config.Transport == config.WSSMUX && !c.config.TLSVerify {
			c.logger.Warn("SECURITY: wssmux server certificate verification is OFF (tls_verify=false); the auth token can be harvested by an on-path party via TLS MITM. Set tls_verify=true once the server presents a verifiable certificate.")
		}
		wsMuxClient := transport.NewWSMuxClient(c.ctx, wsMuxConfig, c.logger)
		go wsMuxClient.Start()

	case config.DNSMUX:
		// Re-using WSMuxClient but theoretically we'd construct DnsMuxConfig similar to WsMuxConfig
		// Let's implement DNSMUX as just WsMuxConfig with DNS transport mode
		wsMuxConfig := &transport.WsMuxConfig{
			RemoteAddr:           c.config.RemoteAddr,
			RemoteAddrs:          c.config.RemoteAddrs,
			EdgeIPs:              c.config.EdgeIPs,
			Nodelay:              c.config.Nodelay,
			KeepAlive:            time.Duration(c.config.Keepalive) * time.Second,
			RetryInterval:        time.Duration(c.config.RetryInterval) * time.Second,
			DialTimeOut:          time.Duration(c.config.DialTimeout) * time.Second,
			ConnPoolSize:         c.config.ConnectionPool,
			Token:                c.config.Token,
			MuxVersion:           c.config.MuxVersion,
			MaxFrameSize:         c.config.MaxFrameSize,
			MaxReceiveBuffer:     c.config.MaxReceiveBuffer,
			MaxStreamBuffer:      c.config.MaxStreamBuffer,
			MuxKeepaliveDisabled: c.config.MuxKeepaliveDisabled,
			StripeFactor:         c.config.StripeFactor,
			StripeParity:         c.config.StripeParity,
			SO_RCVBUF:            c.config.SO_RCVBUF,
			SO_SNDBUF:            c.config.SO_SNDBUF,
			MSS:                  c.config.MSS,
			Sniffer:              c.config.Sniffer,
			WebPort:              c.config.WebPort,
			SnifferLog:           c.config.SnifferLog,
			Mode:                 c.config.Transport,
			AggressivePool:       c.config.AggressivePool,
			EdgeIP:               c.config.EdgeIP,
			Path:                 c.config.Path,
			TLSVerify:            c.config.TLSVerify,
			WSFraming:            c.config.MuxWSFraming,
		}

		wsMuxClient := transport.NewWSMuxClient(c.ctx, wsMuxConfig, c.logger)
		go wsMuxClient.Start()

	case config.UDP:
		udpConfig := &transport.UdpConfig{
			RemoteAddr:     c.config.RemoteAddr,
			RetryInterval:  time.Duration(c.config.RetryInterval) * time.Second,
			DialTimeOut:    time.Duration(c.config.DialTimeout) * time.Second,
			KeepAlive:      time.Duration(c.config.Keepalive) * time.Second,
			ConnPoolSize:   c.config.ConnectionPool,
			Token:          c.config.Token,
			Sniffer:        c.config.Sniffer,
			WebPort:        c.config.WebPort,
			SnifferLog:     c.config.SnifferLog,
			AggressivePool: c.config.AggressivePool,
		}
		udpClient := transport.NewUDPClient(c.ctx, udpConfig, c.logger)
		go udpClient.Start()

	default:
		c.logger.Fatal("invalid transport type: ", c.config.Transport)
	}

	<-c.ctx.Done()

	c.logger.Info("all workers stopped successfully")

	// suppress other logs
	c.logger.SetLevel(logrus.FatalLevel)
}
func (c *Client) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
}
