// Package client wires a [client] configuration to the transport that dials the
// tunnel server.
package client

import (
	"context"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"time"

	"github.com/musix/backhaul/config"
	"github.com/musix/backhaul/internal/client/transport"
	"github.com/musix/backhaul/internal/utils"

	"github.com/sirupsen/logrus"
)

// pprofAddr is loopback-only: pprof serves heap dumps from a process holding the
// tunnel token, so it must never be reachable off-host. Reach it through an SSH
// tunnel if you need it remotely.
const pprofAddr = "127.0.0.1:6061"

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

// Start launches the configured transport and blocks until the client is stopped.
func (c *Client) Start() {
	if c.config.PPROF {
		go func() {
			c.logger.Infof("pprof started at %s", pprofAddr)
			http.ListenAndServe(pprofAddr, nil)
		}()
	}

	remote := c.config.RemoteAddr
	if remote == "" && len(c.config.RemoteAddrs) > 0 {
		remote = strings.Join(c.config.RemoteAddrs, ", ")
	}
	c.logger.Infof("client with remote address %s started successfully", remote)

	t := c.config.Transport
	switch {
	case t == config.WS || t == config.WSS:
		c.startWS()
	case t == config.WSMUX || t == config.WSSMUX:
		c.startWSMux()
	case t.IsDNS():
		c.startDNS()
	default:
		c.logger.Fatal("invalid transport type: ", t)
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

func (c *Client) seconds(n int) time.Duration { return time.Duration(n) * time.Second }

// warnInsecureTLS logs the MITM warning when a TLS transport runs unverified.
func (c *Client) warnInsecureTLS() {
	if c.config.Transport.IsTLS() && !c.config.TLSVerify {
		c.logger.Warnf("SECURITY: %s server certificate verification is OFF (tls_verify=false); the auth token can be harvested by an on-path party via TLS MITM. Set tls_verify=true once the server presents a verifiable certificate.", c.config.Transport)
	}
}

func (c *Client) startWS() {
	cfg := c.config
	c.warnInsecureTLS()
	client := transport.NewWSClient(c.ctx, &transport.WsConfig{
		RemoteAddr:     cfg.RemoteAddr,
		RemoteAddrs:    cfg.RemoteAddrs,
		EdgeIPs:        cfg.EdgeIPs,
		Nodelay:        cfg.Nodelay,
		KeepAlive:      c.seconds(cfg.Keepalive),
		RetryInterval:  c.seconds(cfg.RetryInterval),
		DialTimeOut:    c.seconds(cfg.DialTimeout),
		ConnPoolSize:   cfg.ConnectionPool,
		Token:          cfg.Token,
		Sniffer:        cfg.Sniffer,
		WebPort:        cfg.WebPort,
		SnifferLog:     cfg.SnifferLog,
		Mode:           cfg.Transport,
		AggressivePool: cfg.AggressivePool,
		EdgeIP:         cfg.EdgeIP,
		Path:           cfg.Path,
		SO_RCVBUF:      cfg.SO_RCVBUF,
		SO_SNDBUF:      cfg.SO_SNDBUF,
		MSS:            cfg.MSS,
		TLSVerify:      cfg.TLSVerify,
	}, c.logger)
	go client.Start()
}

func (c *Client) startWSMux() {
	cfg := c.config
	c.warnInsecureTLS()
	client := transport.NewWSMuxClient(c.ctx, &transport.WsMuxConfig{
		RemoteAddr:           cfg.RemoteAddr,
		RemoteAddrs:          cfg.RemoteAddrs,
		EdgeIPs:              cfg.EdgeIPs,
		Nodelay:              cfg.Nodelay,
		KeepAlive:            c.seconds(cfg.Keepalive),
		RetryInterval:        c.seconds(cfg.RetryInterval),
		DialTimeOut:          c.seconds(cfg.DialTimeout),
		ConnPoolSize:         cfg.ConnectionPool,
		Token:                cfg.Token,
		MuxVersion:           cfg.MuxVersion,
		MaxFrameSize:         cfg.MaxFrameSize,
		MaxReceiveBuffer:     cfg.MaxReceiveBuffer,
		MaxStreamBuffer:      cfg.MaxStreamBuffer,
		MuxKeepaliveDisabled: cfg.MuxKeepaliveDisabled,
		StripeFactor:         cfg.StripeFactor,
		StripeParity:         cfg.StripeParity,
		SO_RCVBUF:            cfg.SO_RCVBUF,
		SO_SNDBUF:            cfg.SO_SNDBUF,
		MSS:                  cfg.MSS,
		Sniffer:              cfg.Sniffer,
		WebPort:              cfg.WebPort,
		SnifferLog:           cfg.SnifferLog,
		Mode:                 cfg.Transport,
		AggressivePool:       cfg.AggressivePool,
		EdgeIP:               cfg.EdgeIP,
		Path:                 cfg.Path,
		TLSVerify:            cfg.TLSVerify,
		WSFraming:            cfg.MuxWSFraming,
		StealthHandshake:     cfg.MuxStealthHandshake,
		ResumeWindow:         c.seconds(cfg.ResumeWindow),
	}, c.logger)
	go client.Start()
}

func (c *Client) startDNS() {
	cfg := c.config
	key := cfg.DNSKey
	if key == "" {
		key = cfg.Token
	}
	client := transport.NewDnsMuxClient(c.ctx, &transport.DnsMuxConfig{
		Domain:           cfg.DNSDomain,
		Key:              key,
		Resolvers:        cfg.DNSResolvers,
		RecordTypes:      cfg.DNSRecordTypes,
		ResolverCIDRs:    cfg.DNSResolverCIDRs,
		ResolverCache:    cfg.DNSResolverCache,
		Timeout:          time.Duration(cfg.DNSTimeoutMS) * time.Millisecond,
		Workers:          cfg.DNSWorkers,
		NoHedge:          cfg.DNSNoHedge,
		Token:            cfg.Token,
		RetryInterval:    c.seconds(cfg.RetryInterval),
		DialTimeOut:      c.seconds(cfg.DialTimeout),
		KeepAlive:        c.seconds(cfg.Keepalive),
		ConnPoolSize:     cfg.ConnectionPool,
		MuxVersion:       cfg.MuxVersion,
		MaxFrameSize:     cfg.MaxFrameSize,
		MaxReceiveBuffer: cfg.MaxReceiveBuffer,
		MaxStreamBuffer:  cfg.MaxStreamBuffer,
		Sniffer:          cfg.Sniffer,
		WebPort:          cfg.WebPort,
		SnifferLog:       cfg.SnifferLog,
		MSS:              cfg.MSS,
		SO_RCVBUF:        cfg.SO_RCVBUF,
		SO_SNDBUF:        cfg.SO_SNDBUF,
	}, c.logger)
	go client.Start()
}
