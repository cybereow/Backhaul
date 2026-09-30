package transport

import (
	"context"
	"net"
	"sync/atomic"
	"time"

	dnsx "github.com/musix/backhaul/internal/transport/dns"
	"github.com/musix/backhaul/internal/transport/dns/rel"
	"github.com/musix/backhaul/internal/transport/dns/sel"
	"github.com/musix/backhaul/internal/utils"
	"github.com/sirupsen/logrus"
)

type DNSMuxConfig struct {
	TcpMuxConfig
	Domain   string
	Key      string
	Profiles []sel.Profile
	Timeout  time.Duration
	Rel      rel.Config
}

type DNSMuxTransport struct {
	*TcpMuxTransport
	dnsConfig *DNSMuxConfig
}

func NewDNSMuxClient(parent context.Context, cfg *DNSMuxConfig, logger *logrus.Logger) *DNSMuxTransport {
	if cfg.Rel.RecvBuf == 0 {
		cfg.Rel.RecvBuf = 256 << 10
	}
	if cfg.Rel.SendBuf == 0 {
		cfg.Rel.SendBuf = 256 << 10
	}
	if cfg.Rel.MaxInflight == 0 {
		cfg.Rel.MaxInflight = 65535
	}
	t := &DNSMuxTransport{
		TcpMuxTransport: NewMuxClient(parent, &cfg.TcpMuxConfig, logger),
		dnsConfig:       cfg,
	}
	t.smuxConfig.KeepAliveTimeout = 2 * time.Minute
	return t
}

func (c *DNSMuxTransport) Start() {
	if c.config.WebPort > 0 {
		go c.usageMonitor.Monitor()
	}
	c.config.TunnelStatus = "Disconnected (DNSMux)"
	go c.dnsChannelDialer()
}

func (c *DNSMuxTransport) dial() (net.Conn, error) {
	return dnsx.Dial(c.ctx, dnsx.DialParams{
		Domain: c.dnsConfig.Domain, Key: c.dnsConfig.Key,
		Profiles: c.dnsConfig.Profiles, Timeout: c.dnsConfig.Timeout,
		Rel: c.dnsConfig.Rel,
	})
}

func (c *DNSMuxTransport) dnsChannelDialer() {
	for c.ctx.Err() == nil {
		conn, err := c.dial()
		if err != nil {
			c.waitRetry()
			continue
		}
		if err = utils.SendBinaryTransportString(conn, c.config.Token, utils.SG_Chan); err != nil {
			conn.Close()
			c.waitRetry()
			continue
		}
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		message, _, err := utils.ReceiveBinaryTransportString(conn)
		if err != nil || message != c.config.Token {
			conn.Close()
			c.waitRetry()
			continue
		}
		_ = conn.SetReadDeadline(time.Time{})
		c.controlChannel = conn
		c.config.TunnelStatus = "Connected (DNSMux)"
		for i := 0; i < c.config.ConnPoolSize; i++ {
			go c.dnsTunnelLoop()
		}
		go c.dnsChannelHandler()
		return
	}
}

func (c *DNSMuxTransport) waitRetry() {
	t := time.NewTimer(c.config.RetryInterval)
	defer t.Stop()
	select {
	case <-c.ctx.Done():
	case <-t.C:
	}
}

func (c *DNSMuxTransport) dnsTunnelLoop() {
	for c.ctx.Err() == nil {
		conn, err := c.dial()
		if err != nil {
			c.waitRetry()
			continue
		}
		atomic.AddInt32(&c.poolConnections, 1)
		c.handleSession(conn)
		atomic.AddInt32(&c.poolConnections, -1)
		conn.Close()
		c.waitRetry()
	}
}

func (c *DNSMuxTransport) dnsChannelHandler() {
	for c.ctx.Err() == nil {
		msg, err := utils.ReceiveBinaryByte(c.controlChannel)
		if err != nil {
			return
		}
		switch msg {
		case utils.SG_Chan:
			go c.dnsTunnelLoop()
		case utils.SG_HB:
			// Receipt is the heartbeat acknowledgement used by tcpmux.
		case utils.SG_Closed:
			return
		}
	}
}
