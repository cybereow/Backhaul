package transport

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	dnsx "github.com/musix/backhaul/internal/transport/dns"
	"github.com/musix/backhaul/internal/utils"
	"github.com/musix/backhaul/internal/utils/handlers"
	"github.com/musix/backhaul/internal/utils/network"
	"github.com/musix/backhaul/internal/web"

	"github.com/sirupsen/logrus"
	"github.com/xtaci/smux"
)

// DnsMuxConfig configures the dnsmux client: it reaches the server ONLY through
// the listed recursive resolvers, using whichever record type/transport the
// live measurements favour, and serves smux streams over the resulting conn.
type DnsMuxConfig struct {
	Domain           string
	Key              string
	Resolvers        []string
	RecordTypes      []string
	Timeout          time.Duration // per DNS query (default 2s)
	Token            string
	RetryInterval    time.Duration
	DialTimeOut      time.Duration
	KeepAlive        time.Duration
	ConnPoolSize     int // parallel tunnel conns (default 1)
	MuxVersion       int
	MaxFrameSize     int
	MaxReceiveBuffer int
	MaxStreamBuffer  int
	Sniffer          bool
	WebPort          int
	SnifferLog       string
	TunnelStatus     string
	MSS              int
	SO_RCVBUF        int
	SO_SNDBUF        int
}

type DnsMuxTransport struct {
	config       *DnsMuxConfig
	smuxConfig   *smux.Config
	ctx          context.Context
	cancel       context.CancelFunc
	logger       *logrus.Logger
	usageMonitor *web.Usage
	wg           sync.WaitGroup
}

func NewDnsMuxClient(parentCtx context.Context, config *DnsMuxConfig, logger *logrus.Logger) *DnsMuxTransport {
	ctx, cancel := context.WithCancel(parentCtx)
	sc := smux.DefaultConfig()
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
	if config.Timeout <= 0 {
		config.Timeout = 2 * time.Second
	}
	if config.RetryInterval <= 0 {
		config.RetryInterval = 3 * time.Second
	}
	if config.ConnPoolSize <= 0 {
		config.ConnPoolSize = 1
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

func (c *DnsMuxTransport) Start() {
	if c.config.WebPort > 0 {
		go c.usageMonitor.Monitor()
	}
	c.config.TunnelStatus = "Disconnected (DNSMUX)"
	for i := 0; i < c.config.ConnPoolSize; i++ {
		c.wg.Add(1)
		go c.tunnelLoop()
	}
}

// Close stops the transport and waits for its tunnel loops.
func (c *DnsMuxTransport) Close() {
	c.cancel()
	c.wg.Wait()
}

func (c *DnsMuxTransport) sleep(d time.Duration) bool {
	select {
	case <-c.ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// tunnelLoop keeps one tunnel conn alive: dial, authenticate, serve streams, and
// on any failure reconnect after RetryInterval.
func (c *DnsMuxTransport) tunnelLoop() {
	defer c.wg.Done()
	for c.ctx.Err() == nil {
		c.runTunnel()
		if !c.sleep(c.config.RetryInterval) {
			return
		}
	}
}

func (c *DnsMuxTransport) runTunnel() {
	profiles := dnsx.DefaultProfiles(c.config.Domain, c.config.Resolvers, c.config.RecordTypes)
	if len(profiles) == 0 {
		c.logger.Error("dnsmux: no usable resolver/record-type profiles")
		return
	}

	conn, err := dnsx.Dial(c.ctx, dnsx.DialParams{
		Domain:   c.config.Domain,
		Key:      c.config.Key,
		Profiles: profiles,
		Timeout:  c.config.Timeout,
	})
	if err != nil {
		c.logger.Errorf("dnsmux: dial: %v", err)
		return
	}
	defer conn.Close()

	dial := c.config.DialTimeOut
	if dial <= 0 {
		dial = 30 * time.Second
	}
	_ = conn.SetDeadline(time.Now().Add(dial))
	if err := utils.SendBinaryString(conn, c.config.Token); err != nil {
		c.logger.Errorf("dnsmux: failed to send token: %v", err)
		return
	}
	got, err := utils.ReceiveBinaryString(conn)
	if err != nil {
		c.logger.Errorf("dnsmux: token handshake: %v", err)
		return
	}
	if got != c.config.Token {
		c.logger.Error("dnsmux: server answered with a different token")
		return
	}
	_ = conn.SetDeadline(time.Time{})

	session, err := smux.Server(conn, c.smuxConfig)
	if err != nil {
		c.logger.Errorf("dnsmux: failed to create mux session: %v", err)
		return
	}
	defer session.Close()
	c.config.TunnelStatus = "Connected (DNSMUX)"
	c.logger.Info("dnsmux: tunnel established")
	defer func() { c.config.TunnelStatus = "Disconnected (DNSMUX)" }()

	for {
		stream, err := session.AcceptStream()
		if err != nil {
			c.logger.Warnf("dnsmux: session closed: %v", err)
			return
		}
		remote, err := utils.ReceiveBinaryString(stream)
		if err != nil {
			c.logger.Errorf("dnsmux: unable to read target from stream: %v", err)
			stream.Close()
			continue
		}
		go c.localDialer(stream, remote)
	}
}

func (c *DnsMuxTransport) localDialer(stream *smux.Stream, remoteAddr string) {
	port, resolved, err := network.ResolveRemoteAddr(remoteAddr)
	if err != nil {
		c.logger.Infof("dnsmux: failed to resolve remote port: %v", err)
		stream.Close()
		return
	}

	sendBuf, recvBuf := c.config.SO_SNDBUF, c.config.SO_RCVBUF
	if strings.Contains(resolved, "127.0.0.1") {
		sendBuf, recvBuf = 32*1024, 32*1024
	}
	timeout := c.config.DialTimeOut
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	local, err := network.TcpDialer(c.ctx, resolved, "", timeout, c.config.KeepAlive, true, 1, recvBuf, sendBuf, c.config.MSS)
	if err != nil {
		c.logger.Errorf("dnsmux: local dialer: %v", err)
		stream.Close()
		return
	}
	handlers.TCPConnectionHandler(c.ctx, false, stream, local, c.logger, c.usageMonitor, int(port), c.config.Sniffer)
}
