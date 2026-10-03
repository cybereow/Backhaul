package transport

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	dnsx "github.com/musix/backhaul/internal/transport/dns"
	"github.com/musix/backhaul/internal/transport/dns/rel"
	"github.com/musix/backhaul/internal/transport/dns/sel"
	"github.com/musix/backhaul/internal/utils"
	"github.com/musix/backhaul/internal/utils/handlers"
	"github.com/musix/backhaul/internal/utils/network"
	"github.com/musix/backhaul/internal/web"

	"github.com/musix/backhaul/internal/smux"
	"github.com/sirupsen/logrus"
)

// DnsMuxConfig configures the dnsmux client: it reaches the server ONLY through
// the listed recursive resolvers, using whichever record type/transport the
// live measurements favour, and serves smux streams over the resulting conn.
type DnsMuxConfig struct {
	Domain           string
	Key              string
	Resolvers        []string // empty or containing "auto": also test dnsx.DefaultResolvers
	RecordTypes      []string
	ResolverCIDRs    []string      // extra candidates to discover (CIDRs or IPs)
	ResolverCache    string        // discovery result cache file
	Timeout          time.Duration // per DNS query (default 2s)
	Workers          int           // DNS queries in flight per tunnel conn (default 16)
	NoHedge          bool          // disable hedging
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
	statusMu     sync.Mutex
	live         int // tunnels currently established (under statusMu, with TunnelStatus)
	disc         discoveryShare
}

func NewDnsMuxClient(parentCtx context.Context, config *DnsMuxConfig, logger *logrus.Logger) *DnsMuxTransport {
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
	if config.Timeout <= 0 {
		config.Timeout = 2 * time.Second
	}
	if config.RetryInterval <= 0 {
		config.RetryInterval = 3 * time.Second
	}
	if config.Workers <= 0 {
		config.Workers = 16
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
	refresh := false
	for c.ctx.Err() == nil {
		// A tunnel that was up and then failed invalidates the cached resolver set; a
		// failure before it ever came up (wrong token, handshake trouble) says nothing
		// about the resolvers and must not repeat the sweep.
		refresh = c.runTunnel(refresh)
		if !c.sleep(c.config.RetryInterval) {
			return
		}
	}
}

// dnsMaxInflight is the unacked-byte window of the carrier (the default, 8 MSS,
// would cap a stream at a few hundred bytes in flight however many workers run).
const dnsMaxInflight = 32 * 1024

// dnsAuthTimeout bounds the token handshake on a fresh tunnel conn.
const dnsAuthTimeout = 60 * time.Second

func (c *DnsMuxTransport) runTunnel(refresh bool) (wasUp bool) {
	// Spread workers over several profiles: one resolver/record type collapses
	// well before the path does when it carries every in-flight query.
	topK := c.config.Workers / 3
	if topK < 2 {
		topK = 2
	}
	if topK > 8 {
		topK = 8
	}

	resolvers := dnsx.ExpandResolvers(c.config.Resolvers)
	if dnsx.IsAuto(c.config.Resolvers) || len(c.config.ResolverCIDRs) > 0 {
		// Discovery: measure every candidate and keep the best few, whatever the
		// list came from (built-in, configured, or a CIDR sweep). One sweep serves
		// every tunnel of the pool (see discoveryShare).
		var err error
		resolvers, err = c.disc.get(refresh, func(refresh bool) ([]string, error) {
			cands := resolvers
			if len(c.config.ResolverCIDRs) > 0 {
				extra, err := dnsx.ExpandCIDRs(c.config.ResolverCIDRs, 4096)
				if err != nil {
					return nil, fmt.Errorf("dns_resolver_cidrs: %w", err)
				}
				cands = append(cands, extra...)
			}
			ranked := dnsx.DiscoverResolvers(c.ctx, cands, dnsx.DiscoverOpts{
				Domain: c.config.Domain, Key: c.config.Key, CachePath: c.config.ResolverCache, Refresh: refresh, Logf: c.logger.Infof,
				RRTypes: dnsx.RecordTypeCodes(c.config.Domain, c.config.RecordTypes),
			})
			out := make([]string, 0, len(ranked))
			for _, r := range ranked {
				out = append(out, r.Resolver)
			}
			return out, nil
		})
		if err != nil {
			c.logger.Errorf("dnsmux: %v", err)
			return
		}
		if len(resolvers) == 0 {
			c.logger.Error("dnsmux: no candidate resolver reached the server; will retry")
			return
		}
	}
	profiles := dnsx.DefaultProfiles(c.config.Domain, resolvers, c.config.RecordTypes)
	if len(profiles) == 0 {
		c.logger.Error("dnsmux: no usable resolver/record-type profiles")
		return
	}

	conn, err := dnsx.Dial(c.ctx, dnsx.DialParams{
		Domain:   c.config.Domain,
		Key:      c.config.Key,
		Profiles: profiles,
		Timeout:  c.config.Timeout,
		Workers:  c.config.Workers,
		NoHedge:  c.config.NoHedge,
		Rel:      rel.Config{MinRTO: time.Second, MaxRTO: 4 * time.Second, MaxInflight: dnsMaxInflight}, // see the server side: real DNS RTTs are slow and jittery
		Sel:      sel.Config{TopK: topK, SpreadFloor: 0.3},
		Logf:     c.logger.Debugf,
	})
	if err != nil {
		c.logger.Errorf("dnsmux: dial: %v", err)
		return
	}
	defer conn.Close()

	// Not DialTimeOut (applyDefaults makes it 10s): the handshake is several DNS
	// round trips and must survive profile timeouts and retransmissions.
	_ = conn.SetDeadline(time.Now().Add(dnsAuthTimeout))
	// Challenge-response: the token is never sent (the carrier is not encrypted).
	if err := dnsx.ClientAuth(conn, c.config.Token); err != nil {
		c.logger.Errorf("dnsmux: authentication: %v", err)
		return
	}
	_ = conn.SetDeadline(time.Time{})

	session, err := smux.Server(conn, c.smuxConfig)
	if err != nil {
		c.logger.Errorf("dnsmux: failed to create mux session: %v", err)
		return
	}
	defer session.Close()
	// Count first, then publish: a teardown on another tunnel must never see a
	// transient zero after we have reported Connected.
	c.statusMu.Lock()
	c.live++
	c.config.TunnelStatus = "Connected (DNSMUX)"
	c.statusMu.Unlock()
	c.logger.Info("dnsmux: tunnel established")
	wasUp = true
	defer func() {
		// With a pool, only the last live tunnel going away means disconnected.
		c.statusMu.Lock()
		c.live--
		if c.live == 0 {
			c.config.TunnelStatus = "Disconnected (DNSMUX)"
		}
		c.statusMu.Unlock()
	}()

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

// refreshMinAge: a second request to re-run discovery within this long of the
// last sweep reuses its result, so a pool whose tunnels all fail together does
// not start one sweep per tunnel.
const refreshMinAge = 20 * time.Second

// discoveryShare makes all tunnel loops of a transport share one discovery: the
// sweep is serialized, and loops that arrive while (or shortly after) it ran use
// its result instead of launching their own throttled-in-isolation sweeps.
type discoveryShare struct {
	mu      sync.Mutex
	result  []string
	at      time.Time
	pending bool // a refresh was requested and no new sweep has completed yet
}

// get returns the shared resolver set, running run when there is none yet, or
// when refresh is requested and the last sweep is older than refreshMinAge. An
// empty result is not remembered (the next caller retries).
func (d *discoveryShare) get(refresh bool, run func(refresh bool) ([]string, error)) ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if refresh {
		d.pending = true // stays set until a sweep has actually run, however many retries it takes
	}
	if len(d.result) > 0 && (!d.pending || time.Since(d.at) < refreshMinAge) {
		return append([]string(nil), d.result...), nil
	}
	res, err := run(d.pending) // the effective request: a stored refresh must bypass the resolver cache too
	if err != nil {
		return nil, err
	}
	d.pending = false
	if len(res) > 0 {
		d.result, d.at = res, time.Now()
	}
	return append([]string(nil), res...), nil
}
