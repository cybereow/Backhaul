package cmd

import (
	"github.com/musix/backhaul/config"

	"github.com/sirupsen/logrus"
)

const ( // Default values
	// No default token: a shared secret baked into the binary would
	// authenticate any tokenless deployment with a value that is public in this
	// repo, turning it into an open relay attributable to the host. A token must
	// be configured explicitly (enforced in cmd.Run).
	defaultChannelSize    = 2048
	defaultRetryInterval  = 3 // only for client
	defaultConnectionPool = 8
	defaultLogLevel       = "info"
	defaultMuxSession     = 1
	defaultKeepAlive      = 75
	deafultHeartbeat      = 40 // 40 seconds
	defaultDialTimeout    = 10 // 10 seconds
	// related to smux
	defaultMuxVersion       = 1
	defaultMaxFrameSize     = 32768   // 32KB
	defaultMaxReceiveBuffer = 4194304 // 4MB
	defaultMaxStreamBuffer  = 65536   // 64KB
	defaultSnifferLog       = "backhaul.json"
	defaultMuxCon           = 8
	defaultMuxStripe        = 1    // 1 disables striping - one flow, one connection
	defaultUDPBuffer        = 2048 // datagrams queued per UDP flow before dropping (wsmux/wssmux)
)

func applyDefaults(cfg *config.Config) {
	// Token is intentionally not defaulted - see cmd.Run, which requires the
	// active side to configure one explicitly.

	// Nodelay default is false if not valid value found

	// Channel size
	if cfg.Server.ChannelSize <= 0 {
		cfg.Server.ChannelSize = defaultChannelSize
	}

	// Loglevel
	if _, err := logrus.ParseLevel(cfg.Client.LogLevel); err != nil {
		cfg.Client.LogLevel = defaultLogLevel
	}

	if _, err := logrus.ParseLevel(cfg.Server.LogLevel); err != nil {
		cfg.Server.LogLevel = defaultLogLevel
	}

	// Retry interval
	if cfg.Client.RetryInterval <= 0 {
		cfg.Client.RetryInterval = defaultRetryInterval
	}

	// Connection pool
	if cfg.Client.ConnectionPool <= 0 {
		cfg.Client.ConnectionPool = defaultConnectionPool
	}

	// Mux Session
	if cfg.Server.MuxSession <= 0 {
		cfg.Server.MuxSession = defaultMuxSession
	}
	if cfg.Client.MuxSession <= 0 {
		cfg.Client.MuxSession = defaultMuxSession
	}

	// PPROF default is false if not valid value found

	// keep alive
	if cfg.Server.Keepalive <= 0 {
		cfg.Server.Keepalive = defaultKeepAlive
	}
	if cfg.Client.Keepalive <= 0 {
		cfg.Client.Keepalive = defaultKeepAlive
	}

	// Mux version
	if cfg.Server.MuxVersion <= 0 || cfg.Server.MuxVersion > 2 {
		cfg.Server.MuxVersion = defaultMuxVersion
	}
	if cfg.Client.MuxVersion <= 0 || cfg.Client.MuxVersion > 2 {
		cfg.Client.MuxVersion = defaultMuxVersion
	}
	// MaxFrameSize
	if cfg.Server.MaxFrameSize <= 0 {
		cfg.Server.MaxFrameSize = defaultMaxFrameSize
	}
	if cfg.Client.MaxFrameSize <= 0 {
		cfg.Client.MaxFrameSize = defaultMaxFrameSize
	}
	// MaxReceiveBuffer
	if cfg.Server.MaxReceiveBuffer <= 0 {
		cfg.Server.MaxReceiveBuffer = defaultMaxReceiveBuffer
	}
	if cfg.Client.MaxReceiveBuffer <= 0 {
		cfg.Client.MaxReceiveBuffer = defaultMaxReceiveBuffer
	}
	// MaxStreamBuffer
	if cfg.Server.MaxStreamBuffer <= 0 {
		cfg.Server.MaxStreamBuffer = defaultMaxStreamBuffer
	}
	if cfg.Client.MaxStreamBuffer <= 0 {
		cfg.Client.MaxStreamBuffer = defaultMaxStreamBuffer
	}
	// WebPort returns 0 if not exists

	// SnifferLog
	if cfg.Server.SnifferLog == "" {
		cfg.Server.SnifferLog = defaultSnifferLog
	}
	if cfg.Client.SnifferLog == "" {
		cfg.Client.SnifferLog = defaultSnifferLog
	}
	// Heartbeat
	if cfg.Server.Heartbeat < 1 { // Minimum accepted interval is 1 second
		cfg.Server.Heartbeat = deafultHeartbeat
	}

	// Timeout
	if cfg.Client.DialTimeout < 1 { // Minimum accepted value is 1 second
		cfg.Client.DialTimeout = defaultDialTimeout
	}

	// Mux concurrancy
	if cfg.Server.MuxCon < 1 {
		cfg.Server.MuxCon = defaultMuxCon
	}

	// UDP per-flow buffer - how many datagrams a single UDP flow may queue
	// before packets are dropped. 0/unset keeps the original 2048.
	if cfg.Server.UDPBuffer < 1 {
		cfg.Server.UDPBuffer = defaultUDPBuffer
	}

	// Connection rotation age stays off unless set: it has to sit below the
	// max age of whatever CDN/LB fronts the server, and a guessed value just
	// churns connections for nothing.
	if cfg.Server.MaxConnAge < 0 {
		cfg.Server.MaxConnAge = 0
	}

	// Stripe factor - how many pooled connections a single flow is split
	// across. 1 (the default) leaves the original one-flow-one-connection
	// behavior untouched.
	if cfg.Server.StripeFactor < 1 {
		cfg.Server.StripeFactor = defaultMuxStripe
	}
	if cfg.Client.StripeFactor < 1 {
		cfg.Client.StripeFactor = defaultMuxStripe
	}

	// Stripe parity - Reed-Solomon parity legs added on top of the stripe
	// factor. 0 (the default) disables FEC entirely; a negative value is
	// just a stray config typo, not "disable more than disabled".
	if cfg.Server.StripeParity < 0 {
		cfg.Server.StripeParity = 0
	}
	if cfg.Client.StripeParity < 0 {
		cfg.Client.StripeParity = 0
	}
}
