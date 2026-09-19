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
	// minMaxStreamBuffer is the floor for the *derived* per-stream receive
	// window (see deriveStreamBuffer). It is the value that used to be the
	// fixed default, so the derivation can only ever widen the window, never
	// narrow it below what earlier releases ran with.
	minMaxStreamBuffer = 65536 // 64KB
	defaultSnifferLog  = "backhaul.json"
	defaultMuxCon      = 8
	defaultMuxStripe   = 1    // 1 disables striping - one flow, one connection
	defaultUDPBuffer   = 2048 // datagrams queued per UDP flow before dropping (wsmux/wssmux)
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
	// MaxStreamBuffer - derived from the session budget instead of pinned at
	// 64KB; see deriveStreamBuffer. An explicit mux_streambuffer still wins.
	// The client has no mux_con of its own (stream concurrency per session is
	// the server's setting), so it derives from the default.
	if cfg.Server.MaxStreamBuffer <= 0 {
		cfg.Server.MaxStreamBuffer = deriveStreamBuffer(cfg.Server.MaxReceiveBuffer, cfg.Server.MuxCon)
	}
	if cfg.Client.MaxStreamBuffer <= 0 {
		cfg.Client.MaxStreamBuffer = deriveStreamBuffer(cfg.Client.MaxReceiveBuffer, defaultMuxCon)
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

// deriveStreamBuffer sizes the smux per-stream receive window (mux_streambuffer)
// from the session receive budget (mux_recievebuffer) and how many streams share
// a session (mux_con), instead of pinning it at a fixed 64KB.
//
// Why this is the tunnel's throughput ceiling on mux_version = 2: v2 adds
// per-stream flow control, and smux's writeV2 will not put more than
// MaxStreamBuffer bytes in flight on a stream before it blocks waiting for the
// peer's window update - one full tunnel RTT away. A stream therefore tops out
// at MaxStreamBuffer/RTT no matter how much bandwidth the path has. At the old
// fixed 64KB and a 30ms tunnel that is ~2.2MB/s (~18 Mbps) per stream, while the
// session was already allowed to buffer 4MB - so ~98% of the budget the pool had
// reserved could never be used. Measured over an smux pair with 30ms of injected
// RTT (BenchmarkStreamWindow): 1 stream 2.23 -> 15.9 MB/s, 8 streams 17.5 ->
// 122.1 MB/s, both ~7x.
//
// That cap is what made upload lag download on wsmux/wssmux. The server's tunnel
// legs already force a 4MB SO_SNDBUF for the server -> client direction that
// carries the user's upload (see tunnelLegSendBuf), but smux never let more than
// 64KB per stream reach that socket, so the larger kernel buffer was unreachable.
// Sizing the window to the session budget is what lets those two agree.
//
// Memory does not grow: smux's session token bucket caps everything a session
// buffers at MaxReceiveBuffer and pauses reading the socket when it is spent, so
// per-stream windows only decide how that fixed budget is shared, never how big
// it is. Dividing by muxCon is exactly the fair share - the streams on a session
// can collectively fill the session window and no single one is throttled below
// its slice of it.
//
// The result is clamped to at least the historical 64KB (so a tiny configured
// receive buffer can't shrink the window below what earlier releases used) and
// to at most maxReceiveBuffer, which smux's VerifyConfig requires.
func deriveStreamBuffer(maxReceiveBuffer, muxCon int) int {
	if muxCon < 1 {
		muxCon = defaultMuxCon // defaults are applied later in applyDefaults
	}
	if maxReceiveBuffer <= 0 {
		maxReceiveBuffer = defaultMaxReceiveBuffer
	}

	buf := maxReceiveBuffer / muxCon
	if buf < minMaxStreamBuffer {
		buf = minMaxStreamBuffer
	}
	if buf > maxReceiveBuffer {
		buf = maxReceiveBuffer
	}
	return buf
}
