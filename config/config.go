package config

// TransportType defines the type of transport.
type TransportType string

const (
	TCP    TransportType = "tcp"
	TCPMUX TransportType = "tcpmux"
	WS     TransportType = "ws"
	WSS    TransportType = "wss"
	WSMUX  TransportType = "wsmux"
	WSSMUX TransportType = "wssmux"
	UDP    TransportType = "udp"
)

// ServerConfig represents the configuration for the server.
type ServerConfig struct {
	BindAddr             string        `toml:"bind_addr"`
	Transport            TransportType `toml:"transport"`
	Token                string        `toml:"token"`
	Nodelay              bool          `toml:"nodelay"`
	Keepalive            int           `toml:"keepalive_period"`
	ChannelSize          int           `toml:"channel_size"`
	LogLevel             string        `toml:"log_level"`
	Ports                []string      `toml:"ports"`
	PPROF                bool          `toml:"pprof"`
	MuxSession           int           `toml:"mux_session"`
	MuxVersion           int           `toml:"mux_version"`
	MaxFrameSize         int           `toml:"mux_framesize"`
	MaxReceiveBuffer     int           `toml:"mux_recievebuffer"`
	MaxStreamBuffer      int           `toml:"mux_streambuffer"`
	MuxKeepaliveDisabled bool          `toml:"mux_keepalive_disabled"`
	StripeFactor         int           `toml:"mux_stripe"`
	StripeParity         int           `toml:"mux_stripe_parity"` // Reed-Solomon parity legs added on top of mux_stripe (0 disables FEC); tolerates that many pool legs dying mid-flow without losing the flow. Only meaningful with mux_stripe > 1.
	StripePorts          []string      `toml:"stripe_ports"`
	PromoteBytes         uint64        `toml:"promote_bytes"` // wsmux/wssmux: once a single plain flow has moved this many bytes, migrate it in-flight onto a striped group (mid-stream promotion). Requires mux_version = 2 on both ends. 0 (default) disables promotion. Helps a single long-running bulk flow; leave 0 for many-connection proxy workloads.
	Sniffer              bool          `toml:"sniffer"`
	WebPort              int           `toml:"web_port"`
	SnifferLog           string        `toml:"sniffer_log"`
	TLSCertFile          string        `toml:"tls_cert"`
	TLSKeyFile           string        `toml:"tls_key"`
	TLSCerts             []string      `toml:"tls_certs"` // optional: multiple cert files for SNI (multi-domain); pairs with tls_keys by index
	TLSKeys              []string      `toml:"tls_keys"`  // optional: key files matching tls_certs by index
	Heartbeat            int           `toml:"heartbeat"`
	MuxCon               int           `toml:"mux_con"`
	AcceptUDP            bool          `toml:"accept_udp"`
	UDPBuffer            int           `toml:"udp_buffer"` // wsmux/wssmux: how many datagrams to queue per UDP flow before dropping (default 2048). Raise it to absorb short bursts when a single flow briefly outruns the tunnel; it smooths bursts but does not raise a flow's steady-state throughput.
	SkipOptz             bool          `toml:"skip_optz"`
	MSS                  int           `toml:"mss"`
	SO_RCVBUF            int           `toml:"so_rcvbuf"`
	SO_SNDBUF            int           `toml:"so_sndbuf"`
	ProxyProtocol        bool          `toml:"proxy_protocol"`
	Path                 string        `toml:"path"`
	Fallback             string        `toml:"fallback"`
	TLSEngine            string        `toml:"tls_engine"`
	MaxConnAge           int           `toml:"max_conn_age"` // seconds. Retire a pool connection once it reaches this age, draining the streams still running on it first, so a CDN/LB max-age reset never lands on a connection we are still using. 0 (default) disables rotation - the right value depends on the CDN in front of the server, so it must be set deliberately.
}

// ClientConfig represents the configuration for the client.
type ClientConfig struct {
	RemoteAddr           string        `toml:"remote_addr"`
	RemoteAddrs          []string      `toml:"remote_addrs"` // optional: multiple tunnel endpoints (e.g. same origin behind several CDNs/domains); the ws/wss/wsmux/wssmux pool spreads across them round-robin. Falls back to remote_addr when empty.
	EdgeIPs              []string      `toml:"edge_ips"`     // optional: edge IP to dial per remote_addrs entry (aligned by index); empty entries dial the domain directly.
	Transport            TransportType `toml:"transport"`
	Token                string        `toml:"token"`
	ConnectionPool       int           `toml:"connection_pool"`
	RetryInterval        int           `toml:"retry_interval"`
	Nodelay              bool          `toml:"nodelay"`
	Keepalive            int           `toml:"keepalive_period"`
	LogLevel             string        `toml:"log_level"`
	PPROF                bool          `toml:"pprof"`
	MuxSession           int           `toml:"mux_session"`
	MuxVersion           int           `toml:"mux_version"`
	MaxFrameSize         int           `toml:"mux_framesize"`
	MaxReceiveBuffer     int           `toml:"mux_recievebuffer"`
	MaxStreamBuffer      int           `toml:"mux_streambuffer"`
	MuxKeepaliveDisabled bool          `toml:"mux_keepalive_disabled"`
	StripeFactor         int           `toml:"mux_stripe"`
	StripeParity         int           `toml:"mux_stripe_parity"` // Reed-Solomon parity legs added on top of mux_stripe (0 disables FEC); tolerates that many pool legs dying mid-flow without losing the flow. Only meaningful with mux_stripe > 1.
	StripePorts          []string      `toml:"stripe_ports"`
	Sniffer              bool          `toml:"sniffer"`
	WebPort              int           `toml:"web_port"`
	SnifferLog           string        `toml:"sniffer_log"`
	DialTimeout          int           `toml:"dial_timeout"`
	AggressivePool       bool          `toml:"aggressive_pool"`
	EdgeIP               string        `toml:"edge_ip"`
	SkipOptz             bool          `toml:"skip_optz"`
	MSS                  int           `toml:"mss"`
	SO_RCVBUF            int           `toml:"so_rcvbuf"`
	SO_SNDBUF            int           `toml:"so_sndbuf"`
	Path                 string        `toml:"path"`
	TLSVerify            bool          `toml:"tls_verify"` // wss/wssmux: verify the server's TLS certificate. Enabled by default; set to false for self-signed setups.
}

// Config represents the complete configuration, including both server and client settings.
type Config struct {
	Server ServerConfig `toml:"server"`
	Client ClientConfig `toml:"client"`
}
