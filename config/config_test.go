package config

import (
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/assert"
)

func TestServerConfig(t *testing.T) {
	tomlData := `
bind_addr = "127.0.0.1:8080"
transport = "tcp"
token = "secret123"
nodelay = true
keepalive_period = 30
channel_size = 100
log_level = "info"
ports = ["80", "443"]
pprof = true
mux_session = 1
mux_version = 2
mux_framesize = 4096
mux_recievebuffer = 1048576
mux_streambuffer = 1048576
mux_keepalive_disabled = false
mux_stripe = 4
mux_stripe_parity = 2
stripe_ports = ["8081", "8082"]
promote_bytes = 102400
sniffer = true
web_port = 9090
sniffer_log = "/var/log/sniffer.log"
tls_cert = "cert.pem"
tls_key = "key.pem"
tls_certs = ["cert1.pem", "cert2.pem"]
tls_keys = ["key1.pem", "key2.pem"]
heartbeat = 5
mux_con = 10
accept_udp = true
skip_optz = false
mss = 1460
so_rcvbuf = 4194304
so_sndbuf = 4194304
proxy_protocol = true
path = "/ws"
fallback = "http://127.0.0.1:8081"
tls_engine = "standard"
max_conn_age = 3600
`

	var serverCfg ServerConfig
	_, err := toml.Decode(tomlData, &serverCfg)
	assert.NoError(t, err)

	assert.Equal(t, "127.0.0.1:8080", serverCfg.BindAddr)
	assert.Equal(t, TCP, serverCfg.Transport)
	assert.Equal(t, "secret123", serverCfg.Token)
	assert.True(t, serverCfg.Nodelay)
	assert.Equal(t, 30, serverCfg.Keepalive)
	assert.Equal(t, 100, serverCfg.ChannelSize)
	assert.Equal(t, "info", serverCfg.LogLevel)
	assert.Equal(t, []string{"80", "443"}, serverCfg.Ports)
	assert.True(t, serverCfg.PPROF)
	assert.Equal(t, 1, serverCfg.MuxSession)
	assert.Equal(t, 2, serverCfg.MuxVersion)
	assert.Equal(t, 4096, serverCfg.MaxFrameSize)
	assert.Equal(t, 1048576, serverCfg.MaxReceiveBuffer)
	assert.Equal(t, 1048576, serverCfg.MaxStreamBuffer)
	assert.False(t, serverCfg.MuxKeepaliveDisabled)
	assert.Equal(t, 4, serverCfg.StripeFactor)
	assert.Equal(t, 2, serverCfg.StripeParity)
	assert.Equal(t, []string{"8081", "8082"}, serverCfg.StripePorts)
	assert.Equal(t, uint64(102400), serverCfg.PromoteBytes)
	assert.True(t, serverCfg.Sniffer)
	assert.Equal(t, 9090, serverCfg.WebPort)
	assert.Equal(t, "/var/log/sniffer.log", serverCfg.SnifferLog)
	assert.Equal(t, "cert.pem", serverCfg.TLSCertFile)
	assert.Equal(t, "key.pem", serverCfg.TLSKeyFile)
	assert.Equal(t, []string{"cert1.pem", "cert2.pem"}, serverCfg.TLSCerts)
	assert.Equal(t, []string{"key1.pem", "key2.pem"}, serverCfg.TLSKeys)
	assert.Equal(t, 5, serverCfg.Heartbeat)
	assert.Equal(t, 10, serverCfg.MuxCon)
	assert.True(t, serverCfg.AcceptUDP)
	assert.False(t, serverCfg.SkipOptz)
	assert.Equal(t, 1460, serverCfg.MSS)
	assert.Equal(t, 4194304, serverCfg.SO_RCVBUF)
	assert.Equal(t, 4194304, serverCfg.SO_SNDBUF)
	assert.True(t, serverCfg.ProxyProtocol)
	assert.Equal(t, "/ws", serverCfg.Path)
	assert.Equal(t, "http://127.0.0.1:8081", serverCfg.Fallback)
	assert.Equal(t, "standard", serverCfg.TLSEngine)
	assert.Equal(t, 3600, serverCfg.MaxConnAge)
}

func TestClientConfig(t *testing.T) {
	tomlData := `
remote_addr = "example.com:443"
remote_addrs = ["1.1.1.1:443", "2.2.2.2:443"]
edge_ips = ["192.168.1.1", "192.168.1.2"]
transport = "wssmux"
token = "auth-token"
connection_pool = 10
retry_interval = 5
nodelay = false
keepalive_period = 60
log_level = "debug"
pprof = false
mux_session = 2
mux_version = 1
mux_framesize = 8192
mux_recievebuffer = 2097152
mux_streambuffer = 2097152
mux_keepalive_disabled = true
mux_stripe = 8
mux_stripe_parity = 4
stripe_ports = ["1000", "2000"]
sniffer = false
web_port = 8080
sniffer_log = ""
dial_timeout = 10
aggressive_pool = true
edge_ip = "10.0.0.1"
skip_optz = true
mss = 1400
so_rcvbuf = 8388608
so_sndbuf = 8388608
path = "/tunnel"
tls_verify = false
`

	var clientCfg ClientConfig
	_, err := toml.Decode(tomlData, &clientCfg)
	assert.NoError(t, err)

	assert.Equal(t, "example.com:443", clientCfg.RemoteAddr)
	assert.Equal(t, []string{"1.1.1.1:443", "2.2.2.2:443"}, clientCfg.RemoteAddrs)
	assert.Equal(t, []string{"192.168.1.1", "192.168.1.2"}, clientCfg.EdgeIPs)
	assert.Equal(t, WSSMUX, clientCfg.Transport)
	assert.Equal(t, "auth-token", clientCfg.Token)
	assert.Equal(t, 10, clientCfg.ConnectionPool)
	assert.Equal(t, 5, clientCfg.RetryInterval)
	assert.False(t, clientCfg.Nodelay)
	assert.Equal(t, 60, clientCfg.Keepalive)
	assert.Equal(t, "debug", clientCfg.LogLevel)
	assert.False(t, clientCfg.PPROF)
	assert.Equal(t, 2, clientCfg.MuxSession)
	assert.Equal(t, 1, clientCfg.MuxVersion)
	assert.Equal(t, 8192, clientCfg.MaxFrameSize)
	assert.Equal(t, 2097152, clientCfg.MaxReceiveBuffer)
	assert.Equal(t, 2097152, clientCfg.MaxStreamBuffer)
	assert.True(t, clientCfg.MuxKeepaliveDisabled)
	assert.Equal(t, 8, clientCfg.StripeFactor)
	assert.Equal(t, 4, clientCfg.StripeParity)
	assert.Equal(t, []string{"1000", "2000"}, clientCfg.StripePorts)
	assert.False(t, clientCfg.Sniffer)
	assert.Equal(t, 8080, clientCfg.WebPort)
	assert.Equal(t, "", clientCfg.SnifferLog)
	assert.Equal(t, 10, clientCfg.DialTimeout)
	assert.True(t, clientCfg.AggressivePool)
	assert.Equal(t, "10.0.0.1", clientCfg.EdgeIP)
	assert.True(t, clientCfg.SkipOptz)
	assert.Equal(t, 1400, clientCfg.MSS)
	assert.Equal(t, 8388608, clientCfg.SO_RCVBUF)
	assert.Equal(t, 8388608, clientCfg.SO_SNDBUF)
	assert.Equal(t, "/tunnel", clientCfg.Path)
	assert.False(t, clientCfg.TLSVerify)
}

func TestConfigIsDefined(t *testing.T) {
	tomlData := `
[server]
bind_addr = "0.0.0.0:80"

[client]
remote_addr = "127.0.0.1:443"
`
	var cfg Config
	meta, err := toml.Decode(tomlData, &cfg)
	assert.NoError(t, err)

	assert.True(t, meta.IsDefined("server", "bind_addr"))
	assert.True(t, meta.IsDefined("client", "remote_addr"))
	assert.False(t, meta.IsDefined("server", "transport"))
	assert.False(t, meta.IsDefined("client", "token"))
}

func TestConfigTypesParseCorrectly(t *testing.T) {
	tomlData := `
[server]
transport = "udp"
`
	var cfg Config
	_, err := toml.Decode(tomlData, &cfg)
	assert.NoError(t, err)

	assert.Equal(t, UDP, cfg.Server.Transport)
}
