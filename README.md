# Backhaul

Welcome to the **`Backhaul`** project! This project provides a high-performance reverse tunneling solution optimized for handling massive concurrent connections through NAT and firewalls. This README will guide you through setting up and configuring both server and client components, including details on different transport protocols.

---

## Table of Contents

1. [Introduction](#introduction)
2. [Features](#features)
3. [Installation](#installation)
4. [Usage](#usage)
   - [Configuration Options](#configuration-options)
   - [Detailed Configuration](#detailed-configuration)
      - [TCP Configuration](#tcp-configuration)
      - [TCP Multiplexing Configuration](#tcp-multiplexing-configuration)
      - [UDP Configuration](#udp-configuration)
      - [WebSocket Configuration](#websocket-configuration)
      - [Secure WebSocket Configuration](#secure-websocket-configuration)
      - [WS Multiplexing Configuration](#ws-multiplexing-configuration)
      - [WSS Multiplexing Configuration](#wss-multiplexing-configuration)
5. [Generating a Self-Signed TLS Certificate with OpenSSL](#generating-a-self-signed-tls-certificate-with-openssl)
6. [Running backhaul as a service](#running-backhaul-as-a-service)
7. [FAQ](#faq)
8. [Benchmark](#benchmark)
9. [License](#license)
10. [Donation](#donation)

---

## Introduction

This project offers a robust reverse tunneling solution to overcome NAT and firewall restrictions, supporting various transport protocols. It’s engineered for high efficiency and concurrency.


## Features

* **High Performance**: Optimized for handling massive concurrent connections efficiently.
* **Protocol Flexibility**: Supports TCP, WebSocket (WS), and Secure WebSocket (WSS) transports.
* **UDP over TCP**: Implements UDP traffic encapsulation and forwarding over a TCP connection for reliable delivery with built-in congestion control.
* **Multiplexing**: Enables multiple connections over a single transport with SMUX.
* **NAT & Firewall Bypass**: Overcomes restrictions with reverse tunneling.
* **Traffic Sniffing**: Optional network traffic monitoring with logging support.
* **Configurable Keepalive**: Adjustable keep-alive and heartbeat intervals for stable connections.
* **TLS Encryption**: Secure connections via WSS with support for custom TLS certificates.
* **Web Interface**: Real-time monitoring through a lightweight web interface.
* **Hot Reload Configuration**: Supports dynamic configuration reloading without server restarts.


## Installation

1. **Download** the latest release from the [GitHub releases page](https://github.com/musixal/backhaul/releases).
2. **Extract** the archive (adjust the `filename` if needed):  

   ```bash
   tar -xzf backhaul_linux_amd64.tar.gz
   ``` 
3. **Run** the executable:  

   ```bash
   ./backhaul
   ```
4. You can also build from source if preferred:  

   ```bash
   git clone https://github.com/musixal/backhaul.git
   cd backhaul
   go build
   ./backhaul
   ```

## Usage

The main executable for this project is `backhaul`. It requires a TOML configuration file for both the server and client components.

### Configuration Options

To start using the solution, you'll need to configure both server and client components. Here’s how to set up basic configurations:

* **Server Configuration**

   Create a configuration file named `config.toml`:

    ```toml
    [server]# Local, IRAN
    bind_addr = "0.0.0.0:3080"    # Address and port for the server to listen on (mandatory).
    transport = "tcp"             # Protocol to use ("tcp", "tcpmux", "ws", "wss", "wsmux", "wssmux". mandatory).
    accept_udp = false             # Enable forwarding UDP alongside TCP on every mapped port. Works with the "tcp" transport (UDP over the raw TCP tunnel) and with "wsmux"/"wssmux" (UDP over the mux tunnel, which requires mux_version = 2 on both server and client). This is what lets a single wssmux tunnel also carry UDP services such as L2TP/IPsec (UDP 500/1701/4500) - list those UDP ports in `ports` and set accept_udp = true. (optional, default: false)
    token = "your_token"          # Authentication token for secure communication (required; must match on client and server).
    keepalive_period = 75         # Interval in seconds to send keep-alive packets.(optional, default: 75s)
    nodelay = false               # Enable TCP_NODELAY (optional, default: false).
    channel_size = 2048           # Tunnel and Local channel size. Excess connections are discarded. (optional, default: 2048).
    heartbeat = 40                # In seconds. Ping interval for tunnel stability. Min: 1s. (Optional, default: 40s)
    mux_con = 8                   # Mux concurrency. Number of connections that can be multiplexed into a single stream (optional, default: 8).
    mux_version = 1               # SMUX protocol version (1 or 2). Version 2 may have extra features. (optional)
    mux_framesize = 32768         # 32 KB. The maximum size of a frame that can be sent over a connection. (optional)
    mux_recievebuffer = 4194304   # 4 MB. The maximum buffer size for incoming data per connection. (optional)
    mux_streambuffer = 0          # The per-stream receive window: the most data smux will keep in flight on ONE stream before it must wait a full tunnel round-trip for the peer's window update. A stream therefore tops out at mux_streambuffer/RTT, so a small value is the throughput ceiling of every individual flow on mux_version = 2. Leave it unset/0 and it is derived as mux_recievebuffer / mux_con (4 MB / 8 = 512 KB by default), which is each stream's fair share of the session budget the pool already reserves - raising it costs no extra memory, because smux caps everything a connection buffers at mux_recievebuffer regardless. Set it explicitly only to override that. (optional, default: derived, floor 65536)
    mux_keepalive_disabled = false # wsmux/wssmux: disable smux's built-in per-connection keepalive ping. Every pool connection pings on the same fixed interval, which is a traffic-pattern signal; disabling it relies on TCP keepalive instead. (optional, default: false)
    mux_stripe = 1                 # wsmux/wssmux: split a single flow's data across this many pool connections instead of pinning it to one, so one flow isn't capped by a single connection's congestion window/RTT (the win on a lossy, high-RTT link). Must match on both server and client. 1 disables it (default). The earlier deadlock, tail data-loss, silent-truncation, and connection-pool leak bugs are fixed and covered by tests (incl. -race and end-to-end concurrent load). Still newer than the single-connection path; on its own (mux_stripe_parity = 0) it has no leg-failure recovery - if a pool connection drops mid-transfer the flow ends (surfacing as a reset, never silent corruption) - see mux_stripe_parity below to tolerate that. Because each striped flow is reassembled in order, one leg whose transport backs up can briefly head-of-line-stall that single flow under very high concurrency; it no longer degrades the whole tunnel. Raise it above 1 when the bottleneck is genuinely one long-lived throughput-bound flow. (optional, default: 1)
    mux_stripe_parity = 0          # wsmux/wssmux: Reed-Solomon parity legs added on top of mux_stripe, so up to this many of the mux_stripe+mux_stripe_parity pool legs carrying a striped flow can die mid-transfer (a CDN/LB resetting a pooled connection, a mobile handover) without the flow ending - the missing legs' data is reconstructed from the survivors instead. Requires mux_stripe >= 2 and must match on both server and client. Costs bandwidth (mux_stripe_parity/mux_stripe extra data sent) and a little latency (a chunk can't be delivered until mux_stripe of its shards arrive). 0 disables it (default, and the only value that changes nothing about the wire format).
    mux_half_close = false         # wsmux/wssmux (requires mux_version = 2): let a client half-close its side of a forwarded connection (send its request, then shut down its writing end) and still receive the full reply, which a plain smux stream cannot do (it has no directional EOF, so the shutdown ends both directions and a delayed reply is lost). Plain (non-promotable) flows then carry a small negotiated envelope (a 3-byte header per write), negotiated per upgrade with the `X-Backhaul-Cap: halfclose-v1` header that every current client sends. STRICT: with this on, the server rejects (HTTP 400, and the log names the fix) any upgrade from a client that does not send that capability, so upgrade the clients first. Server-only setting (the client needs no key). Flows that can be promoted mid-stream (`promote_bytes` > 0 together with `mux_stripe` >= 2) and striped flows keep today's full-close behavior. Ignored by every other transport. See "What is `mux_half_close`" in the FAQ. (optional, default: false)
    mux_ws_framing = true          # wsmux/wssmux: carry every mux connection as standards-framed WebSocket binary messages (RFC 6455), negotiated during the upgrade with the `Sec-WebSocket-Protocol: backhaul-mux-v1` header. STRICT: a server with this on rejects any wsmux/wssmux upgrade that does not offer that subprotocol (HTTP 400, and the log says to upgrade the client or set mux_ws_framing = false on both ends). Must match the client. Setting it to false selects the legacy raw mode (smux bytes written straight onto the connection after the HTTP upgrade), on both ends - see "wsmux/wssmux framing and upgrading" in the FAQ. Ignored by every other transport. (optional, default: true)
    sniffer = false               # Enable or disable network sniffing for monitoring data. (optional, default false)
    web_port = 2060               # Port number for the web interface or monitoring interface. (optional, set to 0 to disable).
    sniffer_log ="/root/log.json" # Filename used to store network traffic and usage data logs. (optional, default backhaul.json)
    tls_cert = "/root/server.crt" # Path to the TLS certificate file for wss/wssmux. (mandatory).
    tls_key = "/root/server.key"  # Path to the TLS private key file for wss/wssmux. (mandatory).
    tls_certs = []                # wss/wssmux: optional list of certificate files for serving MULTIPLE domains via SNI (e.g. one backhaul origin behind several CDNs/hostnames). Pairs by index with tls_keys. The server picks the cert whose names match the client's SNI, falling back to the first when none match. Works with both tls_engine values (including "openssl", via its SNI callback). Leave empty to use the single tls_cert/tls_key above. e.g. ["/root/a.crt", "/root/b.crt"]. (optional, default: none)
    tls_keys = []                 # wss/wssmux: private key files aligned by index with tls_certs. (optional, default: none)
    path = ""                     # Custom base path prepended to the /channel and /tunnel endpoints, for ws/wss/wsmux/wssmux. (optional, default: none)
    fallback = ""                 # ws/wss/wsmux/wssmux: host:port of a decoy web backend. Requests that aren't valid tunnel traffic (wrong/absent token, or any non-tunnel path such as "/") are reverse-proxied here instead of getting a 401, so a single backhaul origin can carry the tunnel under a secret path AND look like an ordinary website to probes - no separate nginx needed in the data path. e.g. "127.0.0.1:8080". (optional, default: none)
    tls_engine = "go"             # wss/wssmux: TLS stack that terminates the connection. "go" (default) uses Go's crypto/tls and keeps the pure-Go static binary. "openssl" terminates with the system OpenSSL so the server's TLS handshake fingerprint matches a same-version nginx (Go's stack e.g. always picks AES-128-GCM for TLS 1.3 where nginx/OpenSSL picks AES-256-GCM). "openssl" ONLY works in a binary built with `-tags openssl` (CGO_ENABLED=1); see the OpenSSL build note. Use it when the origin IP is directly reachable and could be fingerprinted. (optional, default: "go")
    max_conn_age = 0              # wsmux/wssmux, in seconds. Retire each pool connection once it reaches this age and replace it before it is used, so a CDN/LB max-connection-age reset never lands on a connection carrying traffic (the "close 1006 (abnormal closure): unexpected EOF" after a long uptime). Rotation is make-before-break in both directions: the aging connection keeps serving until a replacement has actually joined the pool (if the client cannot dial - blackholed edge IP, CDN refusing the upgrade - nothing is retired and the old connection stays in service, so the pool never shrinks), and once the replacement is up the retired one stops taking new streams but stays open until the streams already on it finish, so a long-lived flow (SSH, a big download) is not cut. Set it comfortably BELOW the max age of whatever fronts the server (ArvanCloud/Cloudflare-style CDNs typically reset well under an hour), e.g. 900 for 15 minutes. Server-side only; nothing to change on the client. 0 disables rotation (default) - a guessed value just churns connections for nothing, so set it deliberately. (optional, default: 0)
    log_level = "info"            # Log level ("panic", "fatal", "error", "warn", "info", "debug", "trace", optional, default: "info").
    skip_optz = true              # Skip optimizations performed by Backhaul (default: false)
    mss = 1360                    # TCP/TCPMux: Maximum Segment Size in bytes; controls max TCP payload size to avoid fragmentation. (default: system-defined)
    so_rcvbuf = 4194304           # TCP/TCPMux: Socket receive buffer size (bytes); larger buffer allows higher throughput on receive side. (default: system-defined)
    so_sndbuf = 1048576           # TCP/TCPMux: Socket send buffer size (bytes); controls send queue size to manage outgoing data flow. (default: system-defined)



    ports = [
    "443-600",                  # Listen on all ports in the range 443 to 600
    "443-600=5201",             # Listen on all ports in the range 443 to 600 and forward traffic to 5201
    "443-600=1.1.1.1:5201",     # Listen on all ports in the range 443 to 600 and forward traffic to 1.1.1.1:5201
    "443",                      # Listen on local port 443 and forward to remote port 443 (default forwarding).
    "4000=5000",                # Listen on local port 4000 (bind to all local IPs) and forward to remote port 5000.
    "127.0.0.2:443=5201",       # Bind to specific local IP (127.0.0.2), listen on port 443, and forward to remote port 5201.
    "443=1.1.1.1:5201",         # Listen on local port 443 and forward to a specific remote IP (1.1.1.1) on port 5201.
    "127.0.0.2:443=1.1.1.1:5201",  # Bind to specific local IP (127.0.0.2), listen on port 443, and forward to remote IP (1.1.1.1) on port 5201.
   ]

    ```

   To start the `server`:

   ```sh
   ./backhaul -c config.toml
   ```
* **Client Configuration**

   Create a configuration file named `config.toml` for the client:
   ```toml
   [client]  # Behind NAT, firewall-blocked
   remote_addr = "0.0.0.0:3080"  # Server address and port (mandatory).
   remote_addrs = []             # ws/wss/wsmux/wssmux: optional list of tunnel endpoints (e.g. the same origin fronted by several CDNs/domains). The connection pool spreads across all of them round-robin, so the tunnel aggregates every CDN at once instead of one. Each entry dials with its own host as the TLS SNI. Leave empty to use the single remote_addr above. e.g. ["aosky.ir:443", "nekocafe.sbs:443", "onionchips.sbs:443"]. (optional, default: none)
   edge_ip = "188.114.96.0"      # Edge IP used for CDN connection, specifically for WebSocket-based transports.(Optional, default none)
   edge_ips = []                 # ws/wss/wsmux/wssmux: optional edge IP to dial per remote_addrs entry (aligned by index); empty entries resolve/dial the domain directly. (optional, default: none)
   path = ""                     # Custom base path prepended to the /channel and /tunnel endpoints, for ws/wss/wsmux/wssmux. Must match the server. (optional, default: none)
   tls_verify = false            # wss/wssmux: verify the server's TLS certificate. Enabled by default, set to false for self-signed setups (but while off an on-path party can MITM the token-bearing handshake). (optional, default: true)
   transport = "tcp"             # Protocol to use ("tcp", "tcpmux", "ws", "wss", "wsmux", "wssmux". mandatory).
   token = "your_token"          # Authentication token for secure communication (required; must match on client and server).
   connection_pool = 8           # Number of pre-established connections.(optional, default: 8).
   aggressive_pool = false       # Enables aggressive connection pool management.(optional, default: false).
   keepalive_period = 75         # Interval in seconds to send keep-alive packets. (optional, default: 75s)
   nodelay = false               # Use TCP_NODELAY (optional, default: false).
   retry_interval = 3            # Retry interval in seconds (optional, default: 3s).
   dial_timeout = 10             # Sets the max wait time for establishing a network connection. (optional, default: 10s)
   mux_version = 1               # SMUX protocol version (1 or 2). Version 2 may have extra features. (optional)
   mux_framesize = 32768         # 32 KB. The maximum size of a frame that can be sent over a connection. (optional)
   mux_recievebuffer = 4194304   # 4 MB. The maximum buffer size for incoming data per connection. (optional)
   mux_streambuffer = 0          # The per-stream receive window; see the server config above. Leave it unset/0 to get the derived default (mux_recievebuffer / 8 = 512 KB), which is what keeps a single flow from being pinned to 64 KB per round-trip. (optional, default: derived, floor 65536)
   mux_keepalive_disabled = false # wsmux/wssmux: disable smux's built-in per-connection keepalive ping (see server config for details). (optional, default: false)
   mux_stripe = 1                 # wsmux/wssmux (prototype): split a single flow's data across this many pool connections (see server config for details). Must match the server. (optional, default: 1)
   mux_stripe_parity = 0          # wsmux/wssmux: Reed-Solomon parity legs on top of mux_stripe, tolerating that many dead legs per flow (see server config for details). Must match the server. (optional, default: 0)
   mux_ws_framing = true          # wsmux/wssmux: standards-framed WebSocket mux connections (see the server config for details). FAIL-LOUDLY: the client offers `backhaul-mux-v1` on every upgrade and, if the server does not confirm it, closes the connection and logs an error naming the fix - it never falls back to raw mode by itself. Must match the server. false selects the legacy raw mode. Ignored by every other transport. (optional, default: true)
   sniffer = false               # Enable or disable network sniffing for monitoring data. (optional, default false)
   web_port = 2060               # Port number for the web interface or monitoring interface. (optional, set to 0 to disable).
   sniffer_log ="/root/log.json" # Filename used to store network traffic and usage data logs. (optional, default backhaul.json)
   log_level = "info"            # Log level ("panic", "fatal", "error", "warn", "info", "debug", "trace", optional, default: "info").
   skip_optz = true              # Skip optimizations performed by Backhaul (default: false)
   mss = 1360                    # TCP/TCPMux/WS/WSS/WSMux/WSSMux (client): Maximum Segment Size in bytes; controls max TCP payload size to avoid fragmentation. Previously silently ignored for ws/wss/wsmux/wssmux - now respected the same as TCP/TCPMux. (default: system-defined)
   so_rcvbuf = 1048576           # TCP/TCPMux/WS/WSS/WSMux/WSSMux (client): Socket receive buffer size (bytes) for the pool/tunnel connections; larger buffer allows higher throughput on receive side. Previously silently ignored for ws/wss/wsmux/wssmux, which always used a fixed 1MB or 2MB regardless of this setting - now respected the same as TCP/TCPMux. (default: system-defined)
   so_sndbuf = 4194304           # TCP/TCPMux/WS/WSS/WSMux/WSSMux (client): Socket send buffer size (bytes) for the pool/tunnel connections; controls send queue size to manage outgoing data flow. Same ws/wss/wsmux/wssmux caveat as so_rcvbuf above. (default: system-defined)
   ```

   To start the `client`:

   ```sh
   ./backhaul -c config.toml
   ```

### Detailed Configuration
#### TCP Configuration
* **Server**:

   ```toml
   [server]
   bind_addr = "0.0.0.0:3080"
   transport = "tcp"
   accept_udp = false 
   token = "your_token"
   keepalive_period = 75  
   nodelay = true 
   heartbeat = 40 
   channel_size = 2048
   sniffer = false 
   web_port = 2060
   sniffer_log = "/root/backhaul.json"
   log_level = "info"
   ports = []
   ```
* **Client**:

   ```toml
   [client]
   remote_addr = "0.0.0.0:3080"
   transport = "tcp"
   token = "your_token" 
   connection_pool = 8
   aggressive_pool = false
   keepalive_period = 75
   dial_timeout = 10
   nodelay = true 
   retry_interval = 3
   sniffer = false
   web_port = 2060 
   sniffer_log = "/root/backhaul.json"
   log_level = "info"

   ```
* **Details**:

   `remote_addr`: The IPv4, IPv6, or domain address of the server to which the client connects.

   `token`: An authentication token used to securely validate and authenticate the connection between the client and server within the tunnel.

   `channel_size`: The queue size for forwarding packets from server to the client. If the limit is exceeded, packets will be dropped.

   `connection_pool`: Set the number of pre-established connections for better latency.
   
   `nodelay`: Refers to a TCP socket option (TCP_NODELAY) that improve the latency but decrease the bandwidth


#### TCP Multiplexing Configuration
* **Server**:

   ```toml
   [server]
   bind_addr = "0.0.0.0:3080"
   transport = "tcpmux"
   token = "your_token" 
   keepalive_period = 75
   nodelay = true 
   heartbeat = 40 
   channel_size = 2048
   mux_con = 8
   mux_version = 1
   mux_framesize = 32768 
   mux_recievebuffer = 4194304
   mux_streambuffer = 0          # derived (mux_recievebuffer / mux_con); see the option reference
   sniffer = false 
   web_port = 2060
   sniffer_log = "/root/backhaul.json"
   log_level = "info"
   ports = []
   ```
* **Client**:

   ```toml
   [client]
   remote_addr = "0.0.0.0:3080"
   transport = "tcpmux"
   token = "your_token" 
   connection_pool = 8
   aggressive_pool = false
   keepalive_period = 75
   dial_timeout = 10
   retry_interval = 3
   nodelay = true 
   mux_version = 1
   mux_framesize = 32768 
   mux_recievebuffer = 4194304
   mux_streambuffer = 0          # derived (mux_recievebuffer / mux_con); see the option reference
   sniffer = false 
   web_port = 2060
   sniffer_log = "/root/backhaul.json"
   log_level = "info"
   ```
* **Details**:

   `mux_session`: Number of multiplexed sessions. Increase this if you need to handle more simultaneous sessions over a single connection.
   
   * Refer to TCP configuration for more information.


#### UDP Configuration
* **Server**:

   ```toml
   [server]
   bind_addr = "0.0.0.0:3080"
   transport = "udp"
   token = "your_token"
   heartbeat = 20 
   channel_size = 2048
   sniffer = false 
   web_port = 2060
   sniffer_log = "/root/backhaul.json"
   log_level = "info"
   ports = []
   ```
* **Client**:

   ```toml
   [client]
   remote_addr = "0.0.0.0:3080"
   transport = "udp"
   token = "your_token" 
   connection_pool = 8
   aggressive_pool = false
   retry_interval = 3
   sniffer = false
   web_port = 2060 
   sniffer_log = "/root/backhaul.json"
   log_level = "info"

   ```
   
#### WebSocket Configuration
* **Server**:

   ```toml
   [server]
   bind_addr = "0.0.0.0:8080"
   transport = "ws"
   token = "your_token" 
   channel_size = 2048
   keepalive_period = 75 
   heartbeat = 40
   nodelay = true 
   sniffer = false 
   web_port = 2060
   sniffer_log = "/root/backhaul.json"
   log_level = "info"
   ports = []
   ```

* **Client**:

   ```toml
   [client]
   remote_addr = "0.0.0.0:8080"
   edge_ip = "" 
   transport = "ws"
   token = "your_token" 
   connection_pool = 8
   aggressive_pool = false
   keepalive_period = 75 
   dial_timeout = 10
   retry_interval = 3
   nodelay = true 
   sniffer = false 
   web_port = 2060
   sniffer_log = "/root/backhaul.json"
   log_level = "info"
   ```

* **Details**:

   * Refer to TCP configuration for more information.

#### Secure WebSocket Configuration
* **Server**:

   ```toml
   [server]
   bind_addr = "0.0.0.0:8443"
   transport = "wss"
   token = "your_token" 
   channel_size = 2048
   keepalive_period = 75 
   nodelay = true 
   tls_cert = "/root/server.crt"      
   tls_key = "/root/server.key"
   sniffer = false 
   web_port = 2060
   sniffer_log = "/root/backhaul.json"
   log_level = "info"
   ports = []
   ```

* **Client**:

   ```toml
   [client]
   remote_addr = "0.0.0.0:8443"
   edge_ip = "" 
   transport = "wss"
   token = "your_token" 
   connection_pool = 8
   aggressive_pool = false
   keepalive_period = 75
   dial_timeout = 10
   retry_interval = 3  
   nodelay = true 
   sniffer = false 
   web_port = 2060
   sniffer_log = "/root/backhaul.json"
   log_level = "info"
   ```

* **Details**:

   * Refer to the next section for instructions on generating `tls_cert` and `tls_key`.


#### WS Multiplexing Configuration
* **Server**:

   ```toml
   [server]
   bind_addr = "0.0.0.0:3080"
   transport = "wsmux"
   token = "your_token" 
   keepalive_period = 75
   nodelay = true 
   heartbeat = 40 
   channel_size = 2048
   mux_con = 8
   mux_version = 1
   mux_framesize = 32768 
   mux_recievebuffer = 4194304
   mux_streambuffer = 0          # derived (mux_recievebuffer / mux_con); see the option reference
   sniffer = false 
   web_port = 2060
   sniffer_log = "/root/backhaul.json"
   log_level = "info"
   ports = []
   ```
* **Client**:

   ```toml
   [client]
   remote_addr = "0.0.0.0:3080"
   edge_ip = "" 
   transport = "wsmux"
   token = "your_token" 
   connection_pool = 8
   aggressive_pool = false
   keepalive_period = 75
   dial_timeout = 10
   nodelay = true
   retry_interval = 3
   mux_version = 1
   mux_framesize = 32768 
   mux_recievebuffer = 4194304
   mux_streambuffer = 0          # derived (mux_recievebuffer / mux_con); see the option reference
   sniffer = false 
   web_port = 2060
   sniffer_log = "/root/backhaul.json"
   log_level = "info"
   ```

#### WSS Multiplexing Configuration
* **Server**:

   ```toml
   [server]
   bind_addr = "0.0.0.0:443"
   transport = "wssmux"
   token = "your_token" 
   keepalive_period = 75
   nodelay = true 
   heartbeat = 40 
   channel_size = 2048
   accept_udp = false            # Also forward UDP on each mapped port over the mux tunnel (requires mux_version = 2 on both ends). Enables carrying UDP services such as L2TP/IPsec over wssmux.
   mux_con = 8
   mux_version = 1
   mux_framesize = 32768 
   mux_recievebuffer = 4194304
   mux_streambuffer = 0          # derived (mux_recievebuffer / mux_con); see the option reference
   tls_cert = "/root/server.crt"      
   tls_key = "/root/server.key"
   sniffer = false 
   web_port = 2060
   sniffer_log = "/root/backhaul.json"
   log_level = "info"
   ports = []
   ```
* **Client**:

   ```toml
   [client]
   remote_addr = "0.0.0.0:443"
   edge_ip = "" 
   transport = "wssmux"
   token = "your_token" 
   keepalive_period = 75
   dial_timeout = 10
   nodelay = true
   retry_interval = 3
   connection_pool = 8
   aggressive_pool = false
   mux_version = 1
   mux_framesize = 32768 
   mux_recievebuffer = 4194304
   mux_streambuffer = 0          # derived (mux_recievebuffer / mux_con); see the option reference
   sniffer = false 
   web_port = 2060
   sniffer_log = "/root/backhaul.json"
   log_level = "info"
   ```

## Generating a Self-Signed TLS Certificate with OpenSSL

To generate a TLS certificate and key, you can use tools like OpenSSL. Here’s a step-by-step guide on how to create a self-signed certificate and key using OpenSSL:

### Step 1: Install OpenSSL

If you don't already have OpenSSL installed, you can install it using your system's package manager.

- **On Ubuntu/Debian**:
  ```bash
  sudo apt-get install openssl
  ```
### Step 2: Generate a Private Key
To generate a 2048-bit RSA private key, run the following command:
  ```bash
openssl genpkey -algorithm RSA -out server.key -pkeyopt rsa_keygen_bits:2048
  ```
This will create a file named `server.key`, which is your private key.
### Step 3: Generate a Certificate Signing Request (CSR)

Create a Certificate Signing Request (CSR) using the private key. This CSR is used to generate the SSL certificate:
  ```bash
openssl req -new -key server.key -out server.csr
  ```

You will be prompted to enter information for the CSR. For the common name (CN), use the domain name or IP address where your server will be hosted. Example:
```
Country Name (2 letter code) [AU]:US
State or Province Name (full name) [Some-State]:California
Locality Name (eg, city) []:San Francisco
Organization Name (eg, company) [Internet Widgits Pty Ltd]:Your Company Name
Organizational Unit Name (eg, section) []:
Common Name (e.g. server FQDN or YOUR name) []:example.com
Email Address []:
```

### Step 4: Generate a Self-Signed Certificate

Use the CSR and private key to generate a self-signed certificate. Specify the validity period (in days):
  ```bash
openssl x509 -req -in server.csr -signkey server.key -out server.crt -days 365
  ```
This will generate a certificate named `server.crt`, valid for 365 days.
### Recap of the Files Generated:

* `server.key`: Your private key.
* `server.csr`: The certificate signing request (used to generate the certificate).
* `server.crt`: Your self-signed TLS certificate.

## Running backhaul as a service

To create a service file for your backhaul project that ensures the service restarts automatically, you can use the following template for a systemd service file. Assuming your project runs a reverse tunnel and the main executable file is located in a certain path, here's a basic example:

1. Create the service file `/etc/systemd/system/backhaul.service`:

```ini
[Unit]
Description=Backhaul Reverse Tunnel Service
After=network.target

[Service]
Type=simple
ExecStart=/root/backhaul -c /root/config.toml
Restart=always
RestartSec=3
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
```
2. After creating the service file, enable and start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable backhaul.service
sudo systemctl start backhaul.service
```
3. To verify if the service is running:
```bash
sudo systemctl status backhaul.service
```
4. View the most recent log entries for the backhaul.service unit:
```bash
journalctl -u backhaul.service -e -f
```

## FAQ

**Q: How do I decide which transport protocol to use?**

* `tcp`: Use if you need straightforward TCP connections.
* `tcpmux`: Use if you need to handle multiple sessions over a single connection.
* `ws`: Use if you need to traverse HTTP-based firewalls or proxies.
* `wss`: Use this for secure WebSocket connections that need to traverse HTTP-based firewalls or proxies. It encrypts data for added security, similar to WS but with encryption. Its TLS ClientHello is generated with uTLS to mimic Chrome, for CDN edges/DPI that fingerprint the TLS handshake itself.

**Q: My CPU saturates (~95%) on a low-core box and I can't reach line rate (e.g. 1Gbps). Why, and what actually helps?**

On these boxes the per-byte CPU cost is almost entirely **TLS/AES**, not moving bytes. The right fix depends on whether you're free to choose the transport or locked into WebSocket-over-TLS by a CDN.

*If you control both ends and don't need a CDN/HTTP front:* prefer `tcp` or `tcpmux`. With those, both ends of each forwarded connection are raw `*net.TCPConn`, so the data pump routes straight to `splice(2)` on Linux - bytes move kernel-side without ever entering this process, so backhaul's own per-byte CPU cost is effectively zero (`ws`/`wss`/`wsmux`/`wssmux` cannot take that path). And don't wrap already-encrypted traffic (e.g. an xray/VLESS/Reality payload) in a second TLS layer - that extra AES pass costs CPU without adding confidentiality.

*If you're behind a CDN (Cloudflare, etc.) and must use `ws`/`wss`/`wsmux`/`wssmux` for obfuscation:* `tcp` and `splice` are off the table, so the levers are:

* **Check AES-NI first:** `grep -m1 -o aes /proc/cpuinfo`. Cheap VPS instances often don't expose it to the guest, and without it TLS termination is several times more expensive - frequently the entire reason a 2-core box stalls below 1Gbps. If it's present, make sure the negotiated cipher is AES-GCM (hardware-accelerated) rather than ChaCha20.
* **Get nginx out of the tunnel's data path** - it's usually the real bottleneck. When nginx fronts the tunnel to serve a decoy site under the same hostname (path-based camouflage), it must terminate TLS and reverse-proxy the WebSocket, and it has **no zero-copy path for a proxied WebSocket** - it copies every byte in userspace, burning roughly a core per Gbps. On a 2-core box, a single ~1Gbps flow then eats one core in nginx and another in backhaul, saturating both. Point the CDN/origin **straight at backhaul** (`transport = "wss"`/`"wssmux"`, backhaul terminates the origin TLS itself) and let the `fallback` option (below) preserve the decoy - so the camouflage stays but nginx no longer copies the tunnel's bytes.
* **Trim nginx waste on the tunnel vhost:** `access_log off;`, `gzip off;`, `ssl_protocols TLSv1.2 TLSv1.3;` only, and `worker_cpu_affinity auto;` so workers spread across every core. Keep `proxy_buffering off;` - that's correct for a streamed tunnel.
* **Upload much slower than download on `wsmux`/`wssmux`?** On `mux_version = 2` smux adds *per-stream* flow control: a stream may have at most `mux_streambuffer` bytes in flight before it blocks waiting for the peer's window update, which costs a full tunnel round-trip. One stream therefore tops out at `mux_streambuffer / RTT` no matter how much bandwidth the path has - at the old fixed 64 KB and a 30 ms tunnel that is ~2.2 MB/s (~18 Mbps) per stream, measured, while the connection was already allowed to buffer `mux_recievebuffer` (4 MB). That ceiling is why raising socket buffers or `so_sndbuf` on the tunnel legs never moved upload: smux was never letting more than 64 KB per stream reach those sockets. `mux_streambuffer` now defaults to `mux_recievebuffer / mux_con` (512 KB) instead, which is each stream's fair share of the budget the pool already reserves. If you pinned `mux_streambuffer = 65536` in your config (older sample configs did), **set it to 0** to pick the derived value up. Memory does not grow either way - smux caps everything a connection buffers at `mux_recievebuffer` regardless of the per-stream window.

* **`mux_*` settings only apply to the mux transports.** On plain `ws`/`wss` (and `tcp`), `mux_con`, `mux_framesize`, `mux_recievebuffer`, `mux_streambuffer`, `mux_stripe` and `mux_stripe_parity` are silently ignored - each forwarded connection is its own WebSocket. If you set large mux buffers expecting them to take effect, switch the transport to `wsmux`/`wssmux`. Note that `wsmux`/`wssmux` do NOT necessarily pass through a CDN "exactly like `ws`/`wss`": `ws`/`wss` send every forwarded connection as ordinary WebSocket messages, while the mux transports multiplex many connections over each WebSocket, and only with `mux_ws_framing = true` (the default) is every byte on the wire a standards-framed WebSocket message. See "`wsmux`/`wssmux` framing and upgrading" below.
* **`wsmux`/`wssmux` setup limits.** Every accepted user connection has a fixed 3-second budget, counted from the moment it was accepted, to be handed to a tunnel stream: time spent queued, waiting for the pool to grow, retrying and writing its header all come out of that one budget, and a retry never restarts it. When it runs out the user connection is closed. The number of connections being set up at once is capped at `channel_size` (the rest wait in the queue, and the queue itself drops what does not fit); a running transfer does not count against that cap, and the per-flow admission limit (`mux_con` streams per pool connection) is unchanged. Waiting connections share a single request for a bigger pool instead of each asking on its own. smux cannot cancel a stream open that is already in progress (it can take up to its own 30-second internal timeout when a pool connection has stalled): the user connection is still closed at 3 seconds, but the setup's bookkeeping is released only when that open returns, and a stream that arrives late is closed unused. No pool connection is closed to cancel one flow. On the client, the first bytes of every stream the server opens (flow kind and header) must arrive within `dial_timeout`, otherwise the stream is closed; headers are read one stream at a time, so a stalled header can delay the streams behind it by up to that long. A flow kind the client does not know closes the stream.
* **`wsmux`/`wssmux` pool diagnostics.** The server's existing `<path>/pool` and `<path>/diag` endpoints (both need `Authorization: Bearer <token>`; without it they answer 401 like any other probe) keep every JSON key they had and gain one nested `diagnostics` object. It reports ownership state the transport already tracks and adds no counters of its own: `control_state` (`connected`, `reconnecting` while the control channel is lost and the pool is held for reattach, `restarting`, or `waiting` before the first control channel) with `control_grace_elapsed_ms` while a loss is being held; `sessions_owned` (open pool connections the current run owns), `sessions_eligible` (registered and open: what new flows can be placed on), `sessions_queued` (upgraded, waiting for admission) and `sessions_draining` (owned and open but no longer eligible, i.e. rotated out and finishing its streams; derived, so approximate); `setups_active`, `setups_queued`, `setup_limit` and `setups_saturated` (connections being set up versus the `channel_size` ceiling, and those waiting behind it); `pending_opens` (stream opens that have picked a pool connection and not returned yet); `replacement_waiting` (a rotation is waiting for its replacement connection). A value the transport does not track is omitted rather than guessed; there is no generation ID. The fields are read from independently locked parts one after another, so the snapshot is best effort and not a consistent global view: while things are changing they can disagree for a moment (never negative). Taking a snapshot does no network I/O and records nothing. `/diag` events stay a ring of the latest 128 and now also record the end of a control-loss hold (`control_reattached`) and, once per rotation, `rotation_retired` (plus a single `rotation_deferred` when the replacement is late); none of the new fields or events contains the token, the Authorization header, the configured path, payload bytes or where a connection was going. `per_cdn`'s `cdn` is the remote IP a pool connection arrived from, not a verified configured endpoint or provider: two providers behind one IP group together and one provider behind several IPs is split.
* **If one core is pinned while the other is idle** (a single big flow bottlenecked on one connection's copy/decrypt), the mux transports' `mux_stripe = N` splits that one flow across N pooled connections so it can use more than one core - or, more commonly on an intercontinental link, across N congestion windows so per-flow packet loss doesn't cap it. The earlier deadlock and tail data-loss bugs are fixed (see the option docs). If *both* cores are already saturated (aggregate-bound), striping won't help anyway - you're out of CPU, so reduce per-byte cost with the points above instead.
  * **Striping targets a *single* fat flow, not many connections.** Each striped flow is reassembled in order across its legs, so a leg whose transport backs up (smux flow control filling under load) can head-of-line-block *that one flow* until the byte it's carrying arrives. That's inherent to reassembling one ordered stream across independent legs, so the gain is on a single long-lived, throughput-bound flow on a lossy/high-RTT link - not on many short parallel connections. (A separate bug where striping never released connection-pool slots, so heavy concurrent load leaked smux sessions until the whole tunnel stalled, is fixed - concurrency no longer degrades the tunnel, though a single flow can still see the occasional head-of-line latency spike.) When the workload is many connections rather than one fat flow, `mux_stripe = 1` (the default) is the right choice.
* **A flow resetting mid-transfer on `wsmux`/`wssmux` even though the tunnel underneath is TCP** is almost never real packet loss - TCP already hides that by retransmitting. It's a whole pooled *connection* dying (a CDN/LB idle-killing or max-age-resetting a WebSocket, a mobile network handover), which drops whatever byte range was in flight on it. Plain `mux_stripe` has no recovery for that - the flow just ends. `mux_stripe_parity = N` (with `mux_stripe >= 2`, matched on both sides) adds N Reed-Solomon parity legs so up to N of the `mux_stripe + mux_stripe_parity` legs carrying a flow can die mid-transfer and the flow keeps going, reconstructed from the survivors - at the cost of N/mux_stripe extra bandwidth and a little added latency per chunk.

**Q: What is `mux_ws_framing` (`wsmux`/`wssmux` framing), and how do I upgrade to it?**

*What it changes.* Older builds used WebSocket only as an HTTP upgrade vector for `wsmux`/`wssmux`: after the `101 Switching Protocols` response the tunnel legs carried raw smux bytes, which are **not** WebSocket frames (the control channel always used real frames). That works when nothing in the path looks past the upgrade (a direct connection, a TCP-passthrough proxy), but an intermediary that parses or reassembles WebSocket messages (a CDN in WebSocket-proxy mode) can reject or corrupt such a stream - so the old claim that `wsmux`/`wssmux` pass through a CDN exactly like `ws`/`wss` was not correct. With `mux_ws_framing = true` (the default) every mux tunnel leg is a stream of RFC 6455 binary messages: one message per smux write, client-to-server frames masked, server-to-client frames unmasked, Ping answered with a Pong that echoes its payload, Close answered with a Close. It is negotiated in the HTTP upgrade with `Sec-WebSocket-Protocol: backhaul-mux-v1` on every `wsmux`/`wssmux` upgrade (control channel and tunnel legs). Control-channel frames, `mux_version` and plain `ws`/`wss` are unchanged.

*Strict, and it never falls back silently.*

| client \ server | `mux_ws_framing = true` | `mux_ws_framing = false` |
|---|---|---|
| **true** (new) | framed, works | client closes the connection and logs `server did not confirm the backhaul-mux-v1 websocket subprotocol: ... set mux_ws_framing=false on both ends` on every attempt |
| **false**, or a build without the option | server rejects the upgrade with HTTP 400 and logs the fix (unauthorized requests and the `fallback` decoy behave exactly as before - the check comes after authorization) | legacy raw, works as before |

Each side logs its mode once at startup: `wsmux framing: standards-framed (backhaul-mux-v1)` or `wsmux framing: legacy raw (mux_ws_framing=false)`.

*Upgrading is a flag day.* The server and every client must run the new build; a mixed set will not work, by design (that is what "strict" and "fail loudly" mean - the alternative is a stream that silently corrupts). To keep the old behaviour while you roll out, set `mux_ws_framing = false` in **both** the server and the client configs (on the old build the key is simply ignored). To roll back after upgrading, set `mux_ws_framing = false` on both ends (or reinstall the previous build on both). Do not leave it on for one end only.

*What is and is not verified.* The framing is validated locally: the test suite parses every outer frame with the gobwas/ws frame parser for both plain `wsmux` and `wssmux` with a locally issued, fully verified certificate, checks the mask bit in each direction, and feeds the payloads to smux. **No specific CDN has been tested**; whether a given CDN carries the tunnel is still something to verify in your deployment. Framing costs 2-14 header bytes per smux write, client-side masking, and one WebSocket message per smux frame; on a CPU-bound loopback benchmark (no network latency, 4 streams of 32 MiB, smux frames of 32 KiB) throughput was within about 10% of the raw legacy mode (server-to-client roughly 10% lower, client-to-server within measurement noise); the benchmark is `BenchmarkSmuxLeg` in `internal/utils/network`. The legacy raw mode remains available only for paths where no intermediary inspects WebSocket frames.

**Q: What is `mux_half_close`, and when do I need it?**

*The problem.* An application that sends its whole request, shuts down its writing end (TCP half-close) and only then expects the reply works over a direct connection, plain `tcp`, `tcpmux`, `ws` and `wsmux` flows whose legs are real TCP. But on a `wsmux`/`wssmux` tunnel each forwarded connection is one smux stream, and smux (as pinned here) has no directional close: closing the stream to signal "no more request bytes" also ends the reverse direction, so a reply that is still to come is lost. That is the legacy behavior and it is unchanged unless you opt in.

*The opt-in.* `mux_half_close = true` in the **server** config (`wsmux`/`wssmux` with `mux_version = 2`; the server refuses to start otherwise). Plain flows then carry a per-stream envelope of `type (1 byte) | length (2 bytes, big-endian) | payload`, with `DATA` (0x01, 1-32768 bytes), `END` (0x02, directional EOF) and `ABORT` (0x03, the sender abandoned the flow) records, so each direction ends independently and an aborted or truncated flow reaches the other end as an error, never as a clean end of stream. The cost is a 3-byte header and one extra buffer copy per write, on those flows only.

*Negotiation is strict and separate from `mux_ws_framing`.* Every current `wsmux`/`wssmux` client sends `X-Backhaul-Cap: halfclose-v1` on every upgrade (control channel and tunnel legs), whatever its own settings; a server without the option ignores it. A server with `mux_half_close = true` opens the new flow kind (`0x06`) only on sessions that offered the capability, and rejects every upgrade that did not (HTTP 400; the log says to upgrade the client or set `mux_half_close = false`). It is a different header from the `Sec-WebSocket-Protocol: backhaul-mux-v1` token of `mux_ws_framing`: a client offers both when framing is on, and either check can reject on its own with its own message. Authorization is checked first, so unauthorized requests and the `fallback` decoy behave as before.

| client \ server | `mux_half_close = false` (default) | `mux_half_close = true` |
|---|---|---|
| old build (no header) | works, legacy full-close semantics | upgrade rejected with a clear error (control channel and tunnel) |
| current build | works, legacy semantics (the offer is ignored) | works; plain non-promotable flows use the envelope |

*Limits.* Flows that can be promoted mid-stream (`promote_bytes` > 0 with `mux_stripe` >= 2, on a port that is not striped) keep the legacy `FlowPlain` kind and its full-close behavior, so a request-EOF-then-reply pattern is not protected on those ports; striped flows keep their own end-of-stream framing; plain `ws`/`wss`/`tcp`/`tcpmux` are untouched. *Rollback:* set `mux_half_close = false` on the server; current clients keep working. *Rollout:* upgrade every client first (an old client is rejected the moment the server turns the option on), then enable it on the server.

**Q: But I need the decoy site for obfuscation - that's why nginx is there. How do I drop it without losing camouflage?**

Use the server's `fallback` option so backhaul itself is the CDN origin and plays both roles:

```toml
[server]
bind_addr = "0.0.0.0:443"          # backhaul is the origin, terminates TLS itself
transport = "wss"                  # or "wssmux"
tls_cert = "/root/certs/site.crt"  # reuse the cert nginx was using
tls_key  = "/root/certs/site.key"
path = "/your-secret-path"         # tunnel lives here
fallback = "127.0.0.1:8080"        # everything else -> your decoy site
token = "..."
ports = ["..."]
```

Run your decoy web server on `127.0.0.1:8080` (a tiny static site is fine). Now:

* A real WebSocket client with the token, hitting `path`, gets the tunnel.
* A probe/crawler/browser hitting `/` (or any other path, or without the token) is reverse-proxied to the decoy and sees a normal website - the same camouflage nginx gave you, minus the 401 tell.

Because the tunnel bytes now flow **straight through backhaul** instead of being copied by an extra nginx hop, this is the change that most directly frees a CPU core on a low-core box. The decoy traffic still passes through a local server, but that's a negligible trickle - the 1Gbps of tunnel traffic no longer touches it. (Path-based camouflage inherently needs an L7/TLS-terminating router; `fallback` just makes backhaul be that router instead of a separate nginx.)

**Q: If backhaul terminates TLS itself, doesn't its handshake fingerprint differ from nginx's - and give it away to a censor probing my origin directly?**

Yes - and that's a real limitation of the default build, worth being precise about. When backhaul terminates `wss`/`wssmux` with Go's `crypto/tls`, its ServerHello differs from nginx's: most concretely, for TLS 1.3 Go's stack always selects `AES-128-GCM` when AES-NI is present and does not expose TLS 1.3 ciphersuite ordering, whereas nginx (OpenSSL) selects `AES-256-GCM`. A censor who can reach your origin IP directly (e.g. because it also serves users on another port) can fingerprint that difference.

Two things determine whether it matters:

* **If your origin is only reachable through a CDN** (its real IP isn't exposed), the censor sees the CDN's TLS, not backhaul's - so the default Go engine is fine and none of the below is needed.
* **If your origin IP is directly reachable**, use the OpenSSL TLS engine: set `tls_engine = "openssl"` and run a binary built with OpenSSL. Because it terminates with the *same* OpenSSL library nginx links, the ServerHello - cipher selection and extension order - matches nginx's. (`internal/utils/network` ships a test that asserts the OpenSSL engine negotiates `TLS_AES_256_GCM_SHA384` exactly like nginx while the Go engine negotiates `TLS_AES_128_GCM_SHA256`.)

Honest caveats for the OpenSSL engine:

* It requires cgo, so the binary is **no longer a pure-Go static one**: build with `CGO_ENABLED=1 go build -tags openssl`, and the target servers need a compatible `libssl` installed. Build against and run on the **same OpenSSL major version** your nginx uses for the closest match.
* nginx's `http2` directive also advertises `h2` in ALPN; this listener speaks HTTP/1.1 only (the WebSocket tunnel needs it), so for tightest parity drop `http2` from any nginx you compare against.
* It closes the biggest, directly-observable gap (the TLS 1.3 cipher), but "byte-identical to nginx in every respect" is not a claim to lean on - verify your own setup with a fingerprinting tool (e.g. JARM) against a real nginx before relying on it.

Building the OpenSSL variant:

```bash
# needs a C toolchain and libssl headers, e.g. on Debian/Ubuntu:
#   apt-get install -y gcc libssl-dev
CGO_ENABLED=1 go build -tags openssl -o backhaul .
```

The default `go build` (and the released binaries) remain pure-Go and static; `tls_engine = "openssl"` in one of those exits with a clear error rather than silently downgrading.

**Q: My link is high-latency / intercontinental and throughput stalls well below the line rate even though CPU is fine. What helps?**

Enable **BBR** congestion control (`net.ipv4.tcp_congestion_control=bbr` with `net.core.default_qdisc=fq`). On a high-RTT, mildly-lossy path the default cubic/reno collapses its window on every stray loss and never fills the pipe. BBR paces to the measured bottleneck bandwidth instead, at no extra CPU cost. Backhaul enables this automatically at startup unless `skip_optz = true`. Also size `so_rcvbuf`/`so_sndbuf` to the bandwidth-delay product of the link (e.g. ~8MB for 1Gbps at ~40ms RTT) so the window has room to open.


## Benchmark

For in-depth information, please visit the dedicated [Benchmark page](./benchmark/).

**Built-in speedtest** (`wsmux`/`wssmux` with `mux_version ≥ 2`): hit `GET /speedtest?dir=both&scope=all&seconds=10` on the server's `web_port`. Two measurement concepts deliberately differ:

- **Per-path rate** (`per_cdn[].down_mbps` / `up_mbps`): bytes received by the far end divided by the *receiver-measured* data-window elapsed — the goodput of that individual connection, excluding setup overhead.
- **Aggregate rate** (`total_down_mbps` / `total_up_mbps`): sum of all receivers' bytes divided by the *local phase wall time* (from just before launching all concurrent goroutines to after all return), which includes launch and drain overhead. This is the effective whole-phase throughput and will differ from the sum of per-path rates by design — it captures real contention on shared bottlenecks rather than an inflated arithmetic sum of isolated windows.

Requested `seconds` is the workload duration only, never the rate denominator.


## License

This project is licensed under the AGPL-3.0 license. See the LICENSE file for details.

## Donation

Donate TRX (TRC-20) to support our project:
``` wallet
TMVBGzX4qpt12R1qWsJMpT1ttoKH1kus1H
```
Thanks for your support! 

## Stargazers over time
[![Stargazers over time](https://starchart.cc/Musixal/Backhaul.svg?variant=light)](https://starchart.cc/Musixal/Backhaul)
