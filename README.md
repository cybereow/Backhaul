# Backhaul

A reverse tunnel for getting at services behind NAT and firewalls, built to carry
many concurrent connections. A **server** (publicly reachable) listens on the
ports you map; a **client** (behind the NAT) dials out to it, and every
connection to a mapped port is carried through that outbound tunnel to the
target the client can reach.

```
 user ──► server :443 ══ tunnel ══ client ──► 127.0.0.1:8080
          (public)     (ws/wss/wsmux/wssmux/dns/dnsmux)   (behind NAT)
```

## Contents

- [Transports](#transports)
- [Install](#install)
- [Quick start](#quick-start)
- [Configuration reference](#configuration-reference)
- [Port mappings](#port-mappings)
- [TLS (wss / wssmux)](#tls-wss--wssmux)
- [Hiding behind a decoy site](#hiding-behind-a-decoy-site)
- [DNS transport (dns / dnsmux)](#dns-transport-dns--dnsmux)
- [Multiplexing, rotation and resume (wsmux / wssmux)](#multiplexing-rotation-and-resume-wsmux--wssmux)
- [Running as a service](#running-as-a-service)
- [Tuning and troubleshooting](#tuning-and-troubleshooting)
- [Development and releases](#development-and-releases)
- [Benchmark](#benchmark)
- [License](#license)

## Transports

Exactly six transports are supported. `transport` must be the same on both ends.

| transport | carrier | multiplexed | encrypted | typical use |
|---|---|---|---|---|
| `ws` | WebSocket over TCP | no — one WebSocket per forwarded connection | no | behind a CDN/reverse proxy that terminates TLS |
| `wss` | WebSocket over TLS | no | yes | direct or CDN-fronted, one TLS connection per flow |
| `wsmux` | WebSocket over TCP + smux | yes — many flows per WebSocket | no | as `ws`, with far fewer connections |
| `wssmux` | WebSocket over TLS + smux | yes | yes | the general-purpose choice: CDN-friendly, encrypted, pooled |
| `dns` / `dnsmux` | DNS queries through recursive resolvers + smux | yes (always) | **no** (authenticated only) | networks where nothing else gets through; very low throughput |

Notes:

- `dns` and `dnsmux` are the same transport: the DNS carrier is always
  multiplexed, so either name selects it.
- Plain `ws` / `wsmux` send the upgrade request and the token in the clear. Use
  them only behind something that terminates TLS in front of the origin.
- `wss` / `wssmux` clients present a Chrome-like TLS ClientHello (uTLS) and
  browser-like upgrade headers; the server terminates TLS with **OpenSSL** (see
  [TLS](#tls-wss--wssmux)).
- The former `tcp`, `tcpmux` and `udp` transports were removed. A config that
  names one is rejected at startup.
- UDP services can still be forwarded: set `accept_udp = true` on a `wsmux` /
  `wssmux` server (needs `mux_version = 2` on both ends).

## Install

Backhaul is built for **linux/amd64** only. The server terminates TLS with the
system OpenSSL (via cgo), so the binary is dynamically linked against
`libssl.so.3` (OpenSSL 3.x) and the host needs that library.

### Release binary

```bash
# Debian/Ubuntu: apt-get install -y libssl3
tar -xzf backhaul_linux_amd64.tar.gz
./backhaul -v
```

Download it from the [releases page](https://github.com/cybereow/backhaul/releases).

### Docker

```bash
docker run -d --name backhaul --network host \
  -v /etc/backhaul:/config:ro \
  <dockerhub-user>/backhaul:latest          # runs: backhaul -c /config/config.toml
```

The image is linux/amd64, based on `debian:bookworm-slim` with `libssl3` and CA
certificates. Use `--network host` (or publish the mapped ports) so the tunnel's
listeners are reachable.

### Build from source

Needs Go (see `go.mod`), a C toolchain and the OpenSSL headers:

```bash
sudo apt-get install -y gcc libssl-dev
git clone https://github.com/cybereow/backhaul.git
cd backhaul
CGO_ENABLED=1 go build -ldflags="-s -w" -o backhaul ./main.go
```

## Quick start

A `wssmux` tunnel that exposes the client's local web server on the server's
port 8443.

**Server** (public host) — `server.toml`:

```toml
[server]
bind_addr = "0.0.0.0:443"
transport = "wssmux"
token = "change-me"
tls_cert = "/etc/backhaul/server.crt"
tls_key = "/etc/backhaul/server.key"
ports = ["8443=127.0.0.1:80"]    # listen on :8443, the client dials 127.0.0.1:80
```

**Client** (behind NAT) — `client.toml`:

```toml
[client]
remote_addr = "server.example.com:443"
transport = "wssmux"
token = "change-me"
connection_pool = 8
```

```bash
./backhaul -c server.toml      # on the server
./backhaul -c client.toml      # on the client
```

- A config is a **server** if `[server].bind_addr` is set and a **client** if
  `[client].remote_addr` (or `remote_addrs`) is set. With `dns`/`dnsmux`, the
  role is given by `dns_domain` instead.
- `token` is mandatory on both ends and must match; there is no default.
- The config file is watched: saving it restarts the tunnel with the new values.
- `./backhaul -v` prints the version.

## Configuration reference

Only keys marked **required** must be set. Everything else has the default shown.

### Server — `[server]`

| key | default | applies to | description |
|---|---|---|---|
| `bind_addr` | — **required** | ws\*, wss\* | address the tunnel listens on, e.g. `"0.0.0.0:443"` |
| `transport` | — **required** | all | one of `ws`, `wss`, `wsmux`, `wssmux`, `dns`, `dnsmux` |
| `token` | — **required** | all | shared secret; must equal the client's |
| `ports` | `[]` | all | port mappings, see [Port mappings](#port-mappings) |
| `log_level` | `"info"` | all | `panic`, `fatal`, `error`, `warn`, `info`, `debug`, `trace` |
| `keepalive_period` | `75` | ws\* | TCP keepalive period, seconds |
| `heartbeat` | `40` | ws\* | control-channel ping interval, seconds (min 1) |
| `nodelay` | `false` | all | set `TCP_NODELAY` |
| `channel_size` | `2048` | ws\* | queue of connections waiting for a tunnel; excess is dropped |
| `proxy_protocol` | `false` | all | send a PROXY protocol header so the target sees the real client address |
| `path` | `""` | ws\* | base path for the tunnel endpoints (`<path>/channel`, `<path>/tunnel`) |
| `fallback` | `""` | ws\* | `host:port` of a decoy web backend, see [decoy site](#hiding-behind-a-decoy-site) |
| `tls_cert`, `tls_key` | — | wss, wssmux | certificate and key (PEM) |
| `tls_certs`, `tls_keys` | `[]` | wss, wssmux | several cert/key pairs, chosen by SNI |
| `sniffer` | `false` | all | record per-port traffic to `sniffer_log` |
| `web_port` | `0` | all | port of the web monitor (0 disables) |
| `sniffer_log` | `"backhaul.json"` | all | file the sniffer writes |
| `skip_optz` | `false` | all | skip the Linux sysctl / ulimit tuning applied at startup |
| `pprof` | `false` | all | pprof on `127.0.0.1:6060` (loopback only) |
| `so_rcvbuf`, `so_sndbuf` | OS default | wsmux, wssmux | socket buffer sizes in bytes |
| `mss` | OS default | ws\* | TCP maximum segment size |
| `cdn_max_age` | `0` (off) | ws\* | shortest max connection age of any CDN/LB in front (seconds); see [rotation](#multiplexing-rotation-and-resume-wsmux--wssmux) |
| `resume_window` | `30` with `cdn_max_age` | wsmux, wssmux | seconds a cut flow waits to be resumed; `-1` disables |
| `mux_con` | `8` | wsmux, wssmux, dns | streams per tunnel connection |
| `mux_version` | `1` | wsmux, wssmux, dns | smux protocol version (1 or 2); 2 is needed for UDP, resume, half-close, promotion, speedtest |
| `mux_framesize` | `32768` | wsmux, wssmux, dns | largest smux frame |
| `mux_recievebuffer` | `4194304` | wsmux, wssmux, dns | per-connection receive budget, bytes |
| `mux_streambuffer` | derived | wsmux, wssmux, dns | per-stream window; default `mux_recievebuffer / mux_con` (min 64 KiB) |
| `mux_keepalive_disabled` | `false` | wsmux, wssmux | turn off smux's keepalive ping |
| `mux_stripe` | `1` | wsmux, wssmux | split one flow across this many connections (1–255); must match the client |
| `mux_stripe_parity` | `0` | wsmux, wssmux | Reed-Solomon parity legs on top of `mux_stripe`; needs `mux_stripe ≥ 2`; `mux_stripe + mux_stripe_parity ≤ 255` |
| `stripe_ports` | `[]` | wsmux, wssmux | ports whose flows are striped |
| `promote_bytes` | `0` (off) | wsmux, wssmux | after this many bytes, move a plain flow onto a striped group; needs `mux_version = 2` |
| `mux_ws_framing` | `true` | wsmux, wssmux | carry legs as standard RFC 6455 binary messages; must match the client |
| `mux_half_close` | `false` | wsmux, wssmux | directional EOF for plain flows; needs `mux_version = 2` |
| `accept_udp` | `false` | wsmux, wssmux | also forward UDP on each mapped port; needs `mux_version = 2` |
| `udp_buffer` | `2048` | wsmux, wssmux | datagrams queued per UDP flow before dropping |
| `speedtest` | `false` | wsmux, wssmux | enable the token-gated `<path>/speedtest` endpoint |
| `dns_domain` | — **required for dns** | dns, dnsmux | tunnel domain this server is authoritative for |
| `dns_listen` | `"0.0.0.0:53"` | dns, dnsmux | UDP+TCP address of the DNS responder |
| `dns_key` | = `token` | dns, dnsmux | secret for the per-query MAC |

### Client — `[client]`

| key | default | applies to | description |
|---|---|---|---|
| `remote_addr` | — **required** | ws\* | server address, `host:port` |
| `remote_addrs` | `[]` | ws\* | several endpoints (e.g. one origin behind several CDN domains); the pool spreads across them round-robin, each with its own SNI |
| `edge_ip` | `""` | ws\* | IP to dial instead of resolving `remote_addr` (CDN edge) |
| `edge_ips` | `[]` | ws\* | edge IP per `remote_addrs` entry, by index |
| `transport` | — **required** | all | same as the server |
| `token` | — **required** | all | same as the server |
| `path` | `""` | ws\* | must match the server |
| `connection_pool` | `8` (`1` for dns) | all | tunnel connections kept open |
| `aggressive_pool` | `false` | ws\* | refill the pool more eagerly |
| `retry_interval` | `3` | all | seconds between reconnect attempts |
| `dial_timeout` | `10` | all | seconds to establish a connection |
| `keepalive_period` | `75` | all | TCP keepalive period, seconds |
| `nodelay` | `false` | all | set `TCP_NODELAY` |
| `tls_verify` | `true` | wss, wssmux | verify the server certificate. `false` is for self-signed setups only: while off, an on-path party can read the token |
| `resume_window` | `30` | wsmux, wssmux | seconds to wait for a cut flow to be resumed |
| `mux_*`, `so_*`, `mss`, `log_level`, `sniffer`, `web_port`, `sniffer_log`, `skip_optz`, `pprof` | as server | | same meaning as on the server; `mux_stripe`, `mux_stripe_parity`, `mux_ws_framing`, `mux_version` must match |
| `mux_stealth_handshake` | `true` | wsmux, wssmux | derive the framing subprotocol and half-close capability from the token instead of project-named strings; upgrade servers before clients, or set `false` |
| `dns_domain` | — **required for dns** | dns, dnsmux | tunnel domain |
| `dns_key` | = `token` | dns, dnsmux | per-query MAC secret |
| `dns_resolvers` | `[]` (auto) | dns, dnsmux | recursive resolvers to use; empty or `"auto"` tests a built-in list at startup |
| `dns_record_types` | all | dns, dnsmux | limit the RR types tried (`TXT`, `NULL`, `A`, `AAAA`, `CNAME`, `MX`, `SRV`, `PTR`) |
| `dns_timeout_ms` | `2000` | dns, dnsmux | per-query timeout |
| `dns_workers` | `16` | dns, dnsmux | queries in flight per tunnel connection |
| `dns_resolver_cidrs` | `[]` | dns, dnsmux | extra candidate resolvers (CIDRs/IPs, max 4096 addresses) |
| `dns_resolver_cache` | `""` | dns, dnsmux | file caching the discovered resolvers for 30 minutes |
| `dns_no_hedge` | `false` | dns, dnsmux | disable hedged (duplicate) queries |

`ws*` means `ws`, `wss`, `wsmux` and `wssmux`.

### Per-transport examples

`ws` / `wss` — one WebSocket per forwarded connection:

```toml
[server]
bind_addr = "0.0.0.0:8443"
transport = "wss"                 # or "ws"
token = "change-me"
tls_cert = "/etc/backhaul/server.crt"
tls_key = "/etc/backhaul/server.key"
ports = ["2222=127.0.0.1:22"]

[client]                          # in the client's own file
remote_addr = "server.example.com:8443"
transport = "wss"
token = "change-me"
connection_pool = 8
```

`wsmux` / `wssmux` — pooled and multiplexed:

```toml
[server]
bind_addr = "0.0.0.0:443"
transport = "wssmux"              # or "wsmux"
token = "change-me"
mux_version = 2
mux_con = 8
tls_cert = "/etc/backhaul/server.crt"
tls_key = "/etc/backhaul/server.key"
ports = ["443-600=5201"]

[client]                          # in the client's own file
remote_addr = "server.example.com:443"
transport = "wssmux"
token = "change-me"
connection_pool = 8
mux_version = 2
```

`dnsmux` — see [DNS transport](#dns-transport-dns--dnsmux) and
[`examples/dnsmux.toml`](examples/dnsmux.toml).

## Port mappings

`ports` entries are `local[=target]`. The server listens on `local`; the client
dials `target` (which defaults to the same port on the client's host).

| entry | meaning |
|---|---|
| `"443"` | listen on :443, client dials its own port 443 |
| `"4000=5000"` | listen on :4000, client dials port 5000 |
| `"443=1.1.1.1:5201"` | listen on :443, client dials `1.1.1.1:5201` |
| `"127.0.0.2:443=5201"` | bind only 127.0.0.2, client dials port 5201 |
| `"127.0.0.2:443=1.1.1.1:5201"` | bind 127.0.0.2, client dials `1.1.1.1:5201` |
| `"443-600"` | every port in the range, same port on the client |
| `"443-600=5201"` | every port in the range → port 5201 |
| `"443-600=1.1.1.1:5201"` | every port in the range → `1.1.1.1:5201` |

## TLS (wss / wssmux)

**The server always terminates TLS with the system OpenSSL**, never Go's
`crypto/tls`. The SSL context mirrors a stock nginx: TLS 1.2 and 1.3, server
cipher preference, ALPN `http/1.1`. On OpenSSL 3 that makes TLS 1.3 negotiate
`TLS_AES_256_GCM_SHA384`, exactly like nginx, whereas Go's stack always prefers
AES-128-GCM — a difference a censor probing a directly reachable origin could
fingerprint. A test in `internal/utils/network` asserts the cipher choice.

Caveats:

- For the closest match, run the same OpenSSL **major** version as your nginx.
- nginx's `http2` also advertises `h2` in ALPN; this listener speaks HTTP/1.1
  only (the WebSocket tunnel needs it).
- This closes the largest observable gap, not every one. Verify with a
  fingerprinting tool (e.g. JARM) against a real nginx before relying on it.
- If the origin is only reachable through a CDN, the censor sees the CDN's TLS
  and none of this matters.
- `tls_engine` no longer exists. A config that still sets it logs a warning and
  the value is ignored.

### Several domains (SNI)

```toml
tls_certs = ["/etc/backhaul/a.crt", "/etc/backhaul/b.crt"]
tls_keys  = ["/etc/backhaul/a.key", "/etc/backhaul/b.key"]
```

The server picks the certificate whose names match the client's SNI and falls
back to the first when none match.

### A self-signed certificate

```bash
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -keyout server.key -out server.crt -subj "/CN=example.com" \
  -addext "subjectAltName=DNS:example.com"
```

Clients then need `tls_verify = false`, or a certificate their system trusts.

## Hiding behind a decoy site

With `fallback`, backhaul is itself the web origin: requests that are not valid
tunnel traffic (wrong or missing token, any non-tunnel path such as `/`) are
reverse-proxied to a decoy backend instead of getting a 401, so probes see an
ordinary website. No nginx has to sit in the data path.

```toml
[server]
bind_addr = "0.0.0.0:443"
transport = "wssmux"
tls_cert = "/etc/backhaul/site.crt"
tls_key  = "/etc/backhaul/site.key"
path = "/your-secret-path"        # the tunnel lives here
fallback = "127.0.0.1:8080"       # everything else → your decoy site
token = "..."
```

Putting nginx in front instead costs CPU: nginx has no zero-copy path for a
proxied WebSocket and copies every byte in userspace, roughly a core per Gbps.

## DNS transport (dns / dnsmux)

Carries the tunnel inside DNS queries sent **only through recursive resolvers**;
the client never contacts the server directly. Payload is authenticated
(token challenge-response, per-query HMAC) but **not encrypted** — encrypt
anything sensitive end to end. Throughput is low by design (about 100 bytes per
query upstream, 400 bytes per reply); use it where nothing else works.

Prerequisites:

1. A domain you control, and a sub-zone for the tunnel, e.g. `ttt.example.com`.
2. An `NS` record delegating that sub-zone to a host you own
   (`ttt.example.com. NS tns.example.com.`) and an `A` glue record for it
   pointing at the **server's** public IP.
3. UDP **and** TCP port 53 reachable on the server and free (on Ubuntu, disable
   `systemd-resolved`'s stub listener).

```bash
dig +norec @<server-ip> SOA ttt.example.com   # the server answers authoritatively
dig NS ttt.example.com                        # the delegation is publicly visible
```

```toml
# server
[server]
transport = "dnsmux"
token = "change-me"
dns_domain = "ttt.example.com"
ports = ["8080=127.0.0.1:80"]

# client
[client]
transport = "dnsmux"
token = "change-me"
dns_domain = "ttt.example.com"
dns_resolvers = []        # empty/"auto": test a built-in list and keep the ones that work
```

How it behaves: the client polls (fast while data is pending, ~500 ms idle); it
measures resolvers and record types live and picks the best (`dns_record_types`
restricts them); queries unanswered for ~3× the typical round trip are hedged;
a SACK block avoids resending bytes that arrived behind a gap.

### Prober

`./backhaul -probe probe.toml` runs a standalone diagnostic (no tunnel) that
tests which resolver / record type / transport combinations actually carry bytes
in your network, from a `[dns_probe]` config. Run the responder role on the
server and the prober role on the client side. See
[`internal/transport/dns/README.md`](internal/transport/dns/README.md).

## Multiplexing, rotation and resume (wsmux / wssmux)

**mux_version.** smux version 2 adds per-stream flow control and is required for
UDP forwarding, resume, half-close, promotion and the speedtest. A single stream
tops out at `mux_streambuffer / RTT`, so the derived default
(`mux_recievebuffer / mux_con`, 512 KiB) matters on high-RTT links.

**Striping.** `mux_stripe = N` splits one flow across N pool connections, which
helps one long, throughput-bound flow on a lossy high-RTT link. It does not help
many short flows. `mux_stripe_parity = P` adds P Reed-Solomon legs so up to P
legs can die mid-flow without ending it, at the cost of P/N extra bandwidth.
Both ends must agree.

**Connection rotation (`cdn_max_age`).** Set it on the server to the shortest
max connection age among the CDNs/load balancers in front (e.g. `300`).
Connections are retired at ~70 % of that age (±10 % jitter) only after a
replacement has joined the pool, then drained for the rest of the age. With
`mux_version = 2` on both ends, a flow on a retired connection is moved
byte-for-byte to another connection instead of being cut, so it can outlive the
CDN limit (SSH, long downloads).

**Resume (`resume_window`).** A flow whose connection is cut *without* warning
waits up to `resume_window` seconds to be resumed elsewhere, replaying
unacknowledged data from a ring that grows from 256 KiB to 16 MiB within a
256 MiB process-wide budget. Requires `cdn_max_age` and `mux_version = 2`.

**Half-close (`mux_half_close`, server, `mux_version = 2`).** Lets a client send
its request, shut down its writing end and still get the full reply — a plain
smux stream cannot (the shutdown ends both directions). Plain flows then carry a
3-byte-header envelope. **Strict:** the server rejects upgrades from clients that
do not offer the capability, so upgrade clients first. Promotable and striped
flows keep full-close behaviour.

**Framing (`mux_ws_framing`, default on).** Every mux leg is a stream of
standard RFC 6455 binary messages negotiated with a subprotocol, so an
intermediary that parses WebSocket messages (a CDN) can carry it. It is strict
and never falls back silently: a mismatch fails loudly. It must be set the same
on both ends (`false` on both selects the legacy raw mode). No specific CDN has
been tested; verify yours.

**Handshake stealth (`mux_stealth_handshake`, client, default on).** Derives the
subprotocol and capability header from the token so no handshake string names the
project. Servers accept both forms: upgrade servers first, then clients.

**Setup limit.** Every accepted user connection has a fixed 3-second budget to be
handed to a tunnel stream (queueing, pool growth, retries and header write all
count). When it runs out the connection is closed. Concurrent setups are capped
at `channel_size`.

**Diagnostics.** The server's `<path>/pool` and `<path>/diag` endpoints (header
`Authorization: Bearer <token>`) report pool and control-channel state.

**Speedtest.** With `speedtest = true` on the server and `mux_version = 2` on
both ends:

```bash
curl -H "Authorization: Bearer <token>" \
  "https://server/<path>/speedtest?dir=both&scope=all&seconds=10"
```

`dir` is `down`, `up` or `both`; `scope` is `best` or `all`; `seconds` is 1–30.
Per-path rates use the receiver-measured data window; aggregate rates divide by
the local phase wall time, so they differ by design.

## Running as a service

`/etc/systemd/system/backhaul.service`:

```ini
[Unit]
Description=Backhaul Reverse Tunnel Service
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/backhaul -c /etc/backhaul/config.toml
Restart=always
RestartSec=3
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now backhaul
journalctl -u backhaul -e -f
```

## Tuning and troubleshooting

**CPU saturated on a small box, can't reach line rate.** The per-byte cost is
almost all TLS/AES. Check AES-NI (`grep -m1 -o aes /proc/cpuinfo`) — cheap VPSes
often hide it. Take nginx out of the tunnel's data path (see
[decoy site](#hiding-behind-a-decoy-site)). Don't wrap already-encrypted traffic
(VLESS/Reality, etc.) in a second TLS layer if you control both ends.

**High-latency link stalls below line rate.** Enable BBR
(`net.ipv4.tcp_congestion_control=bbr`, `net.core.default_qdisc=fq`); backhaul
applies this at startup unless `skip_optz = true`. Size `so_rcvbuf` / `so_sndbuf`
to the bandwidth-delay product (about 8 MB for 1 Gbps at 40 ms).

**Upload much slower than download on wsmux/wssmux.** Use `mux_version = 2` and
leave `mux_streambuffer` unset so the per-stream window is derived from the
session budget.

**One core pinned, another idle.** A single big flow is bound to one
connection. `mux_stripe = N` spreads it. If every core is already busy, striping
won't help.

**A flow resets mid-transfer on wsmux/wssmux.** Almost never packet loss — a
whole pooled connection died (CDN idle/max-age reset, mobile handover). Set
`cdn_max_age` for planned rotation, and `mux_stripe_parity` to survive legs
dying.

**`mux_*` settings have no effect on `ws`/`wss`.** Each forwarded connection is
its own WebSocket there; switch to `wsmux`/`wssmux` to use them.

## Development and releases

```bash
sudo apt-get install -y gcc libssl-dev
go vet ./...
go test -race ./...
```

Real-binary end-to-end tests with a no-tunnel baseline and an emulated WAN live
in [`e2e/`](e2e/README.md).

CI is deliberately narrow — **linux/amd64 only**:

| workflow | runs when | does |
|---|---|---|
| `ci.yml` | a commit is pushed to a branch | gofmt, vet, build, `go test -race`, then the e2e matrix (`ws`, `wss`, `wsmux`, `wssmux` × net off/wan) |
| `release.yml` | a `v*` tag is pushed | GoReleaser publishes `backhaul_linux_amd64.tar.gz` and checksums; the Docker image is pushed to Docker Hub |

Pull requests do not trigger anything by themselves. To cut a release, bump
`version` in `main.go`, then `git tag vX.Y.Z && git push --tags`. Docker
publishing needs the repository secrets `DOCKERHUB_USERNAME` and
`DOCKERHUB_TOKEN`; the image is `<username>/backhaul`, tagged with the version,
`major.minor`, the commit SHA and `latest`.

## Benchmark

See the [benchmark page](./benchmark/).

## License

AGPL-3.0. See [LICENSE](LICENSE).
