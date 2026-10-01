# DNS transport — Phase 1: reachability & capacity prober

This is **not the tunnel yet**. It is a standalone diagnostic that answers the one
question the whole DNS-transport design rests on:

> Does a query actually reach the inside responder, **and** does a response
> carrying real bytes come back — through a recursive resolver, in *your* network?

It measures that per profile (`resolver × record-type × UDP/TCP × EDNS × budgets`)
and records three independent facts: (a) the resolver accepted the query, (b) a
MAC-valid response from our peer came back, (c) how many bytes survived each way.

## What you need to test against real DNS

You (the operator) must set up a delegation so recursive resolvers route queries
for your tunnel domain to the inside box:

1. **A domain you control**, and a sub-zone for the tunnel, e.g. `ttt.example.com`.
2. **An NS record** delegating that sub-zone to a nameserver host you own, e.g.
   `ttt.example.com.  NS  tns.example.com.`
3. **A glue A/AAAA record** for that nameserver host pointing at the **inside
   server's public IP**, e.g. `tns.example.com.  A  203.0.113.10`. This is the box
   that runs the responder.
4. **UDP *and* TCP port 53 reachable** on the inside server from the internet
   (firewall/security-group open, nothing else bound to :53).
5. **A candidate resolver list** for the outside box: the recursive resolvers you
   want to test (public like `8.8.8.8`/`1.1.1.1`, and any local/ISP ones). The
   prober only ever queries these plus the system resolver — it never dials the
   authoritative server directly.

Verify the delegation before probing:

```
dig +norec @<inside-ip> SOA ttt.example.com      # responder answers authoritatively
dig NS ttt.example.com                            # delegation is visible publicly
```

## Running it

Inside box (authoritative responder):

```
sudo ./backhaul -probe probe.responder.toml
```

Outside box (prober):

```
./backhaul -probe probe.prober.toml
```

The prober prints a table (and optional JSON) ranking profiles; the rows with
stage `ok` and full `Q-BYTES`/`R-BYTES` are the ones a real transport could be
built on. `unknown` means no reply at all — which is **not** proof the query never
arrived (the reply may have been dropped on the way back); it is deliberately
distinguished from `resolver_replied` (reached a resolver, no usable payload back).

`key` is a shared secret used only to derive a per-query HMAC, so the token never
appears on the wire and only a matching peer's response is accepted. It must match
on both sides. It is not (yet) encryption of the payload — that is a later phase.


## Carrier and `dnsmux` transport

On top of the prober/responder there is a reliable, ordered byte stream
(`Dial` / `Server.Accept`, both `net.Conn`) that the `dnsmux` transport wraps in
smux exactly like `tcpmux`. See `config.dnsmux.example.toml`.

- **Session frame** (inside the existing query/response `Data`): query
  `sid(4) | flags(1) | rel.Packet`, response `flags(1) | rel.Packet`. Flags:
  `SYN` (first exchange, creates the session), `FIN` (sender closed its half),
  `RST` (unknown/expired sid: the client's Read/Write return an error).
- **Pull model**: the server can only answer queries, so the client polls, fast
  while data is pending or in flight, backing off to ~500 ms when idle.
- **Profiles**: the record type is never fixed; `sel` picks resolver, RR type
  and UDP/TCP from live measurements (`DefaultProfiles` builds the candidates).
- **Limits**: client->server segments are QNAME-limited (~100 B); server->client
  segments are capped at 400 B so a reply always fits the EDNS size even beside a
  maximum-size question (retransmissions cannot be shrunk).
- Transfer tests default to 32 KiB per direction (the carrier is slow, `-race`
  slower); `DNS_SLOW_TESTS=1` runs the 256 KiB soak.
- **Resolver discovery**: `dns_resolvers` may be empty or contain `"auto"`; the client then tests a built-in list of Iranian public resolvers (`DefaultResolvers`) in parallel at startup and keeps the ones that work from its vantage point (sel re-checks failed ones with backoff).
- **SACK**: `rel.Packet` carries the first out-of-order block (offset+length, 4 bytes) so the peer does not resend bytes that arrived behind a gap; simulated retransmit overhead 11.2% -> 8.5%.
- **Hedging**: an exchange still unanswered after ~3x the typical round trip (250ms..1.2s) starts one extra exchange (up to `workers` extra in flight), so a resolver that sometimes holds a query ~2s does not idle the workers. Disable with `dns_no_hedge = true`. Real-box A/B (8 workers, 20 KB echo): hedged 602/352 B/s vs plain 503/506 B/s - inconclusive, path noise dominates; the stream is also limited by in-order delivery behind a stalled segment.
- **Discovery** (`DiscoverResolvers`): candidates (built-in list, configured extras, `dns_resolver_cidrs`) go through a rate-limited reachability sweep (UDP then TCP), then the survivors get a few repeated probes; resolvers are ranked by success^2/(mean RTT) - stalls raise the mean, so flaky ones lose - and the best 8 become the profile set. `dns_resolver_cache` keeps the result for 30 minutes.
