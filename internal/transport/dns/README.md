# DNS transport — Phase 1: reachability & capacity prober

## Backhaul `dnsmux` transport

Set `transport = "dnsmux"` to run ordinary smux port forwarding over the
reliable DNS `net.Conn`. The authoritative side requires `dns_domain` and uses
`dns_listen` (default `0.0.0.0:53`); the outside client requires the same domain
and one or more explicit `dns_resolvers`. An omitted `dns_record_types` enables
every built-in codec over both UDP and TCP. `dns_key` authenticates the carrier
and falls back to the normal `token`; the token handshake still runs inside the
tunnel. See `config.dnsmux.example.toml` for a complete example.

The `-probe` mode described below is a standalone diagnostic that answers the
question the DNS transport rests on:

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
