# End-to-end tunnel tests

Real binaries, real sockets, real tunnel. Everything here runs in CI
(`.github/workflows/e2e.yml`) on GitHub's runners, and the same commands work
locally if you want them to.

## Why a baseline

A throughput number for a tunnel means nothing on its own — it mostly measures
the machine it ran on. So every run measures the **same workload twice**: once
straight to the origin service, once through a backhaul tunnel, back to back on
the same host under the same emulated network. What is reported is the ratio.

## Reading the ratios

On the **unshaped** profile the baseline is loopback: an RTT of tens of
microseconds. A tunnel inherently adds two process hops and its own framing, so
its fixed cost — a few hundred microseconds — looks enormous next to that while
being irrelevant on any real path. On that profile read the **absolute** added
latency, not the percentage.

The **wan** profile is the one whose ratios mean something, because there the
baseline pays a realistic RTT too.

Neither profile says anything about behaviour under concurrency: the `rr`
workload is a single connection issuing one exchange at a time, which is the
worst case for the pooled and multiplexed transports, since there is nothing to
multiplex.

## The two workloads

| workload | what it is | what it stresses |
|---|---|---|
| `bulk` | one long stream | copy loops, socket buffers — a file transfer or speedtest |
| `rr` | small request/response exchanges | per-message overhead: syscalls, framing, wakeups — interactive traffic (SSH, gaming, HTTP APIs) |

Both matter and they move independently: a change can leave bulk throughput flat
and still halve the cost of small messages, or the reverse.

Every byte is checked against a deterministic pattern, so a run that reports
numbers has also proved the tunnel did not corrupt, reorder or truncate the
stream. CI fails the job if any of the four measurements reports
`"verified": false`.

## Emulated WAN

`--netem wan` adds 40ms ± 5ms each way and 0.01% loss, i.e. an 80ms RTT — a
realistic intercontinental path for how this project is actually deployed.

Each path must cross the emulated WAN **exactly once**, or the comparison is
rigged. That is why the origin service is served on two ports by two identical
instances:

| port | who reaches it | shaped? |
|---|---|---|
| 19000 | a user going direct, with no tunnel — across the WAN | yes |
| 19002 | the backhaul **client**, which sits beside the service | no |

A single shared origin port cannot express this. Shaping it would also shape
the tunnel's last hop, so the tunnel would cross the WAN twice while the
baseline crossed it once — which is exactly the bug the first version of this
harness had, and it made the tunnel look about 20x worse than it is.

So the shaping goes on port 19000 and the tunnel link (18080), and nowhere
else.

The `rr` workload is strictly sequential — one exchange per round trip — so the
WAN profile caps it at 1/RTT, about 12.5 exchanges per second. The workload is
therefore scaled to the profile (300 exchanges and 16MB bulk under `wan`, 3000
and 64MB without it); passing `--rr-requests`/`--bulk-bytes` overrides that. Each
run reports the workload it actually used, so a WAN row is never mistaken for an
unshaped one.

It needs `sch_netem`/`sch_prio` and root. The workflow probes for them first and
fails loudly rather than reporting a "WAN" run that silently had no WAN.

## Running it

```sh
# one transport, no shaping
./e2e/run.sh --transport ws --netem off

# with the emulated WAN (needs root for tc)
SUDO=sudo ./e2e/run.sh --transport wssmux --netem wan --out result.json
```

Transports: `ws`, `wss`, `wsmux`, `wssmux`. `wss`/`wssmux` get a throwaway
self-signed certificate automatically.

Reuse prebuilt binaries with `BACKHAUL_BIN` and `LOADGEN_BIN` to skip the
rebuild between runs.

## Syscall counting

```sh
BACKHAUL_BIN=./backhaul-a ./e2e/syscalls.sh --transport ws --out a.json
BACKHAUL_BIN=./backhaul-b ./e2e/syscalls.sh --transport ws --out b.json
```

This runs the server under `strace -f -c` and reports read/write calls per
request. It is deliberately a **separate** script from `run.sh`: strace slows
the traced process enough that any timing taken under it is meaningless, but the
counts are exact — and unlike timings they are not perturbed by a noisy shared
CI runner. That makes this the reliable way to check a claim like "the WebSocket
read path costs two extra socket reads per message" against a real deployment
rather than a benchmark harness.

The PR job runs it against both the PR and its base branch and prints the delta.

## Layout

| path | what |
|---|---|
| `loadgen/` | traffic generator and integrity checker; acts as origin or client |
| `run.sh` | baseline vs tunnel throughput, one transport per invocation |
| `syscalls.sh` | syscall counts for one build, for comparing two builds |
