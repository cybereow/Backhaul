# End-to-end tunnel tests

Real binaries, real sockets, real tunnel. Everything here runs in CI
(`.github/workflows/e2e.yml`) on GitHub's runners, and the same commands work
locally if you want them to.

## Why a baseline

A throughput number for a tunnel means nothing on its own — it mostly measures
the machine it ran on. So every run measures the **same workload twice**: once
straight to the origin service, once through a backhaul tunnel, back to back on
the same host under the same emulated network. What is reported is the ratio.

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

The shaping is attached to the **origin port and the tunnel port**, and to
neither of the two local hops. That keeps the comparison honest: without a
tunnel a user crosses the WAN to reach the origin directly; with one, the WAN is
the backhaul client↔server link and both its ends are local. Each path crosses
the emulated WAN exactly once.

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
