#!/usr/bin/env bash
#
# End-to-end tunnel test: measures the same workload twice - once straight to
# the origin service, once through a real backhaul tunnel - so the tunnel's
# cost is reported against a baseline taken on the same machine, in the same
# run, over the same emulated network.
#
# Two workloads, because they stress different things:
#   bulk - one long stream; dominated by copy loops and socket buffers.
#   rr   - small request/response exchanges on ONE connection; dominated by
#          per-message overhead (syscalls, framing, wakeups).
#   conc - the same exchanges over many connections at once. The only workload
#          where connection pooling and stream multiplexing can pay for
#          themselves; on a single sequential connection they are pure cost.
#
# Every byte is verified against a deterministic pattern, so a run that reports
# numbers has also proved the tunnel did not corrupt or truncate the stream.
#
# Usage:
#   e2e/run.sh --transport ws --netem off --out results.json
#
# Syscall counting lives in e2e/syscalls.sh: under strace the timings here would
# be meaningless, so the two measurements are kept apart.
#
set -euo pipefail

TRANSPORT="ws"
NETEM="off"
OUT=""
# Left empty so that "did the caller set this?" can be answered after parsing;
# the defaults depend on the network profile (see below).
BULK_BYTES=""
RR_REQUESTS=""
RR_SIZE=512
CONC_CONNS=""
CONC_REQUESTS=""
CLIENT_TIMEOUT=""

# Emulated WAN applied to the inter-site link. 40ms each way = 80ms RTT, which
# is a realistic intercontinental path for this project's usual deployment.
NETEM_DELAY="${NETEM_DELAY:-40ms}"
NETEM_JITTER="${NETEM_JITTER:-5ms}"
NETEM_LOSS="${NETEM_LOSS:-0.01%}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --transport) TRANSPORT="$2"; shift 2 ;;
    --netem)     NETEM="$2";     shift 2 ;;
    --out)       OUT="$2";       shift 2 ;;
    --bulk-bytes)  BULK_BYTES="$2";  shift 2 ;;
    --rr-requests) RR_REQUESTS="$2"; shift 2 ;;
    --rr-size)     RR_SIZE="$2";     shift 2 ;;
    --conc-conns)    CONC_CONNS="$2";    shift 2 ;;
    --conc-requests) CONC_REQUESTS="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

# The rr workload is strictly sequential: one exchange per round trip. Under the
# emulated WAN that puts a hard ceiling of 1/RTT exchanges per second - about
# 12.5/s at 80ms - so the off-profile count would take roughly four minutes per
# measurement and simply time out. Scale the workload to the profile instead,
# and give the client a deadline with room to spare.
case "$NETEM" in
  wan)
    : "${BULK_BYTES:=$((16 * 1024 * 1024))}"
    : "${RR_REQUESTS:=300}"
    : "${CONC_CONNS:=32}"
    : "${CONC_REQUESTS:=1600}"
    : "${CLIENT_TIMEOUT:=300s}"
    ;;
  *)
    : "${BULK_BYTES:=$((64 * 1024 * 1024))}"
    : "${RR_REQUESTS:=3000}"
    : "${CONC_CONNS:=32}"
    : "${CONC_REQUESTS:=6400}"
    : "${CLIENT_TIMEOUT:=120s}"
    ;;
esac

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$(mktemp -d)"
BACKHAUL="${BACKHAUL_BIN:-$WORK/backhaul}"
LOADGEN="${LOADGEN_BIN:-$WORK/loadgen}"

# The origin service is served on two ports by two identical instances. They
# exist to model where the service sits relative to the emulated WAN:
#
#   ORIGIN_DIRECT_PORT  what a user reaches WITHOUT a tunnel - across the WAN,
#                       so this port is shaped.
#   ORIGIN_TUNNEL_PORT  what the backhaul CLIENT dials. The client is
#                       co-located with the service in every real deployment,
#                       so this hop is local and must NOT be shaped.
#
# One shared origin port cannot express that: shaping it would put the WAN on
# the tunnel's last hop as well, making the tunnel cross the emulated WAN twice
# while the baseline crosses it once.
ORIGIN_DIRECT_PORT=19000
ORIGIN_TUNNEL_PORT=19002
TUNNEL_PORT=18080   # backhaul server <-> backhaul client (the inter-site link)
PUBLIC_PORT=19001   # the port backhaul exposes to users
TOKEN="e2e-test-token"

PIDS=()
NETEM_APPLIED=0

cleanup() {
  local pid
  for pid in "${PIDS[@]:-}"; do
    [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true
  done
  if [[ "$NETEM_APPLIED" == "1" ]]; then
    ${SUDO:-} tc qdisc del dev lo root 2>/dev/null || true
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

log() { echo "[e2e] $*" >&2; }

# wait_port blocks until something accepts on the port, or fails after ~15s.
wait_port() {
  local port="$1" i
  for i in $(seq 1 150); do
    # The probe runs in a subshell so the descriptor is closed when it exits.
    # Never `exec 3<&- 2>/dev/null` in this shell: an exec carrying only
    # redirections applies them permanently, which would send every later
    # log line on this script's stderr to /dev/null.
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
      return 0
    fi
    sleep 0.1
  done
  echo "timed out waiting for port $port" >&2
  return 1
}

# ---------------------------------------------------------------- build

if [[ ! -x "$BACKHAUL" ]]; then
  log "building backhaul"
  ( cd "$REPO_ROOT" && go build -o "$BACKHAUL" ./main.go )
fi
if [[ ! -x "$LOADGEN" ]]; then
  log "building loadgen"
  ( cd "$REPO_ROOT" && go build -o "$LOADGEN" ./e2e/loadgen )
fi

# ---------------------------------------------------------------- network

# The emulated WAN must be crossed EXACTLY ONCE by each path, or the comparison
# is rigged. Without a tunnel a user reaches the service directly across the
# WAN (ORIGIN_DIRECT_PORT). With one, the WAN is the backhaul client <-> server
# link (TUNNEL_PORT); the user->server hop and the client->service hop are both
# local. So those two ports are shaped and nothing else is - in particular
# ORIGIN_TUNNEL_PORT is left alone, which is the whole reason it exists.
if [[ "$NETEM" == "wan" ]]; then
  if ! command -v tc >/dev/null 2>&1; then
    echo "netem requested but tc is not installed" >&2
    exit 3
  fi
  # Shaping needs root. Callers that are not root set SUDO=sudo.
  TC=("${SUDO:-}" tc); [[ -z "${SUDO:-}" ]] && TC=(tc)
  log "applying WAN emulation: ${NETEM_DELAY} +/- ${NETEM_JITTER}, ${NETEM_LOSS} loss"
  # Fail loudly rather than silently reporting a "WAN" run that had no WAN.
  "${TC[@]}" qdisc add dev lo root handle 1: prio
  NETEM_APPLIED=1
  "${TC[@]}" qdisc add dev lo parent 1:3 handle 30: netem \
      delay "$NETEM_DELAY" "$NETEM_JITTER" distribution normal loss "$NETEM_LOSS"
  for p in "$ORIGIN_DIRECT_PORT" "$TUNNEL_PORT"; do
    "${TC[@]}" filter add dev lo protocol ip parent 1:0 prio 3 u32 \
        match ip dport "$p" 0xffff flowid 1:3
    "${TC[@]}" filter add dev lo protocol ip parent 1:0 prio 3 u32 \
        match ip sport "$p" 0xffff flowid 1:3
  done
fi

# ---------------------------------------------------------------- origin

log "starting origin services on $ORIGIN_DIRECT_PORT (shaped) and $ORIGIN_TUNNEL_PORT (local)"
"$LOADGEN" -role=origin -listen="127.0.0.1:$ORIGIN_DIRECT_PORT" >"$WORK/origin-direct.log" 2>&1 &
PIDS+=($!)
"$LOADGEN" -role=origin -listen="127.0.0.1:$ORIGIN_TUNNEL_PORT" >"$WORK/origin-tunnel.log" 2>&1 &
PIDS+=($!)
wait_port "$ORIGIN_DIRECT_PORT"
wait_port "$ORIGIN_TUNNEL_PORT"

measure() { # measure <target-port> <mode>
  local port="$1" mode="$2"
  case "$mode" in
    bulk) "$LOADGEN" -role=client -connect="127.0.0.1:$port" -mode=bulk \
             -bytes="$BULK_BYTES" -timeout="$CLIENT_TIMEOUT" ;;
    rr)   "$LOADGEN" -role=client -connect="127.0.0.1:$port" -mode=rr \
             -requests="$RR_REQUESTS" -size="$RR_SIZE" -timeout="$CLIENT_TIMEOUT" ;;
    conc) "$LOADGEN" -role=client -connect="127.0.0.1:$port" -mode=conc \
             -conns="$CONC_CONNS" -requests="$CONC_REQUESTS" -size="$RR_SIZE" \
             -timeout="$CLIENT_TIMEOUT" ;;
  esac
}

log "measuring BASELINE (no tunnel, straight across the WAN)"
BASE_BULK="$(measure "$ORIGIN_DIRECT_PORT" bulk)"
BASE_RR="$(measure "$ORIGIN_DIRECT_PORT" rr)"
BASE_CONC="$(measure "$ORIGIN_DIRECT_PORT" conc)"

# ---------------------------------------------------------------- tunnel

cat > "$WORK/server.toml" <<EOF
[server]
bind_addr = "127.0.0.1:$TUNNEL_PORT"
transport = "$TRANSPORT"
token = "$TOKEN"
channel_size = 2048
keepalive_period = 75
heartbeat = 40
nodelay = true
log_level = "info"
ports = ["$PUBLIC_PORT=127.0.0.1:$ORIGIN_TUNNEL_PORT"]
EOF

cat > "$WORK/client.toml" <<EOF
[client]
remote_addr = "127.0.0.1:$TUNNEL_PORT"
transport = "$TRANSPORT"
token = "$TOKEN"
connection_pool = 8
keepalive_period = 75
dial_timeout = 10
retry_interval = 3
nodelay = true
log_level = "info"
EOF

# wss/wssmux need a certificate; a self-signed one is fine because the client
# is told not to verify it.
case "$TRANSPORT" in
  wss|wssmux)
    log "generating self-signed certificate"
    openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
      -subj "/CN=localhost" -addext "subjectAltName=IP:127.0.0.1,DNS:localhost" \
      -keyout "$WORK/tls.key" -out "$WORK/tls.crt" >/dev/null 2>&1
    printf 'tls_cert = "%s"\ntls_key = "%s"\n' "$WORK/tls.crt" "$WORK/tls.key" >> "$WORK/server.toml"
    printf 'tls_verify = false\n' >> "$WORK/client.toml"
    ;;
esac

log "starting backhaul server ($TRANSPORT)"
"$BACKHAUL" -c "$WORK/server.toml" >"$WORK/server.log" 2>&1 &
PIDS+=($!)
wait_port "$TUNNEL_PORT"

log "starting backhaul client"
"$BACKHAUL" -c "$WORK/client.toml" >"$WORK/client.log" 2>&1 &
PIDS+=($!)
wait_port "$PUBLIC_PORT"

# The public port binds before the pool is usable, so probe with a real (tiny)
# exchange until one completes rather than guessing with a sleep.
log "waiting for the tunnel to carry traffic"
ready=0
for i in $(seq 1 60); do
  if "$LOADGEN" -role=client -connect="127.0.0.1:$PUBLIC_PORT" -mode=rr \
       -requests=1 -size=64 -timeout=5s >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 0.5
done
if [[ "$ready" != "1" ]]; then
  echo "tunnel never became ready; server log:" >&2
  tail -30 "$WORK/server.log" >&2
  echo "client log:" >&2
  tail -30 "$WORK/client.log" >&2
  exit 1
fi

log "measuring THROUGH TUNNEL ($TRANSPORT)"
TUN_BULK="$(measure "$PUBLIC_PORT" bulk)"
TUN_RR="$(measure "$PUBLIC_PORT" rr)"
TUN_CONC="$(measure "$PUBLIC_PORT" conc)"

# ---------------------------------------------------------------- report

RESULT=$(cat <<EOF
{
  "transport": "$TRANSPORT",
  "netem": "$NETEM",
  "workload": { "bulk_bytes": $BULK_BYTES, "rr_requests": $RR_REQUESTS, "rr_size": $RR_SIZE,
                "conc_conns": $CONC_CONNS, "conc_requests": $CONC_REQUESTS },
  "baseline": { "bulk": $BASE_BULK, "rr": $BASE_RR, "conc": $BASE_CONC },
  "tunnel":   { "bulk": $TUN_BULK,  "rr": $TUN_RR,  "conc": $TUN_CONC }
}
EOF
)

if [[ -n "$OUT" ]]; then
  echo "$RESULT" > "$OUT"
fi
echo "$RESULT"
