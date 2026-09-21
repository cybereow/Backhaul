#!/usr/bin/env bash
#
# Counts the read/write syscalls a backhaul server makes while carrying a fixed
# workload, by running it under `strace -f -c`.
#
# This is kept separate from e2e/run.sh on purpose. strace slows the traced
# process enough that throughput measured under it is meaningless, but the
# syscall COUNTS are exact and, unlike timings, are not perturbed by a noisy
# shared CI runner. That makes this the reliable way to check a claim like
# "the WebSocket read path costs three socket reads per message" on a real
# deployment rather than in a benchmark harness.
#
# Compare two builds by running it once per binary with the same workload:
#   BACKHAUL_BIN=./backhaul-main e2e/syscalls.sh --out main.json
#   BACKHAUL_BIN=./backhaul-pr   e2e/syscalls.sh --out pr.json
#
set -euo pipefail

TRANSPORT="ws"
OUT=""
RR_REQUESTS=2000
RR_SIZE=512

while [[ $# -gt 0 ]]; do
  case "$1" in
    --transport)   TRANSPORT="$2";   shift 2 ;;
    --out)         OUT="$2";         shift 2 ;;
    --rr-requests) RR_REQUESTS="$2"; shift 2 ;;
    --rr-size)     RR_SIZE="$2";     shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$(mktemp -d)"
BACKHAUL="${BACKHAUL_BIN:-$WORK/backhaul}"
LOADGEN="${LOADGEN_BIN:-$WORK/loadgen}"

ORIGIN_PORT=19100
TUNNEL_PORT=18180
PUBLIC_PORT=19101
TOKEN="e2e-syscall-token"

PIDS=()
cleanup() {
  local pid
  for pid in "${PIDS[@]:-}"; do
    [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true
  done
  rm -rf "$WORK"
}
trap cleanup EXIT

log() { echo "[syscalls] $*" >&2; }

wait_port() {
  local port="$1" i
  for i in $(seq 1 150); do
    # Subshell so the descriptor closes on its own; never `exec` a bare
    # redirection here, it would apply to this shell permanently.
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
      return 0
    fi
    sleep 0.1
  done
  echo "timed out waiting for port $port" >&2
  return 1
}

command -v strace >/dev/null 2>&1 || { echo "strace is not installed" >&2; exit 3; }

if [[ ! -x "$BACKHAUL" ]]; then
  ( cd "$REPO_ROOT" && go build -o "$BACKHAUL" ./main.go )
fi
if [[ ! -x "$LOADGEN" ]]; then
  ( cd "$REPO_ROOT" && go build -o "$LOADGEN" ./e2e/loadgen )
fi

cat > "$WORK/server.toml" <<EOF
[server]
bind_addr = "127.0.0.1:$TUNNEL_PORT"
transport = "$TRANSPORT"
token = "$TOKEN"
channel_size = 2048
nodelay = true
log_level = "error"
ports = ["$PUBLIC_PORT=127.0.0.1:$ORIGIN_PORT"]
EOF

cat > "$WORK/client.toml" <<EOF
[client]
remote_addr = "127.0.0.1:$TUNNEL_PORT"
transport = "$TRANSPORT"
token = "$TOKEN"
connection_pool = 8
nodelay = true
log_level = "error"
EOF

case "$TRANSPORT" in
  wss|wssmux)
    openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
      -subj "/CN=localhost" -addext "subjectAltName=IP:127.0.0.1,DNS:localhost" \
      -keyout "$WORK/tls.key" -out "$WORK/tls.crt" >/dev/null 2>&1
    printf 'tls_cert = "%s"\ntls_key = "%s"\n' "$WORK/tls.crt" "$WORK/tls.key" >> "$WORK/server.toml"
    printf 'tls_verify = false\n' >> "$WORK/client.toml"
    ;;
esac

log "starting origin"
"$LOADGEN" -role=origin -listen="127.0.0.1:$ORIGIN_PORT" >"$WORK/origin.log" 2>&1 &
PIDS+=($!)
wait_port "$ORIGIN_PORT"

log "starting backhaul server under strace ($TRANSPORT)"
strace -f -c -e trace=read,write -o "$WORK/strace.txt" \
  "$BACKHAUL" -c "$WORK/server.toml" >"$WORK/server.log" 2>&1 &
STRACE_PID=$!
PIDS+=($STRACE_PID)
wait_port "$TUNNEL_PORT"

# strace writes its -c summary only when it sees the traced process exit, so
# the signal has to go to the tracee. Killing strace itself leaves the file
# empty. Resolve the child now and confirm we found it.
TRACEE_PID=""
for i in $(seq 1 50); do
  TRACEE_PID="$(ps -o pid= --ppid "$STRACE_PID" 2>/dev/null | tr -d ' ' | head -1)"
  [[ -n "$TRACEE_PID" ]] && break
  sleep 0.1
done
[[ -n "$TRACEE_PID" ]] || { echo "could not find the traced backhaul process" >&2; exit 4; }
log "strace=$STRACE_PID tracee=$TRACEE_PID"

log "starting backhaul client"
"$BACKHAUL" -c "$WORK/client.toml" >"$WORK/client.log" 2>&1 &
PIDS+=($!)
wait_port "$PUBLIC_PORT"

# The public port binds before the pool can carry traffic; probe with real
# exchanges until one completes. These probe syscalls are counted too, which is
# fine: both sides of a comparison pay the same fixed cost, and it is tiny
# beside the measured workload.
log "waiting for the tunnel to carry traffic"
ready=0
for i in $(seq 1 60); do
  if "$LOADGEN" -role=client -connect="127.0.0.1:$PUBLIC_PORT" -mode=rr \
       -requests=1 -size=64 -timeout=5s >/dev/null 2>&1; then
    ready=1; break
  fi
  sleep 0.5
done
[[ "$ready" == "1" ]] || { echo "tunnel never became ready" >&2; tail -20 "$WORK/server.log" >&2; exit 5; }

log "running workload: $RR_REQUESTS exchanges of ${RR_SIZE}B"
WORKLOAD="$("$LOADGEN" -role=client -connect="127.0.0.1:$PUBLIC_PORT" -mode=rr \
    -requests="$RR_REQUESTS" -size="$RR_SIZE" -timeout=600s)"

# Stop the tracee and wait for strace to flush the summary.
kill -TERM "$TRACEE_PID" 2>/dev/null || true
for i in $(seq 1 200); do
  grep -q "total" "$WORK/strace.txt" 2>/dev/null && break
  sleep 0.1
done
grep -q "total" "$WORK/strace.txt" 2>/dev/null || {
  echo "strace produced no summary" >&2; cat "$WORK/strace.txt" >&2; exit 6; }

reads="$(awk '$NF=="read"  {print $4; exit}' "$WORK/strace.txt")"
writes="$(awk '$NF=="write" {print $4; exit}' "$WORK/strace.txt")"
reads="${reads:-0}"; writes="${writes:-0}"

RESULT=$(cat <<EOF
{
  "transport": "$TRANSPORT",
  "requests": $RR_REQUESTS,
  "request_size": $RR_SIZE,
  "server_read_calls": $reads,
  "server_write_calls": $writes,
  "reads_per_request": $(awk -v r="$reads" -v n="$RR_REQUESTS" 'BEGIN{printf "%.3f", r/n}'),
  "writes_per_request": $(awk -v w="$writes" -v n="$RR_REQUESTS" 'BEGIN{printf "%.3f", w/n}'),
  "workload": $WORKLOAD
}
EOF
)

if [[ -n "$OUT" ]]; then
  echo "$RESULT" > "$OUT"
  cp "$WORK/strace.txt" "${OUT%.json}.strace.txt" 2>/dev/null || true
fi
echo "$RESULT"
