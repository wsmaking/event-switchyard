#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

OUT_DIR="var/results"
mkdir -p "$OUT_DIR"
STAMP="$(date +%Y%m%d_%H%M%S)"
RAW_CSV="$OUT_DIR/inflight_fault_sweep_raw_${STAMP}.csv"
SUMMARY_MD="$OUT_DIR/inflight_fault_sweep_summary_${STAMP}.md"

HOST="${HOST:-localhost}"
PORT="${PORT:-29001}"
JWT_SECRET="${JWT_HS256_SECRET:-secret123}"
DURATION="${DURATION:-12}"
CONCURRENCY="${CONCURRENCY:-250}"
ACCOUNTS="${ACCOUNTS:-32}"
RUNS="${RUNS:-5}"

CAP="${CAP:-1024}"
ALPHA="${ALPHA:-0.8}"
INITIAL="${INITIAL:-300}"
MIN_INFLIGHT="${MIN_INFLIGHT:-64}"
TICK_MS="${TICK_MS:-200}"
SLEW="${SLEW:-0.25}"
BATCH="${BATCH:-64}"
WAIT_SET="${WAIT_SET:-200 1000 5000}"

cat > "$RAW_CSV" <<'CSV'
wait_us,run,cap,alpha,throughput_rps,thr_p99_us,rejected,bp_inflight_total,queue_full_total,ack_p99_us,fdatasync_p99_us,wal_age_ms,limit_dynamic,inflight
CSV

metrics_value() {
  local key="$1"
  local text="$2"
  printf "%s\n" "$text" | awk -v k="$key" '$1==k{print $2}' | tail -n1
}

ensure_port_free() {
  if lsof -iTCP:"$PORT" -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "ERROR: port $PORT is already in use" >&2
    lsof -iTCP:"$PORT" -sTCP:LISTEN -n -P >&2 || true
    exit 1
  fi
}

start_server() {
  local wait_us="$1"
  BACKPRESSURE_INFLIGHT_DYNAMIC=1 \
  BACKPRESSURE_INFLIGHT_INITIAL="$INITIAL" \
  BACKPRESSURE_INFLIGHT_MIN="$MIN_INFLIGHT" \
  BACKPRESSURE_INFLIGHT_CAP="$CAP" \
  BACKPRESSURE_INFLIGHT_ALPHA="$ALPHA" \
  BACKPRESSURE_INFLIGHT_TICK_MS="$TICK_MS" \
  BACKPRESSURE_INFLIGHT_SLEW_RATIO="$SLEW" \
  BACKPRESSURE_INFLIGHT_TARGET_WAL_AGE_SEC=1.0 \
  FASTPATH_DRAIN_ENABLE=1 \
  FASTPATH_DRAIN_WORKERS=4 \
  AUDIT_ASYNC_WAL=1 \
  AUDIT_FDATASYNC=1 \
  AUDIT_FDATASYNC_MAX_WAIT_US="$wait_us" \
  AUDIT_FDATASYNC_MAX_BATCH="$BATCH" \
  KAFKA_ENABLE=0 \
  JWT_HS256_SECRET="$JWT_SECRET" \
  GATEWAY_PORT="$PORT" \
  GATEWAY_TCP_PORT=0 \
  "$ROOT_DIR/gateway-rust/target/debug/gateway-rust" \
    > "$OUT_DIR/inflight_fault_server_${STAMP}_${wait_us}.log" 2>&1 &
  SERVER_PID=$!

  for _ in $(seq 1 200); do
    if curl -sf "http://$HOST:$PORT/health" >/dev/null; then
      return 0
    fi
    sleep 0.25
  done

  echo "ERROR: server failed to start (wait_us=$wait_us)" >&2
  tail -n 80 "$OUT_DIR/inflight_fault_server_${STAMP}_${wait_us}.log" >&2 || true
  return 1
}

stop_server() {
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
}

run_once() {
  local wait_us="$1"
  local run_idx="$2"

  ensure_port_free
  start_server "$wait_us"

  python3 scripts/ops/bench_gateway.py throughput \
    --host "$HOST" --port "$PORT" --duration 2 --concurrency 60 --accounts 16 >/dev/null 2>&1 || true

  local bench_out
  bench_out=$(python3 scripts/ops/bench_gateway.py throughput \
    --host "$HOST" --port "$PORT" \
    --duration "$DURATION" --concurrency "$CONCURRENCY" --accounts "$ACCOUNTS")

  local thr thr_p99 rejected
  thr=$(printf "%s\n" "$bench_out" | sed -n 's/^  Throughput: \([0-9.]*\) req\/s.*/\1/p' | tail -n1)
  thr_p99=$(printf "%s\n" "$bench_out" | sed -n 's/^  p50: .* p99: \([0-9.]*\)µs.*/\1/p' | tail -n1)
  rejected=$(printf "%s\n" "$bench_out" | sed -n 's/^  Throughput: .* Rejected: \([0-9]*\).*/\1/p' | tail -n1)

  local metrics
  metrics=$(curl -sf "http://$HOST:$PORT/metrics")

  local bp queue ack fdp99 wal_age limit inflight
  bp=$(metrics_value gateway_backpressure_inflight_total "$metrics")
  queue=$(metrics_value gateway_reject_queue_full_total "$metrics")
  ack=$(metrics_value gateway_ack_p99_us "$metrics")
  fdp99=$(metrics_value gateway_fdatasync_p99_us "$metrics")
  wal_age=$(metrics_value gateway_wal_age_ms "$metrics")
  limit=$(metrics_value gateway_inflight_limit_dynamic "$metrics")
  inflight=$(metrics_value gateway_inflight "$metrics")

  echo "$wait_us,$run_idx,$CAP,$ALPHA,$thr,$thr_p99,$rejected,$bp,$queue,$ack,$fdp99,$wal_age,$limit,$inflight" >> "$RAW_CSV"

  stop_server
  unset SERVER_PID
}

trap 'stop_server' EXIT

for w in $WAIT_SET; do
  for i in $(seq 1 "$RUNS"); do
    echo "==> wait_us=$w run=$i"
    run_once "$w" "$i"
  done
done

python3 - "$RAW_CSV" "$SUMMARY_MD" <<'PY'
import csv, statistics, sys
from collections import defaultdict

raw_csv, summary_md = sys.argv[1], sys.argv[2]
rows = list(csv.DictReader(open(raw_csv)))
by = defaultdict(list)
for r in rows:
    by[r['wait_us']].append(r)

def med(vals):
    return statistics.median([float(v) for v in vals])

def medi(vals):
    return int(round(med(vals)))

lines = []
lines.append("| wait_us | throughput_rps(median) | thr_p99_us(median) | rejected(median) | bp_inflight_total(median) | queue_full_total(median) | ack_p99_us(median) | fdatasync_p99_us(median) | wal_age_ms(median) | limit_dynamic(median) |")
lines.append("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
for w in sorted(by.keys(), key=lambda x: int(x)):
    g = by[w]
    lines.append(
        "| {w} | {thr:.2f} | {p99:.2f} | {rej} | {bp} | {q} | {ack} | {fd} | {wal} | {lim} |".format(
            w=w,
            thr=med([x['throughput_rps'] for x in g]),
            p99=med([x['thr_p99_us'] for x in g]),
            rej=medi([x['rejected'] for x in g]),
            bp=medi([x['bp_inflight_total'] for x in g]),
            q=medi([x['queue_full_total'] for x in g]),
            ack=medi([x['ack_p99_us'] for x in g]),
            fd=medi([x['fdatasync_p99_us'] for x in g]),
            wal=medi([x['wal_age_ms'] for x in g]),
            lim=medi([x['limit_dynamic'] for x in g]),
        )
    )

open(summary_md, 'w').write("\n".join(lines) + "\n")
print("\n".join(lines))
PY

echo "RAW_CSV=$RAW_CSV"
echo "SUMMARY_MD=$SUMMARY_MD"
