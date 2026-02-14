#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

OUT_DIR="var/results"
mkdir -p "$OUT_DIR"
STAMP="$(date +%Y%m%d_%H%M%S)"
RAW_CSV="$OUT_DIR/inflight_ab_raw_${STAMP}.csv"
SUMMARY_MD="$OUT_DIR/inflight_ab_summary_${STAMP}.md"

HOST="${HOST:-localhost}"
PORT="${PORT:-29001}"
JWT_SECRET="${JWT_HS256_SECRET:-secret123}"
DURATION="${DURATION:-12}"
CONCURRENCY="${CONCURRENCY:-250}"
ACCOUNTS="${ACCOUNTS:-32}"
RUNS="${RUNS:-3}"

cat > "$RAW_CSV" <<'CSV'
case_name,run,cap,alpha,throughput_rps,thr_p99_us,rejected,bp_inflight_total,queue_full_total,ack_p99_us,limit_min,limit_max,max_inflight
CSV

ensure_port_free() {
  if lsof -iTCP:"$PORT" -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "ERROR: port $PORT is already in use" >&2
    lsof -iTCP:"$PORT" -sTCP:LISTEN -n -P >&2 || true
    exit 1
  fi
}

start_server() {
  local cap="$1"
  local alpha="$2"
  BACKPRESSURE_INFLIGHT_DYNAMIC=1 \
  BACKPRESSURE_INFLIGHT_INITIAL=300 \
  BACKPRESSURE_INFLIGHT_MIN=64 \
  BACKPRESSURE_INFLIGHT_CAP="$cap" \
  BACKPRESSURE_INFLIGHT_ALPHA="$alpha" \
  BACKPRESSURE_INFLIGHT_TICK_MS=200 \
  BACKPRESSURE_INFLIGHT_SLEW_RATIO=0.25 \
  BACKPRESSURE_INFLIGHT_TARGET_WAL_AGE_SEC=1.0 \
  FASTPATH_DRAIN_ENABLE=1 \
  FASTPATH_DRAIN_WORKERS=4 \
  AUDIT_ASYNC_WAL=1 \
  AUDIT_FDATASYNC=1 \
  AUDIT_FDATASYNC_MAX_WAIT_US=200 \
  AUDIT_FDATASYNC_MAX_BATCH=64 \
  KAFKA_ENABLE=0 \
  JWT_HS256_SECRET="$JWT_SECRET" \
  GATEWAY_PORT="$PORT" \
  GATEWAY_TCP_PORT=0 \
  "$ROOT_DIR/gateway-rust/target/debug/gateway-rust" \
    > "$OUT_DIR/inflight_server_${STAMP}_${cap}_${alpha}.log" 2>&1 &
  SERVER_PID=$!

  for _ in $(seq 1 200); do
    if curl -sf "http://$HOST:$PORT/health" >/dev/null; then
      return 0
    fi
    sleep 0.25
  done

  echo "ERROR: server failed to start (cap=$cap alpha=$alpha)" >&2
  tail -n 80 "$OUT_DIR/inflight_server_${STAMP}_${cap}_${alpha}.log" >&2 || true
  return 1
}

stop_server() {
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
}

metrics_value() {
  local key="$1"
  local text="$2"
  printf "%s\n" "$text" | awk -v k="$key" '$1==k{print $2}' | tail -n1
}

run_case_once() {
  local case_name="$1"
  local run_idx="$2"
  local cap="$3"
  local alpha="$4"

  ensure_port_free
  start_server "$cap" "$alpha"

  # warmup to stabilize token/cache paths a bit
  python3 scripts/ops/bench_gateway.py throughput \
    --host "$HOST" --port "$PORT" \
    --duration 2 --concurrency 60 --accounts 16 >/dev/null 2>&1 || true

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

  local bp queue ack limit maxinf inflight
  bp=$(metrics_value gateway_backpressure_inflight_total "$metrics")
  queue=$(metrics_value gateway_reject_queue_full_total "$metrics")
  ack=$(metrics_value gateway_ack_p99_us "$metrics")
  limit=$(metrics_value gateway_inflight_limit_dynamic "$metrics")
  inflight=$(metrics_value gateway_inflight_current "$metrics")

  echo "$case_name,$run_idx,$cap,$alpha,$thr,$thr_p99,$rejected,$bp,$queue,$ack,$limit,$limit,$inflight" >> "$RAW_CSV"

  stop_server
  unset SERVER_PID
}

trap 'stop_server' EXIT

# case_name,cap,alpha
CASES=(
  "A_1024_08,1024,0.8"
  "B_0512_10,512,1.0"
)

for c in "${CASES[@]}"; do
  IFS=',' read -r case_name cap alpha <<< "$c"
  for i in $(seq 1 "$RUNS"); do
    echo "==> running $case_name run=$i cap=$cap alpha=$alpha"
    run_case_once "$case_name" "$i" "$cap" "$alpha"
  done
done

python3 - "$RAW_CSV" "$SUMMARY_MD" <<'PY'
import csv, statistics, sys
from collections import defaultdict

raw_csv, summary_md = sys.argv[1], sys.argv[2]
rows = list(csv.DictReader(open(raw_csv)))
by = defaultdict(list)
for r in rows:
    by[r['case_name']].append(r)

def med(vals):
    return statistics.median([float(v) for v in vals])

def medi(vals):
    return int(round(med(vals)))

lines = []
lines.append("| case | cap | alpha | throughput_rps(median) | thr_p99_us(median) | rejected(median) | bp_inflight_total(median) | queue_full_total(median) | ack_p99_us(median) | limit_dynamic(median) |")
lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
for case in sorted(by.keys()):
    g = by[case]
    cap = g[0]['cap']
    alpha = g[0]['alpha']
    line = "| {case} | {cap} | {alpha} | {thr:.2f} | {p99:.2f} | {rej} | {bp} | {q} | {ack} | {lim} |".format(
        case=case,
        cap=cap,
        alpha=alpha,
        thr=med([x['throughput_rps'] for x in g]),
        p99=med([x['thr_p99_us'] for x in g]),
        rej=medi([x['rejected'] for x in g]),
        bp=medi([x['bp_inflight_total'] for x in g]),
        q=medi([x['queue_full_total'] for x in g]),
        ack=medi([x['ack_p99_us'] for x in g]),
        lim=medi([x['limit_max'] for x in g]),
    )
    lines.append(line)

open(summary_md, 'w').write("\n".join(lines) + "\n")
print("\n".join(lines))
PY

echo "RAW_CSV=$RAW_CSV"
echo "SUMMARY_MD=$SUMMARY_MD"
