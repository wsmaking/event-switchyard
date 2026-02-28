#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

OUT_DIR="var/results"
mkdir -p "$OUT_DIR"
STAMP="$(date +%Y%m%d_%H%M%S)"
RAW_CSV="$OUT_DIR/preset_switch_raw_${STAMP}.csv"
SUMMARY_MD="$OUT_DIR/preset_switch_summary_${STAMP}.md"

HOST="${HOST:-localhost}"
PORT="${PORT:-29001}"
JWT_SECRET="${JWT_HS256_SECRET:-secret123}"
DURATION="${DURATION:-10}"
CONCURRENCY="${CONCURRENCY:-220}"
ACCOUNTS="${ACCOUNTS:-32}"
NORMAL_WAIT_US="${NORMAL_WAIT_US:-200}"
DEGRADED_WAIT_US="${DEGRADED_WAIT_US:-5000}"

cat > "$RAW_CSV" <<'CSV'
phase,preset,wait_us,batch,cap,alpha,throughput_rps,thr_p99_us,rejected,bp_inflight_total,queue_full_total,ack_p99_us,wal_age_ms,limit_dynamic,inflight
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
  local preset="$1"
  local wait_us="$2"

  local cap alpha batch
  if [[ "$preset" == "normal" ]]; then
    cap="${NORMAL_CAP:-1024}"
    alpha="${NORMAL_ALPHA:-0.8}"
    batch="${NORMAL_BATCH:-128}"
  else
    cap="${DEGRADED_CAP:-512}"
    alpha="${DEGRADED_ALPHA:-0.8}"
    batch="${DEGRADED_BATCH:-1}"
  fi

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
  AUDIT_FDATASYNC_ADAPTIVE="${AUDIT_FDATASYNC_ADAPTIVE:-0}" \
  AUDIT_FDATASYNC_ADAPTIVE_MIN_WAIT_US="${AUDIT_FDATASYNC_ADAPTIVE_MIN_WAIT_US:-50}" \
  AUDIT_FDATASYNC_ADAPTIVE_MAX_WAIT_US="${AUDIT_FDATASYNC_ADAPTIVE_MAX_WAIT_US:-${wait_us}}" \
  AUDIT_FDATASYNC_ADAPTIVE_MIN_BATCH="${AUDIT_FDATASYNC_ADAPTIVE_MIN_BATCH:-16}" \
  AUDIT_FDATASYNC_ADAPTIVE_MAX_BATCH="${AUDIT_FDATASYNC_ADAPTIVE_MAX_BATCH:-${batch}}" \
  AUDIT_FDATASYNC_ADAPTIVE_TARGET_SYNC_US="${AUDIT_FDATASYNC_ADAPTIVE_TARGET_SYNC_US:-4000}" \
  AUDIT_FDATASYNC_MAX_WAIT_US="$wait_us" \
  AUDIT_FDATASYNC_MAX_BATCH="$batch" \
  KAFKA_ENABLE=0 \
  JWT_HS256_SECRET="$JWT_SECRET" \
  GATEWAY_PORT="$PORT" \
  GATEWAY_TCP_PORT=0 \
  "$ROOT_DIR/gateway-rust/target/debug/gateway-rust" \
    > "$OUT_DIR/preset_switch_server_${STAMP}_${preset}_${wait_us}.log" 2>&1 &
  SERVER_PID=$!

  for _ in $(seq 1 200); do
    if curl -sf "http://$HOST:$PORT/health" >/dev/null; then
      CAP_USED="$cap"
      ALPHA_USED="$alpha"
      BATCH_USED="$batch"
      return 0
    fi
    sleep 0.25
  done

  echo "ERROR: server failed to start (preset=$preset wait=$wait_us)" >&2
  tail -n 80 "$OUT_DIR/preset_switch_server_${STAMP}_${preset}_${wait_us}.log" >&2 || true
  return 1
}

stop_server() {
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
}

run_phase() {
  local phase="$1"
  local preset="$2"
  local wait_us="$3"

  ensure_port_free
  start_server "$preset" "$wait_us"

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

  local bp queue ack wal limit inflight
  bp=$(metrics_value gateway_backpressure_inflight_total "$metrics")
  queue=$(metrics_value gateway_reject_queue_full_total "$metrics")
  ack=$(metrics_value gateway_ack_p99_us "$metrics")
  wal=$(metrics_value gateway_wal_age_ms "$metrics")
  limit=$(metrics_value gateway_inflight_limit_dynamic "$metrics")
  inflight=$(metrics_value gateway_inflight "$metrics")

  echo "$phase,$preset,$wait_us,$BATCH_USED,$CAP_USED,$ALPHA_USED,$thr,$thr_p99,$rejected,$bp,$queue,$ack,$wal,$limit,$inflight" >> "$RAW_CSV"

  stop_server
  unset SERVER_PID
}

trap 'stop_server' EXIT

echo "==> phase 1: normal baseline"
run_phase "normal_baseline" "normal" "$NORMAL_WAIT_US"

echo "==> phase 2: degraded under injected stress"
run_phase "degraded_injected" "degraded" "$DEGRADED_WAIT_US"

echo "==> phase 3: back to normal (recovery check)"
run_phase "normal_recovery" "normal" "$NORMAL_WAIT_US"

python3 - "$RAW_CSV" "$SUMMARY_MD" <<'PY'
import csv, sys
raw_csv, summary_md = sys.argv[1], sys.argv[2]
rows = list(csv.DictReader(open(raw_csv)))
lines = []
lines.append("| phase | preset | wait_us | batch | cap | alpha | throughput_rps | thr_p99_us | rejected | bp_inflight_total | queue_full_total | ack_p99_us | wal_age_ms | limit_dynamic |")
lines.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
for r in rows:
    lines.append(
        "| {phase} | {preset} | {wait_us} | {batch} | {cap} | {alpha} | {throughput_rps} | {thr_p99_us} | {rejected} | {bp_inflight_total} | {queue_full_total} | {ack_p99_us} | {wal_age_ms} | {limit_dynamic} |".format(**r)
    )

open(summary_md, 'w').write("\n".join(lines) + "\n")
print("\n".join(lines))
PY

echo "RAW_CSV=$RAW_CSV"
echo "SUMMARY_MD=$SUMMARY_MD"
