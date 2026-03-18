#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-29103}"
TCP_PORT="${TCP_PORT:-0}"
TARGET_RPS="${TARGET_RPS:-5000}"
DURATION="${DURATION:-20}"
LOAD_WORKERS="${LOAD_WORKERS:-64}"
LOAD_ACCOUNTS="${LOAD_ACCOUNTS:-16}"
LOAD_QUEUE_CAPACITY="${LOAD_QUEUE_CAPACITY:-200000}"
REQUEST_TIMEOUT_SEC="${REQUEST_TIMEOUT_SEC:-2}"
DRAIN_TIMEOUT_SEC="${DRAIN_TIMEOUT_SEC:-10}"
TARGET_ACCEPTED_RATE="${TARGET_ACCEPTED_RATE:-0.99}"
TARGET_REPLICA_APPEND_TOTAL_MIN="${TARGET_REPLICA_APPEND_TOTAL_MIN:-1}"
TARGET_REPLICA_WRITE_ERROR_TOTAL_MAX="${TARGET_REPLICA_WRITE_ERROR_TOTAL_MAX:-0}"
TARGET_REPLICA_RECEIPT_TIMEOUT_TOTAL_MAX="${TARGET_REPLICA_RECEIPT_TIMEOUT_TOTAL_MAX:-0}"
BUILD_RELEASE="${BUILD_RELEASE:-0}"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/var/results}"
V3_SOFT_REJECT_PCT="${V3_SOFT_REJECT_PCT:-85}"
V3_HARD_REJECT_PCT="${V3_HARD_REJECT_PCT:-92}"
V3_KILL_REJECT_PCT="${V3_KILL_REJECT_PCT:-98}"
V3_KILL_AUTO_RECOVER="${V3_KILL_AUTO_RECOVER:-false}"
V3_SESSION_LOSS_SUSPECT_THRESHOLD="${V3_SESSION_LOSS_SUSPECT_THRESHOLD:-4096}"
V3_SHARD_LOSS_SUSPECT_THRESHOLD="${V3_SHARD_LOSS_SUSPECT_THRESHOLD:-32768}"
V3_GLOBAL_LOSS_SUSPECT_THRESHOLD="${V3_GLOBAL_LOSS_SUSPECT_THRESHOLD:-131072}"
V3_DURABLE_CONTROL_PRESET="${V3_DURABLE_CONTROL_PRESET:-hft_stable}"
V3_DURABLE_WORKER_BATCH_WAIT_US="${V3_DURABLE_WORKER_BATCH_WAIT_US:-30}"
V3_DURABLE_WORKER_BATCH_WAIT_MIN_US="${V3_DURABLE_WORKER_BATCH_WAIT_MIN_US:-15}"
V3_DURABLE_REPLICA_RECEIPT_TIMEOUT_US="${V3_DURABLE_REPLICA_RECEIPT_TIMEOUT_US:-20000000}"

mkdir -p "$OUT_DIR"
cd "$ROOT_DIR"

STAMP="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="$OUT_DIR/v3_replica_gate_${STAMP}"
mkdir -p "$RUN_DIR"

GATEWAY_LOG="$RUN_DIR/gateway.log"
LOAD_OUT="$RUN_DIR/load.txt"
METRICS_OUT="$RUN_DIR/metrics.prom"
SUMMARY="$RUN_DIR/summary.txt"
AUDIT_LOG_PATH="$RUN_DIR/audit.log"
PID_FILE="$RUN_DIR/gateway.pid"

if [[ "$BUILD_RELEASE" == "1" ]]; then
  scripts/ops/build_gateway_rust_release.sh >/tmp/v3_replica_gate_build.log 2>&1
elif [[ ! -x ./gateway-rust/target/release/gateway-rust ]]; then
  scripts/ops/build_gateway_rust_release.sh >/tmp/v3_replica_gate_build.log 2>&1
fi

metric_value() {
  local file="$1"
  local key="$2"
  awk -v k="$key" '$1==k {print $2}' "$file" | tail -n1
}

load_value() {
  local file="$1"
  local key="$2"
  awk -F= -v k="$key" '$1==k {print $2}' "$file" | tail -n1
}

wait_health() {
  local i
  for i in $(seq 1 200); do
    if curl -sS "http://${HOST}:${PORT}/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  return 1
}

start_gateway() {
  V3_HTTP_ENABLE=true \
  V3_HTTP_INGRESS_ENABLE=true \
  V3_HTTP_CONFIRM_ENABLE=true \
  V3_TCP_ENABLE=false \
  GATEWAY_PORT="$PORT" \
  GATEWAY_TCP_PORT="$TCP_PORT" \
  GATEWAY_WAL_PATH="$AUDIT_LOG_PATH" \
  GATEWAY_AUDIT_PATH="$AUDIT_LOG_PATH" \
  V3_SOFT_REJECT_PCT="$V3_SOFT_REJECT_PCT" \
  V3_HARD_REJECT_PCT="$V3_HARD_REJECT_PCT" \
  V3_KILL_REJECT_PCT="$V3_KILL_REJECT_PCT" \
  V3_KILL_AUTO_RECOVER="$V3_KILL_AUTO_RECOVER" \
  V3_SESSION_LOSS_SUSPECT_THRESHOLD="$V3_SESSION_LOSS_SUSPECT_THRESHOLD" \
  V3_SHARD_LOSS_SUSPECT_THRESHOLD="$V3_SHARD_LOSS_SUSPECT_THRESHOLD" \
  V3_GLOBAL_LOSS_SUSPECT_THRESHOLD="$V3_GLOBAL_LOSS_SUSPECT_THRESHOLD" \
  V3_DURABLE_CONTROL_PRESET="$V3_DURABLE_CONTROL_PRESET" \
  V3_DURABLE_WORKER_BATCH_WAIT_US="$V3_DURABLE_WORKER_BATCH_WAIT_US" \
  V3_DURABLE_WORKER_BATCH_WAIT_MIN_US="$V3_DURABLE_WORKER_BATCH_WAIT_MIN_US" \
  V3_DURABLE_REPLICA_ENABLED=true \
  V3_DURABLE_REPLICA_REQUIRED=true \
  V3_DURABLE_REPLICA_RECEIPT_TIMEOUT_US="$V3_DURABLE_REPLICA_RECEIPT_TIMEOUT_US" \
  JWT_HS256_SECRET=secret123 \
  KAFKA_ENABLE=0 \
  V3_CONFIRM_REBUILD_ON_START=false \
  AUDIT_LOG_PATH="$AUDIT_LOG_PATH" \
  ./gateway-rust/target/release/gateway-rust >"$GATEWAY_LOG" 2>&1 &
  echo $! >"$PID_FILE"
  if ! wait_health; then
    local pid
    pid="$(cat "$PID_FILE")"
    if ps -p "$pid" >/dev/null 2>&1; then
      kill -TERM "$pid" >/dev/null 2>&1 || true
      wait "$pid" >/dev/null 2>&1 || true
    fi
    echo "--- gateway startup log (tail) ---"
    tail -n 80 "$GATEWAY_LOG" || true
    echo "FAIL: gateway failed to start (log=$GATEWAY_LOG)"
    exit 1
  fi
}

stop_gateway() {
  if [[ -f "$PID_FILE" ]]; then
    local pid
    pid="$(cat "$PID_FILE")"
    kill -TERM "$pid" >/dev/null 2>&1 || true
    wait "$pid" >/dev/null 2>&1 || true
    rm -f "$PID_FILE"
  fi
}

cleanup() {
  stop_gateway
}
trap cleanup EXIT

echo "[phase1] start gateway"
start_gateway

echo "[phase2] open-loop load"
python3 scripts/ops/open_loop_v3_load.py \
  --host "$HOST" \
  --port "$PORT" \
  --path /v3/orders \
  --duration-sec "$DURATION" \
  --target-rps "$TARGET_RPS" \
  --workers "$LOAD_WORKERS" \
  --accounts "$LOAD_ACCOUNTS" \
  --account-prefix "replica_gate" \
  --jwt-secret secret123 \
  --queue-capacity "$LOAD_QUEUE_CAPACITY" \
  --request-timeout-sec "$REQUEST_TIMEOUT_SEC" \
  --drain-timeout-sec "$DRAIN_TIMEOUT_SEC" >"$LOAD_OUT"

curl -sS "http://${HOST}:${PORT}/metrics" >"$METRICS_OUT"
stop_gateway

accepted_rate="$(load_value "$LOAD_OUT" accepted_rate)"
accepted_rate="${accepted_rate:-0}"
replica_enabled="$(metric_value "$METRICS_OUT" gateway_v3_durable_replica_enabled)"
replica_required="$(metric_value "$METRICS_OUT" gateway_v3_durable_replica_required)"
replica_append_total="$(metric_value "$METRICS_OUT" gateway_v3_durable_replica_append_total)"
replica_write_error_total="$(metric_value "$METRICS_OUT" gateway_v3_durable_replica_write_error_total)"
replica_receipt_timeout_total="$(metric_value "$METRICS_OUT" gateway_v3_durable_replica_receipt_timeout_total)"
replica_enabled="${replica_enabled:-0}"
replica_required="${replica_required:-0}"
replica_append_total="${replica_append_total:-0}"
replica_write_error_total="${replica_write_error_total:-0}"
replica_receipt_timeout_total="${replica_receipt_timeout_total:-0}"

lane_file_count=0
replica_file_count=0
replica_file_missing_or_empty=0
for primary in "$AUDIT_LOG_PATH".v3.lane*.log; do
  if [[ ! -e "$primary" ]]; then
    continue
  fi
  lane_file_count=$((lane_file_count + 1))
  replica="${primary}.replica"
  if [[ -s "$replica" ]]; then
    replica_file_count=$((replica_file_count + 1))
  else
    replica_file_missing_or_empty=$((replica_file_missing_or_empty + 1))
  fi
done

pass_accepted_rate="$(awk -v v="$accepted_rate" -v t="$TARGET_ACCEPTED_RATE" 'BEGIN{print (v+0>=t+0)?1:0}')"
pass_replica_enabled="$(awk -v v="$replica_enabled" 'BEGIN{print (v+0==1)?1:0}')"
pass_replica_required="$(awk -v v="$replica_required" 'BEGIN{print (v+0==1)?1:0}')"
pass_replica_append="$(awk -v v="$replica_append_total" -v t="$TARGET_REPLICA_APPEND_TOTAL_MIN" 'BEGIN{print (v+0>=t+0)?1:0}')"
pass_replica_write_error="$(awk -v v="$replica_write_error_total" -v t="$TARGET_REPLICA_WRITE_ERROR_TOTAL_MAX" 'BEGIN{print (v+0<=t+0)?1:0}')"
pass_replica_receipt_timeout="$(awk -v v="$replica_receipt_timeout_total" -v t="$TARGET_REPLICA_RECEIPT_TIMEOUT_TOTAL_MAX" 'BEGIN{print (v+0<=t+0)?1:0}')"
pass_replica_files="$(awk -v l="$lane_file_count" -v r="$replica_file_count" -v m="$replica_file_missing_or_empty" 'BEGIN{print (l>0 && r==l && m==0)?1:0}')"
gate_pass=$((pass_accepted_rate & pass_replica_enabled & pass_replica_required & pass_replica_append & pass_replica_write_error & pass_replica_receipt_timeout & pass_replica_files))

cat <<EOF >"$SUMMARY"
v3_replica_gate
target_rps=${TARGET_RPS}
duration=${DURATION}
target_accepted_rate=${TARGET_ACCEPTED_RATE}
target_replica_append_total_min=${TARGET_REPLICA_APPEND_TOTAL_MIN}
target_replica_write_error_total_max=${TARGET_REPLICA_WRITE_ERROR_TOTAL_MAX}
target_replica_receipt_timeout_total_max=${TARGET_REPLICA_RECEIPT_TIMEOUT_TOTAL_MAX}
v3_soft_reject_pct=${V3_SOFT_REJECT_PCT}
v3_hard_reject_pct=${V3_HARD_REJECT_PCT}
v3_kill_reject_pct=${V3_KILL_REJECT_PCT}
v3_kill_auto_recover=${V3_KILL_AUTO_RECOVER}
v3_durable_control_preset=${V3_DURABLE_CONTROL_PRESET}
v3_durable_worker_batch_wait_us=${V3_DURABLE_WORKER_BATCH_WAIT_US}
v3_durable_worker_batch_wait_min_us=${V3_DURABLE_WORKER_BATCH_WAIT_MIN_US}
v3_durable_replica_receipt_timeout_us=${V3_DURABLE_REPLICA_RECEIPT_TIMEOUT_US}
accepted_rate=${accepted_rate}
replica_enabled=${replica_enabled}
replica_required=${replica_required}
replica_append_total=${replica_append_total}
replica_write_error_total=${replica_write_error_total}
replica_receipt_timeout_total=${replica_receipt_timeout_total}
lane_file_count=${lane_file_count}
replica_file_count=${replica_file_count}
replica_file_missing_or_empty=${replica_file_missing_or_empty}
pass_accepted_rate=${pass_accepted_rate}
pass_replica_enabled=${pass_replica_enabled}
pass_replica_required=${pass_replica_required}
pass_replica_append=${pass_replica_append}
pass_replica_write_error=${pass_replica_write_error}
pass_replica_receipt_timeout=${pass_replica_receipt_timeout}
pass_replica_files=${pass_replica_files}
gate_pass=${gate_pass}
audit_log_path=${AUDIT_LOG_PATH}
metrics=${METRICS_OUT}
load=${LOAD_OUT}
gateway_log=${GATEWAY_LOG}
EOF

echo "[artifacts] summary=${SUMMARY}"
echo "[artifacts] run_dir=${RUN_DIR}"

if [[ "$gate_pass" == "1" ]]; then
  echo "PASS: replica gate satisfied"
  exit 0
fi

echo "FAIL: replica gate failed"
exit 1
