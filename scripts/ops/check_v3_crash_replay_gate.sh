#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-29101}"
TCP_PORT="${TCP_PORT:-0}"
TARGET_RPS="${TARGET_RPS:-5000}"
DURATION_BEFORE="${DURATION_BEFORE:-20}"
DURATION_AFTER="${DURATION_AFTER:-20}"
LOAD_WORKERS="${LOAD_WORKERS:-64}"
LOAD_ACCOUNTS="${LOAD_ACCOUNTS:-16}"
LOAD_QUEUE_CAPACITY="${LOAD_QUEUE_CAPACITY:-200000}"
REQUEST_TIMEOUT_SEC="${REQUEST_TIMEOUT_SEC:-2}"
DRAIN_TIMEOUT_SEC="${DRAIN_TIMEOUT_SEC:-10}"
TARGET_ACCEPTED_RATE="${TARGET_ACCEPTED_RATE:-0.99}"
TARGET_WRITE_ERROR_TOTAL_MAX="${TARGET_WRITE_ERROR_TOTAL_MAX:-0}"
TARGET_RECEIPT_TIMEOUT_TOTAL_MAX="${TARGET_RECEIPT_TIMEOUT_TOTAL_MAX:-0}"
TARGET_LOSS_SUSPECT_TOTAL_MAX="${TARGET_LOSS_SUSPECT_TOTAL_MAX:-0}"
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

mkdir -p "$OUT_DIR"
cd "$ROOT_DIR"

STAMP="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="$OUT_DIR/v3_crash_replay_gate_${STAMP}"
mkdir -p "$RUN_DIR"

GATEWAY_LOG_BEFORE="$RUN_DIR/gateway_before.log"
GATEWAY_LOG_AFTER="$RUN_DIR/gateway_after.log"
LOAD_BEFORE_OUT="$RUN_DIR/load_before.txt"
LOAD_AFTER_OUT="$RUN_DIR/load_after.txt"
METRICS_BEFORE="$RUN_DIR/metrics_before.prom"
METRICS_REBUILD="$RUN_DIR/metrics_rebuild.prom"
METRICS_AFTER="$RUN_DIR/metrics_after.prom"
SUMMARY="$RUN_DIR/summary.txt"
AUDIT_LOG_PATH="$RUN_DIR/audit.log"
PID_FILE="$RUN_DIR/gateway.pid"

if [[ "$BUILD_RELEASE" == "1" ]]; then
  scripts/ops/build_gateway_rust_release.sh >/tmp/v3_crash_replay_build.log 2>&1
elif [[ ! -x ./gateway-rust/target/release/gateway-rust ]]; then
  scripts/ops/build_gateway_rust_release.sh >/tmp/v3_crash_replay_build.log 2>&1
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
  local log_file="$1"
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
  JWT_HS256_SECRET=secret123 \
  KAFKA_ENABLE=0 \
  V3_CONFIRM_REBUILD_ON_START=true \
  AUDIT_LOG_PATH="$AUDIT_LOG_PATH" \
  ./gateway-rust/target/release/gateway-rust >"$log_file" 2>&1 &
  echo $! >"$PID_FILE"
  if ! wait_health; then
    local pid
    pid="$(cat "$PID_FILE")"
    if ps -p "$pid" >/dev/null 2>&1; then
      kill -TERM "$pid" >/dev/null 2>&1 || true
      wait "$pid" >/dev/null 2>&1 || true
    fi
    echo "--- gateway startup log (tail) ---"
    tail -n 80 "$log_file" || true
    echo "FAIL: gateway failed to start (log=$log_file)"
    exit 1
  fi
}

stop_gateway_term() {
  if [[ -f "$PID_FILE" ]]; then
    local pid
    pid="$(cat "$PID_FILE")"
    kill -TERM "$pid" >/dev/null 2>&1 || true
    wait "$pid" >/dev/null 2>&1 || true
    rm -f "$PID_FILE"
  fi
}

stop_gateway_kill() {
  if [[ -f "$PID_FILE" ]]; then
    local pid
    pid="$(cat "$PID_FILE")"
    kill -KILL "$pid" >/dev/null 2>&1 || true
    wait "$pid" >/dev/null 2>&1 || true
    rm -f "$PID_FILE"
  fi
}

cleanup() {
  stop_gateway_term
}
trap cleanup EXIT

echo "[phase1] start gateway"
start_gateway "$GATEWAY_LOG_BEFORE"

echo "[phase1] open-loop load"
python3 scripts/ops/open_loop_v3_load.py \
  --host "$HOST" \
  --port "$PORT" \
  --path /v3/orders \
  --duration-sec "$DURATION_BEFORE" \
  --target-rps "$TARGET_RPS" \
  --workers "$LOAD_WORKERS" \
  --accounts "$LOAD_ACCOUNTS" \
  --account-prefix "crash_before" \
  --jwt-secret secret123 \
  --queue-capacity "$LOAD_QUEUE_CAPACITY" \
  --request-timeout-sec "$REQUEST_TIMEOUT_SEC" \
  --drain-timeout-sec "$DRAIN_TIMEOUT_SEC" >"$LOAD_BEFORE_OUT"

curl -sS "http://${HOST}:${PORT}/metrics" >"$METRICS_BEFORE"

pre_loss_suspect_total="$(metric_value "$METRICS_BEFORE" gateway_v3_loss_suspect_total)"
pre_loss_suspect_total="${pre_loss_suspect_total:-0}"

echo "[phase1] crash gateway"
stop_gateway_kill

echo "[phase2] restart gateway (rebuild)"
start_gateway "$GATEWAY_LOG_AFTER"
curl -sS "http://${HOST}:${PORT}/metrics" >"$METRICS_REBUILD"

rebuild_restored_total="$(metric_value "$METRICS_REBUILD" gateway_v3_confirm_rebuild_restored_total)"
rebuild_elapsed_ms="$(metric_value "$METRICS_REBUILD" gateway_v3_confirm_rebuild_elapsed_ms)"
rebuild_restored_total="${rebuild_restored_total:-0}"
rebuild_elapsed_ms="${rebuild_elapsed_ms:-0}"

echo "[phase2] post-restart load"
python3 scripts/ops/open_loop_v3_load.py \
  --host "$HOST" \
  --port "$PORT" \
  --path /v3/orders \
  --duration-sec "$DURATION_AFTER" \
  --target-rps "$TARGET_RPS" \
  --workers "$LOAD_WORKERS" \
  --accounts "$LOAD_ACCOUNTS" \
  --account-prefix "crash_after" \
  --jwt-secret secret123 \
  --queue-capacity "$LOAD_QUEUE_CAPACITY" \
  --request-timeout-sec "$REQUEST_TIMEOUT_SEC" \
  --drain-timeout-sec "$DRAIN_TIMEOUT_SEC" >"$LOAD_AFTER_OUT"

curl -sS "http://${HOST}:${PORT}/metrics" >"$METRICS_AFTER"
stop_gateway_term

post_accepted_rate="$(load_value "$LOAD_AFTER_OUT" accepted_rate)"
post_accepted_rate="${post_accepted_rate:-0}"
post_write_error_total="$(metric_value "$METRICS_AFTER" gateway_v3_durable_write_error_total)"
post_receipt_timeout_total="$(metric_value "$METRICS_AFTER" gateway_v3_durable_receipt_timeout_total)"
post_loss_suspect_total="$(metric_value "$METRICS_AFTER" gateway_v3_loss_suspect_total)"
post_write_error_total="${post_write_error_total:-0}"
post_receipt_timeout_total="${post_receipt_timeout_total:-0}"
post_loss_suspect_total="${post_loss_suspect_total:-0}"

delta_loss_suspect_total="$(awk -v a="$post_loss_suspect_total" -v b="$pre_loss_suspect_total" 'BEGIN{d=(a+0)-(b+0); if (d<0) d=0; printf "%.0f", d}')"

pass_rebuild="$(awk -v v="$rebuild_restored_total" 'BEGIN{print (v+0>0)?1:0}')"
pass_accepted_rate="$(awk -v v="$post_accepted_rate" -v t="$TARGET_ACCEPTED_RATE" 'BEGIN{print (v+0>=t+0)?1:0}')"
pass_write_error="$(awk -v v="$post_write_error_total" -v t="$TARGET_WRITE_ERROR_TOTAL_MAX" 'BEGIN{print (v+0<=t+0)?1:0}')"
pass_receipt_timeout="$(awk -v v="$post_receipt_timeout_total" -v t="$TARGET_RECEIPT_TIMEOUT_TOTAL_MAX" 'BEGIN{print (v+0<=t+0)?1:0}')"
pass_loss_suspect="$(awk -v v="$delta_loss_suspect_total" -v t="$TARGET_LOSS_SUSPECT_TOTAL_MAX" 'BEGIN{print (v+0<=t+0)?1:0}')"
gate_pass=$((pass_rebuild & pass_accepted_rate & pass_write_error & pass_receipt_timeout & pass_loss_suspect))

cat <<EOF >"$SUMMARY"
v3_crash_replay_gate
target_rps=${TARGET_RPS}
duration_before=${DURATION_BEFORE}
duration_after=${DURATION_AFTER}
target_accepted_rate=${TARGET_ACCEPTED_RATE}
target_write_error_total_max=${TARGET_WRITE_ERROR_TOTAL_MAX}
target_receipt_timeout_total_max=${TARGET_RECEIPT_TIMEOUT_TOTAL_MAX}
target_loss_suspect_total_max=${TARGET_LOSS_SUSPECT_TOTAL_MAX}
v3_soft_reject_pct=${V3_SOFT_REJECT_PCT}
v3_hard_reject_pct=${V3_HARD_REJECT_PCT}
v3_kill_reject_pct=${V3_KILL_REJECT_PCT}
v3_kill_auto_recover=${V3_KILL_AUTO_RECOVER}
v3_session_loss_suspect_threshold=${V3_SESSION_LOSS_SUSPECT_THRESHOLD}
v3_shard_loss_suspect_threshold=${V3_SHARD_LOSS_SUSPECT_THRESHOLD}
v3_global_loss_suspect_threshold=${V3_GLOBAL_LOSS_SUSPECT_THRESHOLD}
v3_durable_control_preset=${V3_DURABLE_CONTROL_PRESET}
v3_durable_worker_batch_wait_us=${V3_DURABLE_WORKER_BATCH_WAIT_US}
v3_durable_worker_batch_wait_min_us=${V3_DURABLE_WORKER_BATCH_WAIT_MIN_US}
rebuild_restored_total=${rebuild_restored_total}
rebuild_elapsed_ms=${rebuild_elapsed_ms}
post_accepted_rate=${post_accepted_rate}
post_write_error_total=${post_write_error_total}
post_receipt_timeout_total=${post_receipt_timeout_total}
pre_loss_suspect_total=${pre_loss_suspect_total}
post_loss_suspect_total=${post_loss_suspect_total}
delta_loss_suspect_total=${delta_loss_suspect_total}
pass_rebuild=${pass_rebuild}
pass_accepted_rate=${pass_accepted_rate}
pass_write_error=${pass_write_error}
pass_receipt_timeout=${pass_receipt_timeout}
pass_loss_suspect=${pass_loss_suspect}
gate_pass=${gate_pass}
audit_log_path=${AUDIT_LOG_PATH}
metrics_before=${METRICS_BEFORE}
metrics_rebuild=${METRICS_REBUILD}
metrics_after=${METRICS_AFTER}
load_before=${LOAD_BEFORE_OUT}
load_after=${LOAD_AFTER_OUT}
gateway_log_before=${GATEWAY_LOG_BEFORE}
gateway_log_after=${GATEWAY_LOG_AFTER}
EOF

echo "[artifacts] summary=${SUMMARY}"
echo "[artifacts] run_dir=${RUN_DIR}"

if [[ "$gate_pass" == "1" ]]; then
  echo "PASS: crash replay gate satisfied"
  exit 0
fi

echo "FAIL: crash replay gate failed"
exit 1
