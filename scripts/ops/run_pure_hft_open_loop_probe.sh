#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-29001}"
TARGET_RPS="${TARGET_RPS:-10000}"
DURATION="${DURATION:-30}"
LOAD_WORKERS="${LOAD_WORKERS:-64}"
LOAD_ACCOUNTS="${LOAD_ACCOUNTS:-16}"
ACCOUNT_PREFIX="${ACCOUNT_PREFIX:-openloop}"
REQUEST_TIMEOUT_SEC="${REQUEST_TIMEOUT_SEC:-2}"
DRAIN_TIMEOUT_SEC="${DRAIN_TIMEOUT_SEC:-5}"
BUILD_RELEASE="${BUILD_RELEASE:-0}"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/var/results}"
TARGET_COMPLETED_RPS="${TARGET_COMPLETED_RPS:-$TARGET_RPS}"
TARGET_ACK_P99_US="${TARGET_ACK_P99_US:-100}"
TARGET_ACCEPTED_RATE="${TARGET_ACCEPTED_RATE:-0.99}"
TARGET_REJECTED_KILLED_MAX="${TARGET_REJECTED_KILLED_MAX:-0}"
TARGET_LOSS_SUSPECT_MAX="${TARGET_LOSS_SUSPECT_MAX:-0}"
TARGET_OFFERED_RPS_RATIO_MIN="${TARGET_OFFERED_RPS_RATIO_MIN:-0.99}"
TARGET_DROPPED_OFFER_RATIO_MAX="${TARGET_DROPPED_OFFER_RATIO_MAX:-0.001}"
TARGET_UNSENT_TOTAL_MAX="${TARGET_UNSENT_TOTAL_MAX:-0}"
ENFORCE_GATE="${ENFORCE_GATE:-0}"

# Raised preset by default (can be overridden via env).
V3_SOFT_REJECT_PCT="${V3_SOFT_REJECT_PCT:-85}"
V3_HARD_REJECT_PCT="${V3_HARD_REJECT_PCT:-92}"
V3_KILL_REJECT_PCT="${V3_KILL_REJECT_PCT:-98}"
V3_KILL_AUTO_RECOVER="${V3_KILL_AUTO_RECOVER:-false}"
V3_INGRESS_QUEUE_CAPACITY="${V3_INGRESS_QUEUE_CAPACITY:-131072}"
V3_DURABILITY_QUEUE_CAPACITY="${V3_DURABILITY_QUEUE_CAPACITY:-200000}"
V3_SHARD_COUNT="${V3_SHARD_COUNT:-4}"
V3_LOSS_GAP_TIMEOUT_MS="${V3_LOSS_GAP_TIMEOUT_MS:-2000}"
V3_SESSION_LOSS_SUSPECT_THRESHOLD="${V3_SESSION_LOSS_SUSPECT_THRESHOLD:-1}"
V3_SHARD_LOSS_SUSPECT_THRESHOLD="${V3_SHARD_LOSS_SUSPECT_THRESHOLD:-3}"
V3_GLOBAL_LOSS_SUSPECT_THRESHOLD="${V3_GLOBAL_LOSS_SUSPECT_THRESHOLD:-6}"
FASTPATH_DRAIN_WORKERS="${FASTPATH_DRAIN_WORKERS:-4}"

if [[ -z "${LOAD_QUEUE_CAPACITY:-}" ]]; then
  LOAD_QUEUE_CAPACITY=$((TARGET_RPS * 2))
fi

mkdir -p "$OUT_DIR"
cd "$ROOT_DIR"

STAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="$OUT_DIR/pure_hft_open_loop_${STAMP}.gateway.log"
LOAD_OUT="$OUT_DIR/pure_hft_open_loop_${STAMP}.load.txt"
METRICS_OUT="$OUT_DIR/pure_hft_open_loop_${STAMP}.metrics.prom"
SUMMARY_OUT="$OUT_DIR/pure_hft_open_loop_${STAMP}.summary.txt"

if [[ "$BUILD_RELEASE" == "1" ]]; then
  echo "[build] building release binary..."
  scripts/ops/build_gateway_rust_release.sh >/tmp/pure_hft_open_loop_build.log 2>&1
elif [[ ! -x ./gateway-rust/target/release/gateway-rust ]]; then
  echo "[build] release binary not found. building..."
  scripts/ops/build_gateway_rust_release.sh >/tmp/pure_hft_open_loop_build.log 2>&1
fi

cleanup() {
  if [[ -n "${GATEWAY_PID:-}" ]]; then
    kill "${GATEWAY_PID}" >/dev/null 2>&1 || true
    wait "${GATEWAY_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "[start] gateway-rust on ${HOST}:${PORT}"
V3_SOFT_REJECT_PCT="$V3_SOFT_REJECT_PCT" \
V3_HARD_REJECT_PCT="$V3_HARD_REJECT_PCT" \
V3_KILL_REJECT_PCT="$V3_KILL_REJECT_PCT" \
V3_KILL_AUTO_RECOVER="$V3_KILL_AUTO_RECOVER" \
V3_INGRESS_QUEUE_CAPACITY="$V3_INGRESS_QUEUE_CAPACITY" \
V3_DURABILITY_QUEUE_CAPACITY="$V3_DURABILITY_QUEUE_CAPACITY" \
V3_SHARD_COUNT="$V3_SHARD_COUNT" \
V3_LOSS_GAP_TIMEOUT_MS="$V3_LOSS_GAP_TIMEOUT_MS" \
V3_SESSION_LOSS_SUSPECT_THRESHOLD="$V3_SESSION_LOSS_SUSPECT_THRESHOLD" \
V3_SHARD_LOSS_SUSPECT_THRESHOLD="$V3_SHARD_LOSS_SUSPECT_THRESHOLD" \
V3_GLOBAL_LOSS_SUSPECT_THRESHOLD="$V3_GLOBAL_LOSS_SUSPECT_THRESHOLD" \
GATEWAY_PORT="$PORT" \
GATEWAY_TCP_PORT=0 \
JWT_HS256_SECRET=secret123 \
KAFKA_ENABLE=0 \
FASTPATH_DRAIN_ENABLE=1 \
FASTPATH_DRAIN_WORKERS="$FASTPATH_DRAIN_WORKERS" \
./gateway-rust/target/release/gateway-rust >"$LOG_FILE" 2>&1 &
GATEWAY_PID=$!

for _ in $(seq 1 100); do
  if curl -sS "http://${HOST}:${PORT}/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done
if ! curl -sS "http://${HOST}:${PORT}/health" >/dev/null 2>&1; then
  echo "FAIL: gateway failed to start"
  exit 1
fi

echo "[load] open-loop /v3/orders target_rps=${TARGET_RPS} duration=${DURATION}s workers=${LOAD_WORKERS} accounts=${LOAD_ACCOUNTS}"
python3 scripts/ops/open_loop_v3_load.py \
  --host "$HOST" \
  --port "$PORT" \
  --path /v3/orders \
  --duration-sec "$DURATION" \
  --target-rps "$TARGET_RPS" \
  --workers "$LOAD_WORKERS" \
  --accounts "$LOAD_ACCOUNTS" \
  --account-prefix "$ACCOUNT_PREFIX" \
  --jwt-secret secret123 \
  --queue-capacity "$LOAD_QUEUE_CAPACITY" \
  --request-timeout-sec "$REQUEST_TIMEOUT_SEC" \
  --drain-timeout-sec "$DRAIN_TIMEOUT_SEC" >"$LOAD_OUT"

curl -sS "http://${HOST}:${PORT}/metrics" >"$METRICS_OUT"

load_value() {
  local key="$1"
  awk -F= -v k="$key" '$1==k {print $2}' "$LOAD_OUT" | tail -n1
}

metric_value() {
  local key="$1"
  awk -v k="$key" '$1==k {print $2}' "$METRICS_OUT" | tail -n1
}

offered_rps="$(load_value offered_rps)"
completed_rps="$(load_value completed_rps)"
client_accepted_rate="$(load_value accepted_rate)"
client_ack_p99_us="$(load_value client_ack_p99_us)"
client_ack_accepted_p99_us="$(load_value client_ack_accepted_p99_us)"
accepted_total="$(load_value accepted_total)"
rejected_total="$(load_value rejected_total)"
error_total="$(load_value error_total)"
drop_ratio="$(load_value dropped_offer_ratio)"
unsent_total="$(load_value unsent_total)"
offered_total="$(load_value offered_total)"
status_202_total="$(load_value status_202_total)"
status_429_total="$(load_value status_429_total)"
status_503_total="$(load_value status_503_total)"

server_live_ack_p99_us="$(metric_value gateway_live_ack_p99_us)"
server_live_ack_accepted_p99_us="$(metric_value gateway_live_ack_accepted_p99_us)"
server_accepted_rate="$(metric_value gateway_v3_accepted_rate)"
server_accepted_total="$(metric_value gateway_v3_accepted_total)"
server_rejected_soft_total="$(metric_value gateway_v3_rejected_soft_total)"
server_rejected_hard_total="$(metric_value gateway_v3_rejected_hard_total)"
server_rejected_killed_total="$(metric_value gateway_v3_rejected_killed_total)"
server_loss_suspect_total="$(metric_value gateway_v3_loss_suspect_total)"

completed_rps="${completed_rps:-0}"
offered_rps="${offered_rps:-0}"
drop_ratio="${drop_ratio:-1}"
unsent_total="${unsent_total:-999999999}"
server_live_ack_p99_us="${server_live_ack_p99_us:-999999}"
server_accepted_rate="${server_accepted_rate:-0}"
server_rejected_killed_total="${server_rejected_killed_total:-0}"
server_loss_suspect_total="${server_loss_suspect_total:-0}"

offered_rps_ratio="$(awk -v o="$offered_rps" -v t="$TARGET_RPS" 'BEGIN{if (t+0<=0) {print 0} else {printf "%.6f", (o+0)/(t+0)}}')"
pass_completed_rps="$(awk -v v="$completed_rps" -v t="$TARGET_COMPLETED_RPS" 'BEGIN{print (v+0>=t+0) ? 1 : 0}')"
pass_ack="$(awk -v v="$server_live_ack_p99_us" -v t="$TARGET_ACK_P99_US" 'BEGIN{print (v+0<=t+0) ? 1 : 0}')"
pass_rate="$(awk -v v="$server_accepted_rate" -v t="$TARGET_ACCEPTED_RATE" 'BEGIN{print (v+0>=t+0) ? 1 : 0}')"
pass_killed="$(awk -v v="$server_rejected_killed_total" -v t="$TARGET_REJECTED_KILLED_MAX" 'BEGIN{print (v+0<=t+0) ? 1 : 0}')"
pass_loss="$(awk -v v="$server_loss_suspect_total" -v t="$TARGET_LOSS_SUSPECT_MAX" 'BEGIN{print (v+0<=t+0) ? 1 : 0}')"
pass_offered_ratio="$(awk -v v="$offered_rps_ratio" -v t="$TARGET_OFFERED_RPS_RATIO_MIN" 'BEGIN{print (v+0>=t+0) ? 1 : 0}')"
pass_drop_ratio="$(awk -v v="$drop_ratio" -v t="$TARGET_DROPPED_OFFER_RATIO_MAX" 'BEGIN{print (v+0<=t+0) ? 1 : 0}')"
pass_unsent="$(awk -v v="$unsent_total" -v t="$TARGET_UNSENT_TOTAL_MAX" 'BEGIN{print (v+0<=t+0) ? 1 : 0}')"
overall_pass=$((pass_completed_rps & pass_ack & pass_rate & pass_killed & pass_loss & pass_offered_ratio & pass_drop_ratio & pass_unsent))

cat >"$SUMMARY_OUT" <<EOF
pure_hft_open_loop_probe
date=${STAMP}
host=${HOST}:${PORT}
duration_sec=${DURATION}
target_rps=${TARGET_RPS}
target_completed_rps=${TARGET_COMPLETED_RPS}
target_live_ack_p99_us=${TARGET_ACK_P99_US}
target_accepted_rate=${TARGET_ACCEPTED_RATE}
target_rejected_killed_max=${TARGET_REJECTED_KILLED_MAX}
target_loss_suspect_max=${TARGET_LOSS_SUSPECT_MAX}
target_offered_rps_ratio_min=${TARGET_OFFERED_RPS_RATIO_MIN}
target_dropped_offer_ratio_max=${TARGET_DROPPED_OFFER_RATIO_MAX}
target_unsent_total_max=${TARGET_UNSENT_TOTAL_MAX}
load_workers=${LOAD_WORKERS}
load_accounts=${LOAD_ACCOUNTS}
load_queue_capacity=${LOAD_QUEUE_CAPACITY}
v3_soft_reject_pct=${V3_SOFT_REJECT_PCT}
v3_hard_reject_pct=${V3_HARD_REJECT_PCT}
v3_kill_reject_pct=${V3_KILL_REJECT_PCT}
v3_ingress_queue_capacity=${V3_INGRESS_QUEUE_CAPACITY}
v3_durability_queue_capacity=${V3_DURABILITY_QUEUE_CAPACITY}
v3_loss_gap_timeout_ms=${V3_LOSS_GAP_TIMEOUT_MS}
v3_session_loss_suspect_threshold=${V3_SESSION_LOSS_SUSPECT_THRESHOLD}
v3_shard_loss_suspect_threshold=${V3_SHARD_LOSS_SUSPECT_THRESHOLD}
v3_global_loss_suspect_threshold=${V3_GLOBAL_LOSS_SUSPECT_THRESHOLD}
offered_total=${offered_total}
offered_rps=${offered_rps}
offered_rps_ratio=${offered_rps_ratio}
completed_rps=${completed_rps}
client_accepted_rate=${client_accepted_rate}
client_ack_p99_us=${client_ack_p99_us}
client_ack_accepted_p99_us=${client_ack_accepted_p99_us}
client_accepted_total=${accepted_total}
client_rejected_total=${rejected_total}
client_error_total=${error_total}
client_unsent_total=${unsent_total}
client_dropped_offer_ratio=${drop_ratio}
status_202_total=${status_202_total}
status_429_total=${status_429_total}
status_503_total=${status_503_total}
server_live_ack_p99_us=${server_live_ack_p99_us}
server_live_ack_accepted_p99_us=${server_live_ack_accepted_p99_us}
server_accepted_rate=${server_accepted_rate}
server_accepted_total=${server_accepted_total}
server_rejected_soft_total=${server_rejected_soft_total}
server_rejected_hard_total=${server_rejected_hard_total}
server_rejected_killed_total=${server_rejected_killed_total}
server_loss_suspect_total=${server_loss_suspect_total}
gate_pass_completed_rps=${pass_completed_rps}
gate_pass_live_ack_p99=${pass_ack}
gate_pass_accepted_rate=${pass_rate}
gate_pass_rejected_killed=${pass_killed}
gate_pass_loss_suspect=${pass_loss}
gate_pass_offered_rps_ratio=${pass_offered_ratio}
gate_pass_dropped_offer_ratio=${pass_drop_ratio}
gate_pass_unsent_total=${pass_unsent}
gate_pass=${overall_pass}
load_out=${LOAD_OUT}
metrics_out=${METRICS_OUT}
gateway_log=${LOG_FILE}
EOF

echo "[summary] offered_rps=${offered_rps} completed_rps=${completed_rps} client_accepted_rate=${client_accepted_rate} server_accepted_rate=${server_accepted_rate}"
echo "[summary] server_live_ack_p99_us=${server_live_ack_p99_us} server_live_ack_accepted_p99_us=${server_live_ack_accepted_p99_us}"
echo "[gate] completed_rps>=${TARGET_COMPLETED_RPS}:${pass_completed_rps} live_ack_p99<=${TARGET_ACK_P99_US}:${pass_ack} accepted_rate>=${TARGET_ACCEPTED_RATE}:${pass_rate} rejected_killed<=${TARGET_REJECTED_KILLED_MAX}:${pass_killed} loss_suspect<=${TARGET_LOSS_SUSPECT_MAX}:${pass_loss}"
echo "[gate_supply] offered_rps_ratio>=${TARGET_OFFERED_RPS_RATIO_MIN}:${pass_offered_ratio} dropped_offer_ratio<=${TARGET_DROPPED_OFFER_RATIO_MAX}:${pass_drop_ratio} unsent_total<=${TARGET_UNSENT_TOTAL_MAX}:${pass_unsent}"
echo "[artifacts] summary=${SUMMARY_OUT}"
echo "[artifacts] load=${LOAD_OUT}"
echo "[artifacts] metrics=${METRICS_OUT}"

if [[ "$ENFORCE_GATE" == "1" && "$overall_pass" != "1" ]]; then
  echo "FAIL: open-loop strict gate not satisfied"
  exit 1
fi
