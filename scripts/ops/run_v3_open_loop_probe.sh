#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-29001}"
# Disable TCP listener by default for HTTP-only gate runs.
TCP_PORT="${TCP_PORT:-0}"
V3_INGRESS_TRANSPORT="${V3_INGRESS_TRANSPORT:-http}"
V3_TCP_PORT="${V3_TCP_PORT:-39001}"
TARGET_RPS="${TARGET_RPS:-10000}"
DURATION="${DURATION:-30}"
ACCOUNT_PREFIX="${ACCOUNT_PREFIX:-openloop}"
REQUEST_TIMEOUT_SEC="${REQUEST_TIMEOUT_SEC:-2}"
DRAIN_TIMEOUT_SEC="${DRAIN_TIMEOUT_SEC:-5}"
BUILD_RELEASE="${BUILD_RELEASE:-0}"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/var/results}"
TARGET_COMPLETED_RPS="${TARGET_COMPLETED_RPS:-$TARGET_RPS}"
TARGET_ACK_P99_US="${TARGET_ACK_P99_US:-100}"
TARGET_ACK_ACCEPTED_P99_US="${TARGET_ACK_ACCEPTED_P99_US:-40}"
TARGET_ACCEPTED_RATE="${TARGET_ACCEPTED_RATE:-0.99}"
TARGET_DURABLE_CONFIRM_P99_US="${TARGET_DURABLE_CONFIRM_P99_US:-0}"
WARN_DURABLE_CONFIRM_P99_US="${WARN_DURABLE_CONFIRM_P99_US:-0}"
TARGET_REJECTED_KILLED_MAX="${TARGET_REJECTED_KILLED_MAX:-0}"
TARGET_LOSS_SUSPECT_MAX="${TARGET_LOSS_SUSPECT_MAX:-0}"
TARGET_OFFERED_RPS_RATIO_MIN="${TARGET_OFFERED_RPS_RATIO_MIN:-0.99}"
TARGET_DROPPED_OFFER_RATIO_MAX="${TARGET_DROPPED_OFFER_RATIO_MAX:-0.001}"
TARGET_UNSENT_TOTAL_MAX="${TARGET_UNSENT_TOTAL_MAX:-0}"
TARGET_STRICT_LANE_TOPOLOGY="${TARGET_STRICT_LANE_TOPOLOGY:-1}"
TARGET_STRICT_PER_LANE_CHECKS="${TARGET_STRICT_PER_LANE_CHECKS:-1}"
TARGET_DURABLE_INFLIGHT_SKEW_RATIO_MAX="${TARGET_DURABLE_INFLIGHT_SKEW_RATIO_MAX:-3.50}"
TARGET_DURABLE_INFLIGHT_HOT_LANE_SHARE_MAX="${TARGET_DURABLE_INFLIGHT_HOT_LANE_SHARE_MAX:-0.40}"
WARN_DURABLE_INFLIGHT_SKEW_RATIO="${WARN_DURABLE_INFLIGHT_SKEW_RATIO:-2.50}"
WARN_DURABLE_INFLIGHT_HOT_LANE_SHARE="${WARN_DURABLE_INFLIGHT_HOT_LANE_SHARE:-0.30}"
ENFORCE_GATE="${ENFORCE_GATE:-0}"

# Raised preset by default (can be overridden via env).
V3_SOFT_REJECT_PCT="${V3_SOFT_REJECT_PCT:-85}"
V3_HARD_REJECT_PCT="${V3_HARD_REJECT_PCT:-92}"
V3_KILL_REJECT_PCT="${V3_KILL_REJECT_PCT:-98}"
V3_KILL_AUTO_RECOVER="${V3_KILL_AUTO_RECOVER:-false}"
V3_INGRESS_QUEUE_CAPACITY="${V3_INGRESS_QUEUE_CAPACITY:-131072}"
V3_DURABILITY_QUEUE_CAPACITY="${V3_DURABILITY_QUEUE_CAPACITY:-4000000}"
V3_LOSS_GAP_TIMEOUT_MS="${V3_LOSS_GAP_TIMEOUT_MS:-2000}"
V3_SESSION_LOSS_SUSPECT_THRESHOLD="${V3_SESSION_LOSS_SUSPECT_THRESHOLD:-1}"
V3_SHARD_LOSS_SUSPECT_THRESHOLD="${V3_SHARD_LOSS_SUSPECT_THRESHOLD:-3}"
V3_GLOBAL_LOSS_SUSPECT_THRESHOLD="${V3_GLOBAL_LOSS_SUSPECT_THRESHOLD:-6}"
V3_DURABLE_WORKER_BATCH_MAX="${V3_DURABLE_WORKER_BATCH_MAX:-24}"
V3_DURABLE_WORKER_BATCH_MIN="${V3_DURABLE_WORKER_BATCH_MIN:-12}"
V3_DURABLE_WORKER_BATCH_WAIT_US="${V3_DURABLE_WORKER_BATCH_WAIT_US:-40}"
V3_DURABLE_WORKER_BATCH_WAIT_MIN_US="${V3_DURABLE_WORKER_BATCH_WAIT_MIN_US:-20}"
V3_DURABLE_WORKER_RECEIPT_TIMEOUT_US="${V3_DURABLE_WORKER_RECEIPT_TIMEOUT_US:-20000000}"
V3_DURABLE_WORKER_MAX_INFLIGHT_RECEIPTS="${V3_DURABLE_WORKER_MAX_INFLIGHT_RECEIPTS:-49152}"
V3_DURABLE_WORKER_INFLIGHT_SOFT_CAP_PCT="${V3_DURABLE_WORKER_INFLIGHT_SOFT_CAP_PCT:-50}"
V3_DURABLE_WORKER_INFLIGHT_HARD_CAP_PCT="${V3_DURABLE_WORKER_INFLIGHT_HARD_CAP_PCT:-25}"
V3_DURABLE_WORKER_BATCH_ADAPTIVE="${V3_DURABLE_WORKER_BATCH_ADAPTIVE:-false}"
V3_DURABLE_WORKER_BATCH_ADAPTIVE_LOW_UTIL_PCT="${V3_DURABLE_WORKER_BATCH_ADAPTIVE_LOW_UTIL_PCT:-10}"
V3_DURABLE_WORKER_BATCH_ADAPTIVE_HIGH_UTIL_PCT="${V3_DURABLE_WORKER_BATCH_ADAPTIVE_HIGH_UTIL_PCT:-60}"
V3_DURABLE_ADMISSION_CONTROLLER_ENABLED="${V3_DURABLE_ADMISSION_CONTROLLER_ENABLED:-true}"
V3_DURABLE_ADMISSION_SUSTAIN_TICKS="${V3_DURABLE_ADMISSION_SUSTAIN_TICKS:-4}"
V3_DURABLE_ADMISSION_RECOVER_TICKS="${V3_DURABLE_ADMISSION_RECOVER_TICKS:-8}"
V3_DURABLE_ADMISSION_SOFT_FSYNC_P99_US="${V3_DURABLE_ADMISSION_SOFT_FSYNC_P99_US:-6000}"
V3_DURABLE_ADMISSION_HARD_FSYNC_P99_US="${V3_DURABLE_ADMISSION_HARD_FSYNC_P99_US:-12000}"
V3_DURABLE_ADMISSION_FSYNC_PRESIGNAL_PCT="${V3_DURABLE_ADMISSION_FSYNC_PRESIGNAL_PCT:-1.0}"
V3_DURABLE_SOFT_REJECT_PCT="${V3_DURABLE_SOFT_REJECT_PCT:-$V3_SOFT_REJECT_PCT}"
V3_DURABLE_HARD_REJECT_PCT="${V3_DURABLE_HARD_REJECT_PCT:-$V3_HARD_REJECT_PCT}"
V3_DURABLE_BACKLOG_SIGNAL_MIN_QUEUE_PCT="${V3_DURABLE_BACKLOG_SIGNAL_MIN_QUEUE_PCT:-30}"
V3_DURABLE_CONFIRM_SOFT_REJECT_AGE_US="${V3_DURABLE_CONFIRM_SOFT_REJECT_AGE_US:-0}"
V3_DURABLE_CONFIRM_HARD_REJECT_AGE_US="${V3_DURABLE_CONFIRM_HARD_REJECT_AGE_US:-0}"
V3_CONFIRM_REBUILD_ON_START="${V3_CONFIRM_REBUILD_ON_START:-false}"
V3_CONFIRM_REBUILD_MAX_LINES="${V3_CONFIRM_REBUILD_MAX_LINES:-500000}"
FASTPATH_DRAIN_WORKERS="${FASTPATH_DRAIN_WORKERS:-4}"
AUDIT_FDATASYNC_MAX_WAIT_US="${AUDIT_FDATASYNC_MAX_WAIT_US:-150}"
AUDIT_FDATASYNC_MAX_BATCH="${AUDIT_FDATASYNC_MAX_BATCH:-64}"

V3_INGRESS_TRANSPORT="$(echo "$V3_INGRESS_TRANSPORT" | tr '[:upper:]' '[:lower:]')"
case "$V3_INGRESS_TRANSPORT" in
  http)
    V3_HTTP_INGRESS_ENABLE="${V3_HTTP_INGRESS_ENABLE:-true}"
    V3_HTTP_CONFIRM_ENABLE="${V3_HTTP_CONFIRM_ENABLE:-true}"
    V3_TCP_ENABLE="${V3_TCP_ENABLE:-false}"
    ;;
  tcp)
    V3_HTTP_INGRESS_ENABLE="${V3_HTTP_INGRESS_ENABLE:-false}"
    V3_HTTP_CONFIRM_ENABLE="${V3_HTTP_CONFIRM_ENABLE:-true}"
    V3_TCP_ENABLE="${V3_TCP_ENABLE:-true}"
    ;;
  *)
    echo "FAIL: unknown V3_INGRESS_TRANSPORT=$V3_INGRESS_TRANSPORT (expected http|tcp)"
    exit 1
    ;;
esac
if [[ "$V3_INGRESS_TRANSPORT" == "tcp" && "$V3_TCP_PORT" -le 0 ]]; then
  echo "FAIL: V3_TCP_PORT must be > 0 for tcp transport"
  exit 1
fi
if [[ "$V3_HTTP_INGRESS_ENABLE" == "true" || "$V3_HTTP_CONFIRM_ENABLE" == "true" ]]; then
  V3_HTTP_ENABLE="true"
else
  V3_HTTP_ENABLE="false"
fi

# For 12k+ exploration, prefer a higher default load generator profile.
is_high_target="$(awk -v r="$TARGET_RPS" 'BEGIN{print ((r+0)>=12000)?1:0}')"
if [[ -z "${LOAD_WORKERS:-}" ]]; then
  if [[ "$is_high_target" == "1" ]]; then
    LOAD_WORKERS=192
  else
    LOAD_WORKERS=64
  fi
fi
if [[ -z "${LOAD_ACCOUNTS:-}" ]]; then
  if [[ "$is_high_target" == "1" ]]; then
    LOAD_ACCOUNTS=24
  else
    LOAD_ACCOUNTS=16
  fi
fi

if [[ -z "${LOAD_QUEUE_CAPACITY:-}" ]]; then
  LOAD_QUEUE_CAPACITY=$((TARGET_RPS * 2))
fi
LOAD_MAX_BURST_PER_TICK="${LOAD_MAX_BURST_PER_TICK:-2000}"
LOAD_FINAL_CATCHUP="${LOAD_FINAL_CATCHUP:-false}"
if [[ -z "${V3_SHARD_COUNT:-}" ]]; then
  if [[ "$is_high_target" == "1" ]]; then
    V3_SHARD_COUNT=8
  else
    V3_SHARD_COUNT=4
  fi
fi
if [[ "$is_high_target" == "1" ]]; then
  if [[ "${V3_LOSS_GAP_TIMEOUT_MS}" == "2000" ]]; then
    V3_LOSS_GAP_TIMEOUT_MS=360000
  fi
  if [[ "${V3_SESSION_LOSS_SUSPECT_THRESHOLD}" == "1" ]]; then
    V3_SESSION_LOSS_SUSPECT_THRESHOLD=4096
  fi
  if [[ "${V3_SHARD_LOSS_SUSPECT_THRESHOLD}" == "3" ]]; then
    V3_SHARD_LOSS_SUSPECT_THRESHOLD=32768
  fi
  if [[ "${V3_GLOBAL_LOSS_SUSPECT_THRESHOLD}" == "6" ]]; then
    V3_GLOBAL_LOSS_SUSPECT_THRESHOLD=131072
  fi
fi

mkdir -p "$OUT_DIR"
cd "$ROOT_DIR"

STAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="$OUT_DIR/v3_open_loop_${STAMP}.gateway.log"
LOAD_OUT="$OUT_DIR/v3_open_loop_${STAMP}.load.txt"
METRICS_OUT="$OUT_DIR/v3_open_loop_${STAMP}.metrics.prom"
SUMMARY_OUT="$OUT_DIR/v3_open_loop_${STAMP}.summary.txt"

if [[ "$BUILD_RELEASE" == "1" ]]; then
  echo "[build] building release binary..."
  scripts/ops/build_gateway_rust_release.sh >/tmp/v3_open_loop_build.log 2>&1
elif [[ ! -x ./gateway-rust/target/release/gateway-rust ]]; then
  echo "[build] release binary not found. building..."
  scripts/ops/build_gateway_rust_release.sh >/tmp/v3_open_loop_build.log 2>&1
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
V3_DURABLE_WORKER_BATCH_MAX="$V3_DURABLE_WORKER_BATCH_MAX" \
V3_DURABLE_WORKER_BATCH_MIN="$V3_DURABLE_WORKER_BATCH_MIN" \
V3_DURABLE_WORKER_BATCH_WAIT_US="$V3_DURABLE_WORKER_BATCH_WAIT_US" \
V3_DURABLE_WORKER_BATCH_WAIT_MIN_US="$V3_DURABLE_WORKER_BATCH_WAIT_MIN_US" \
V3_DURABLE_WORKER_RECEIPT_TIMEOUT_US="$V3_DURABLE_WORKER_RECEIPT_TIMEOUT_US" \
V3_DURABLE_WORKER_MAX_INFLIGHT_RECEIPTS="$V3_DURABLE_WORKER_MAX_INFLIGHT_RECEIPTS" \
V3_DURABLE_WORKER_INFLIGHT_SOFT_CAP_PCT="$V3_DURABLE_WORKER_INFLIGHT_SOFT_CAP_PCT" \
V3_DURABLE_WORKER_INFLIGHT_HARD_CAP_PCT="$V3_DURABLE_WORKER_INFLIGHT_HARD_CAP_PCT" \
V3_DURABLE_WORKER_BATCH_ADAPTIVE="$V3_DURABLE_WORKER_BATCH_ADAPTIVE" \
V3_DURABLE_WORKER_BATCH_ADAPTIVE_LOW_UTIL_PCT="$V3_DURABLE_WORKER_BATCH_ADAPTIVE_LOW_UTIL_PCT" \
V3_DURABLE_WORKER_BATCH_ADAPTIVE_HIGH_UTIL_PCT="$V3_DURABLE_WORKER_BATCH_ADAPTIVE_HIGH_UTIL_PCT" \
V3_DURABLE_ADMISSION_CONTROLLER_ENABLED="$V3_DURABLE_ADMISSION_CONTROLLER_ENABLED" \
V3_DURABLE_ADMISSION_SUSTAIN_TICKS="$V3_DURABLE_ADMISSION_SUSTAIN_TICKS" \
V3_DURABLE_ADMISSION_RECOVER_TICKS="$V3_DURABLE_ADMISSION_RECOVER_TICKS" \
V3_DURABLE_ADMISSION_SOFT_FSYNC_P99_US="$V3_DURABLE_ADMISSION_SOFT_FSYNC_P99_US" \
V3_DURABLE_ADMISSION_HARD_FSYNC_P99_US="$V3_DURABLE_ADMISSION_HARD_FSYNC_P99_US" \
V3_DURABLE_ADMISSION_FSYNC_PRESIGNAL_PCT="$V3_DURABLE_ADMISSION_FSYNC_PRESIGNAL_PCT" \
V3_DURABLE_SOFT_REJECT_PCT="$V3_DURABLE_SOFT_REJECT_PCT" \
V3_DURABLE_HARD_REJECT_PCT="$V3_DURABLE_HARD_REJECT_PCT" \
V3_DURABLE_BACKLOG_SIGNAL_MIN_QUEUE_PCT="$V3_DURABLE_BACKLOG_SIGNAL_MIN_QUEUE_PCT" \
V3_DURABLE_CONFIRM_SOFT_REJECT_AGE_US="$V3_DURABLE_CONFIRM_SOFT_REJECT_AGE_US" \
V3_DURABLE_CONFIRM_HARD_REJECT_AGE_US="$V3_DURABLE_CONFIRM_HARD_REJECT_AGE_US" \
V3_CONFIRM_REBUILD_ON_START="$V3_CONFIRM_REBUILD_ON_START" \
V3_CONFIRM_REBUILD_MAX_LINES="$V3_CONFIRM_REBUILD_MAX_LINES" \
AUDIT_FDATASYNC_MAX_WAIT_US="$AUDIT_FDATASYNC_MAX_WAIT_US" \
AUDIT_FDATASYNC_MAX_BATCH="$AUDIT_FDATASYNC_MAX_BATCH" \
V3_HTTP_ENABLE="$V3_HTTP_ENABLE" \
V3_HTTP_INGRESS_ENABLE="$V3_HTTP_INGRESS_ENABLE" \
V3_HTTP_CONFIRM_ENABLE="$V3_HTTP_CONFIRM_ENABLE" \
V3_TCP_ENABLE="$V3_TCP_ENABLE" \
V3_TCP_PORT="$V3_TCP_PORT" \
GATEWAY_PORT="$PORT" \
GATEWAY_TCP_PORT="$TCP_PORT" \
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

echo "[load] open-loop transport=${V3_INGRESS_TRANSPORT} target_rps=${TARGET_RPS} duration=${DURATION}s workers=${LOAD_WORKERS} accounts=${LOAD_ACCOUNTS}"
load_final_catchup_flag=()
if [[ "${LOAD_FINAL_CATCHUP}" == "1" || "${LOAD_FINAL_CATCHUP}" == "true" || "${LOAD_FINAL_CATCHUP}" == "TRUE" ]]; then
  load_final_catchup_flag+=(--final-catchup)
fi
if [[ "$V3_INGRESS_TRANSPORT" == "tcp" ]]; then
  python3 scripts/ops/open_loop_v3_tcp_load.py \
    --host "$HOST" \
    --port "$PORT" \
    --tcp-port "$V3_TCP_PORT" \
    --duration-sec "$DURATION" \
    --target-rps "$TARGET_RPS" \
    --workers "$LOAD_WORKERS" \
    --accounts "$LOAD_ACCOUNTS" \
    --account-prefix "$ACCOUNT_PREFIX" \
    --jwt-secret secret123 \
    --queue-capacity "$LOAD_QUEUE_CAPACITY" \
    --max-burst-per-tick "$LOAD_MAX_BURST_PER_TICK" \
    --request-timeout-sec "$REQUEST_TIMEOUT_SEC" \
    --drain-timeout-sec "$DRAIN_TIMEOUT_SEC" \
    "${load_final_catchup_flag[@]}" >"$LOAD_OUT"
else
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
    --max-burst-per-tick "$LOAD_MAX_BURST_PER_TICK" \
    --request-timeout-sec "$REQUEST_TIMEOUT_SEC" \
    --drain-timeout-sec "$DRAIN_TIMEOUT_SEC" \
    "${load_final_catchup_flag[@]}" >"$LOAD_OUT"
fi

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
server_durable_confirm_p99_us="$(metric_value gateway_v3_durable_confirm_p99_us)"
server_durable_backpressure_soft_total="$(metric_value gateway_v3_durable_backpressure_soft_total)"
server_durable_backpressure_hard_total="$(metric_value gateway_v3_durable_backpressure_hard_total)"
server_durable_admission_controller_enabled="$(metric_value gateway_v3_durable_admission_controller_enabled)"
server_durable_admission_level="$(metric_value gateway_v3_durable_admission_level)"
server_durable_admission_soft_trip_total="$(metric_value gateway_v3_durable_admission_soft_trip_total)"
server_durable_admission_hard_trip_total="$(metric_value gateway_v3_durable_admission_hard_trip_total)"
server_durable_admission_sustain_ticks="$(metric_value gateway_v3_durable_admission_sustain_ticks)"
server_durable_admission_recover_ticks="$(metric_value gateway_v3_durable_admission_recover_ticks)"
server_durable_admission_soft_fsync_p99_us="$(metric_value gateway_v3_durable_admission_soft_fsync_p99_us)"
server_durable_admission_hard_fsync_p99_us="$(metric_value gateway_v3_durable_admission_hard_fsync_p99_us)"
server_durable_admission_fsync_presignal_pct="$(metric_value gateway_v3_durable_admission_fsync_presignal_pct)"
server_durable_soft_reject_pct="$(metric_value gateway_v3_durable_soft_reject_pct)"
server_durable_hard_reject_pct="$(metric_value gateway_v3_durable_hard_reject_pct)"
server_durable_backlog_soft_reject_per_sec="$(metric_value gateway_v3_durable_backlog_soft_reject_per_sec)"
server_durable_backlog_hard_reject_per_sec="$(metric_value gateway_v3_durable_backlog_hard_reject_per_sec)"
server_durable_backlog_signal_min_queue_pct="$(metric_value gateway_v3_durable_backlog_signal_min_queue_pct)"
server_durable_lanes="$(metric_value gateway_v3_durable_lanes)"
server_confirm_store_lanes="$(metric_value gateway_v3_confirm_store_lanes)"
server_durable_worker_max_inflight_receipts="$(metric_value gateway_v3_durable_worker_max_inflight_receipts)"
server_durable_worker_max_inflight_receipts_global="$(metric_value gateway_v3_durable_worker_max_inflight_receipts_global)"
server_durable_receipt_inflight_max="$(metric_value gateway_v3_durable_receipt_inflight_max)"

lane_stats="$(awk '
/^gateway_v3_durable_receipt_inflight_max_per_lane\{lane=\"[0-9]+\"\} /{
  v=$2+0;
  if (count==0 || v>max) max=v;
  if (count==0 || v<min) min=v;
  sum+=v;
  count++;
}
END{
  if (count==0) {
    printf "0 0 0 0 1.000000 0.000000";
    exit;
  }
  avg=sum/count;
  skew=(avg>0)?max/avg:1;
  share=(sum>0)?max/sum:0;
  printf "%d %.6f %.6f %.6f %.6f %.6f", count, max, min, avg, skew, share;
}' "$METRICS_OUT")"
read -r server_durable_receipt_inflight_lanes_observed \
  server_durable_receipt_inflight_max_lane_max \
  server_durable_receipt_inflight_max_lane_min \
  server_durable_receipt_inflight_max_lane_avg \
  server_durable_receipt_inflight_skew_ratio \
  server_durable_receipt_inflight_hot_lane_share <<<"$lane_stats"

completed_rps="${completed_rps:-0}"
offered_rps="${offered_rps:-0}"
drop_ratio="${drop_ratio:-1}"
unsent_total="${unsent_total:-999999999}"
server_live_ack_p99_us="${server_live_ack_p99_us:-999999}"
server_accepted_rate="${server_accepted_rate:-0}"
server_rejected_killed_total="${server_rejected_killed_total:-0}"
server_loss_suspect_total="${server_loss_suspect_total:-0}"
server_durable_confirm_p99_us="${server_durable_confirm_p99_us:-0}"
server_durable_lanes="${server_durable_lanes:-0}"
server_confirm_store_lanes="${server_confirm_store_lanes:-0}"
server_durable_worker_max_inflight_receipts="${server_durable_worker_max_inflight_receipts:-0}"
server_durable_worker_max_inflight_receipts_global="${server_durable_worker_max_inflight_receipts_global:-0}"
server_durable_receipt_inflight_max="${server_durable_receipt_inflight_max:-0}"
server_durable_receipt_inflight_lanes_observed="${server_durable_receipt_inflight_lanes_observed:-0}"
server_durable_receipt_inflight_max_lane_max="${server_durable_receipt_inflight_max_lane_max:-0}"
server_durable_receipt_inflight_max_lane_min="${server_durable_receipt_inflight_max_lane_min:-0}"
server_durable_receipt_inflight_max_lane_avg="${server_durable_receipt_inflight_max_lane_avg:-0}"
server_durable_receipt_inflight_skew_ratio="${server_durable_receipt_inflight_skew_ratio:-1}"
server_durable_receipt_inflight_hot_lane_share="${server_durable_receipt_inflight_hot_lane_share:-0}"

offered_rps_ratio="$(awk -v o="$offered_rps" -v t="$TARGET_RPS" 'BEGIN{if (t+0<=0) {print 0} else {printf "%.6f", (o+0)/(t+0)}}')"
pass_completed_rps="$(awk -v v="$completed_rps" -v t="$TARGET_COMPLETED_RPS" 'BEGIN{print (v+0>=t+0) ? 1 : 0}')"
pass_ack="$(awk -v v="$server_live_ack_p99_us" -v t="$TARGET_ACK_P99_US" 'BEGIN{print (v+0<=t+0) ? 1 : 0}')"
pass_ack_accepted="$(awk -v v="$server_live_ack_accepted_p99_us" -v t="$TARGET_ACK_ACCEPTED_P99_US" 'BEGIN{print (v+0<=t+0) ? 1 : 0}')"
pass_rate="$(awk -v v="$server_accepted_rate" -v t="$TARGET_ACCEPTED_RATE" 'BEGIN{print (v+0>=t+0) ? 1 : 0}')"
pass_durable_confirm="$(awk -v v="$server_durable_confirm_p99_us" -v t="$TARGET_DURABLE_CONFIRM_P99_US" 'BEGIN{if ((t+0)<=0) print 1; else print (v+0<=t+0)?1:0}')"
warn_durable_confirm="$(awk -v v="$server_durable_confirm_p99_us" -v t="$WARN_DURABLE_CONFIRM_P99_US" 'BEGIN{if ((t+0)<=0) print 0; else print (v+0>t+0)?1:0}')"
pass_killed="$(awk -v v="$server_rejected_killed_total" -v t="$TARGET_REJECTED_KILLED_MAX" 'BEGIN{print (v+0<=t+0) ? 1 : 0}')"
pass_loss="$(awk -v v="$server_loss_suspect_total" -v t="$TARGET_LOSS_SUSPECT_MAX" 'BEGIN{print (v+0<=t+0) ? 1 : 0}')"
pass_offered_ratio="$(awk -v v="$offered_rps_ratio" -v t="$TARGET_OFFERED_RPS_RATIO_MIN" 'BEGIN{print (v+0>=t+0) ? 1 : 0}')"
pass_drop_ratio="$(awk -v v="$drop_ratio" -v t="$TARGET_DROPPED_OFFER_RATIO_MAX" 'BEGIN{print (v+0<=t+0) ? 1 : 0}')"
pass_unsent="$(awk -v v="$unsent_total" -v t="$TARGET_UNSENT_TOTAL_MAX" 'BEGIN{print (v+0<=t+0) ? 1 : 0}')"
pass_lane_topology="$(awk -v strict="$TARGET_STRICT_LANE_TOPOLOGY" -v d="$server_durable_lanes" -v c="$server_confirm_store_lanes" 'BEGIN{if (strict+0<=0) print 1; else print ((d+0)>0 && (d+0)==(c+0)) ? 1 : 0}')"
pass_lane_coverage="$(awk -v strict="$TARGET_STRICT_PER_LANE_CHECKS" -v obs="$server_durable_receipt_inflight_lanes_observed" -v lanes="$server_durable_lanes" 'BEGIN{if (strict+0<=0) print 1; else if ((obs+0)<=0) print 0; else if ((lanes+0)>0 && (obs+0)!=(lanes+0)) print 0; else print 1}')"
pass_lane_inflight_cap="$(awk -v strict="$TARGET_STRICT_PER_LANE_CHECKS" -v m="$server_durable_receipt_inflight_max_lane_max" -v cap="$server_durable_worker_max_inflight_receipts" 'BEGIN{if (strict+0<=0) print 1; else if ((cap+0)<=0) print 0; else print ((m+0)<=(cap+0))?1:0}')"
pass_global_inflight_cap="$(awk -v strict="$TARGET_STRICT_PER_LANE_CHECKS" -v m="$server_durable_receipt_inflight_max" -v cap="$server_durable_worker_max_inflight_receipts_global" 'BEGIN{if (strict+0<=0) print 1; else if ((cap+0)<=0) print 0; else print ((m+0)<=(cap+0))?1:0}')"
pass_lane_inflight_skew="$(awk -v strict="$TARGET_STRICT_PER_LANE_CHECKS" -v v="$server_durable_receipt_inflight_skew_ratio" -v t="$TARGET_DURABLE_INFLIGHT_SKEW_RATIO_MAX" 'BEGIN{if (strict+0<=0) print 1; else print (v+0<=t+0)?1:0}')"
pass_lane_hot_lane_share="$(awk -v strict="$TARGET_STRICT_PER_LANE_CHECKS" -v v="$server_durable_receipt_inflight_hot_lane_share" -v t="$TARGET_DURABLE_INFLIGHT_HOT_LANE_SHARE_MAX" 'BEGIN{if (strict+0<=0) print 1; else print (v+0<=t+0)?1:0}')"
pass_lane_checks=$((pass_lane_coverage & pass_lane_inflight_cap & pass_global_inflight_cap & pass_lane_inflight_skew & pass_lane_hot_lane_share))
warn_lane_inflight_skew="$(awk -v v="$server_durable_receipt_inflight_skew_ratio" -v t="$WARN_DURABLE_INFLIGHT_SKEW_RATIO" 'BEGIN{print (v+0>t+0)?1:0}')"
warn_lane_hot_lane_share="$(awk -v v="$server_durable_receipt_inflight_hot_lane_share" -v t="$WARN_DURABLE_INFLIGHT_HOT_LANE_SHARE" 'BEGIN{print (v+0>t+0)?1:0}')"
overall_pass=$((pass_completed_rps & pass_ack & pass_ack_accepted & pass_rate & pass_durable_confirm & pass_killed & pass_loss & pass_offered_ratio & pass_drop_ratio & pass_unsent & pass_lane_topology & pass_lane_checks))

cat >"$SUMMARY_OUT" <<EOF
v3_open_loop_probe
date=${STAMP}
host=${HOST}:${PORT}
duration_sec=${DURATION}
target_rps=${TARGET_RPS}
target_completed_rps=${TARGET_COMPLETED_RPS}
target_live_ack_p99_us=${TARGET_ACK_P99_US}
target_live_ack_accepted_p99_us=${TARGET_ACK_ACCEPTED_P99_US}
target_accepted_rate=${TARGET_ACCEPTED_RATE}
target_durable_confirm_p99_us=${TARGET_DURABLE_CONFIRM_P99_US}
warn_durable_confirm_p99_us=${WARN_DURABLE_CONFIRM_P99_US}
target_rejected_killed_max=${TARGET_REJECTED_KILLED_MAX}
target_loss_suspect_max=${TARGET_LOSS_SUSPECT_MAX}
target_offered_rps_ratio_min=${TARGET_OFFERED_RPS_RATIO_MIN}
target_dropped_offer_ratio_max=${TARGET_DROPPED_OFFER_RATIO_MAX}
target_unsent_total_max=${TARGET_UNSENT_TOTAL_MAX}
target_strict_lane_topology=${TARGET_STRICT_LANE_TOPOLOGY}
target_strict_per_lane_checks=${TARGET_STRICT_PER_LANE_CHECKS}
target_durable_inflight_skew_ratio_max=${TARGET_DURABLE_INFLIGHT_SKEW_RATIO_MAX}
target_durable_inflight_hot_lane_share_max=${TARGET_DURABLE_INFLIGHT_HOT_LANE_SHARE_MAX}
warn_durable_inflight_skew_ratio=${WARN_DURABLE_INFLIGHT_SKEW_RATIO}
warn_durable_inflight_hot_lane_share=${WARN_DURABLE_INFLIGHT_HOT_LANE_SHARE}
ingress_transport=${V3_INGRESS_TRANSPORT}
v3_http_ingress_enable=${V3_HTTP_INGRESS_ENABLE}
v3_http_confirm_enable=${V3_HTTP_CONFIRM_ENABLE}
v3_tcp_enable=${V3_TCP_ENABLE}
v3_tcp_port=${V3_TCP_PORT}
load_workers=${LOAD_WORKERS}
load_accounts=${LOAD_ACCOUNTS}
load_queue_capacity=${LOAD_QUEUE_CAPACITY}
load_max_burst_per_tick=${LOAD_MAX_BURST_PER_TICK}
load_final_catchup=${LOAD_FINAL_CATCHUP}
v3_soft_reject_pct=${V3_SOFT_REJECT_PCT}
v3_hard_reject_pct=${V3_HARD_REJECT_PCT}
v3_kill_reject_pct=${V3_KILL_REJECT_PCT}
v3_ingress_queue_capacity=${V3_INGRESS_QUEUE_CAPACITY}
v3_durability_queue_capacity=${V3_DURABILITY_QUEUE_CAPACITY}
v3_loss_gap_timeout_ms=${V3_LOSS_GAP_TIMEOUT_MS}
v3_session_loss_suspect_threshold=${V3_SESSION_LOSS_SUSPECT_THRESHOLD}
v3_shard_loss_suspect_threshold=${V3_SHARD_LOSS_SUSPECT_THRESHOLD}
v3_global_loss_suspect_threshold=${V3_GLOBAL_LOSS_SUSPECT_THRESHOLD}
v3_durable_worker_batch_max=${V3_DURABLE_WORKER_BATCH_MAX}
v3_durable_worker_batch_min=${V3_DURABLE_WORKER_BATCH_MIN}
v3_durable_worker_batch_wait_us=${V3_DURABLE_WORKER_BATCH_WAIT_US}
v3_durable_worker_batch_wait_min_us=${V3_DURABLE_WORKER_BATCH_WAIT_MIN_US}
v3_durable_worker_receipt_timeout_us=${V3_DURABLE_WORKER_RECEIPT_TIMEOUT_US}
v3_durable_worker_max_inflight_receipts=${V3_DURABLE_WORKER_MAX_INFLIGHT_RECEIPTS}
v3_durable_worker_inflight_soft_cap_pct=${V3_DURABLE_WORKER_INFLIGHT_SOFT_CAP_PCT}
v3_durable_worker_inflight_hard_cap_pct=${V3_DURABLE_WORKER_INFLIGHT_HARD_CAP_PCT}
v3_durable_worker_batch_adaptive=${V3_DURABLE_WORKER_BATCH_ADAPTIVE}
v3_durable_worker_batch_adaptive_low_util_pct=${V3_DURABLE_WORKER_BATCH_ADAPTIVE_LOW_UTIL_PCT}
v3_durable_worker_batch_adaptive_high_util_pct=${V3_DURABLE_WORKER_BATCH_ADAPTIVE_HIGH_UTIL_PCT}
v3_durable_admission_controller_enabled=${V3_DURABLE_ADMISSION_CONTROLLER_ENABLED}
v3_durable_admission_sustain_ticks=${V3_DURABLE_ADMISSION_SUSTAIN_TICKS}
v3_durable_admission_recover_ticks=${V3_DURABLE_ADMISSION_RECOVER_TICKS}
v3_durable_admission_soft_fsync_p99_us=${V3_DURABLE_ADMISSION_SOFT_FSYNC_P99_US}
v3_durable_admission_hard_fsync_p99_us=${V3_DURABLE_ADMISSION_HARD_FSYNC_P99_US}
v3_durable_admission_fsync_presignal_pct=${V3_DURABLE_ADMISSION_FSYNC_PRESIGNAL_PCT}
v3_durable_soft_reject_pct=${V3_DURABLE_SOFT_REJECT_PCT}
v3_durable_hard_reject_pct=${V3_DURABLE_HARD_REJECT_PCT}
v3_durable_backlog_signal_min_queue_pct=${V3_DURABLE_BACKLOG_SIGNAL_MIN_QUEUE_PCT}
v3_durable_confirm_soft_reject_age_us=${V3_DURABLE_CONFIRM_SOFT_REJECT_AGE_US}
v3_durable_confirm_hard_reject_age_us=${V3_DURABLE_CONFIRM_HARD_REJECT_AGE_US}
audit_fdatasync_max_wait_us=${AUDIT_FDATASYNC_MAX_WAIT_US}
audit_fdatasync_max_batch=${AUDIT_FDATASYNC_MAX_BATCH}
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
server_durable_confirm_p99_us=${server_durable_confirm_p99_us}
server_accepted_total=${server_accepted_total}
server_rejected_soft_total=${server_rejected_soft_total}
server_rejected_hard_total=${server_rejected_hard_total}
server_rejected_killed_total=${server_rejected_killed_total}
server_loss_suspect_total=${server_loss_suspect_total}
server_durable_backpressure_soft_total=${server_durable_backpressure_soft_total}
server_durable_backpressure_hard_total=${server_durable_backpressure_hard_total}
server_durable_admission_controller_enabled=${server_durable_admission_controller_enabled}
server_durable_admission_level=${server_durable_admission_level}
server_durable_admission_soft_trip_total=${server_durable_admission_soft_trip_total}
server_durable_admission_hard_trip_total=${server_durable_admission_hard_trip_total}
server_durable_admission_sustain_ticks=${server_durable_admission_sustain_ticks}
server_durable_admission_recover_ticks=${server_durable_admission_recover_ticks}
server_durable_admission_soft_fsync_p99_us=${server_durable_admission_soft_fsync_p99_us}
server_durable_admission_hard_fsync_p99_us=${server_durable_admission_hard_fsync_p99_us}
server_durable_admission_fsync_presignal_pct=${server_durable_admission_fsync_presignal_pct}
server_durable_soft_reject_pct=${server_durable_soft_reject_pct}
server_durable_hard_reject_pct=${server_durable_hard_reject_pct}
server_durable_backlog_soft_reject_per_sec=${server_durable_backlog_soft_reject_per_sec}
server_durable_backlog_hard_reject_per_sec=${server_durable_backlog_hard_reject_per_sec}
server_durable_backlog_signal_min_queue_pct=${server_durable_backlog_signal_min_queue_pct}
server_durable_lanes=${server_durable_lanes}
server_confirm_store_lanes=${server_confirm_store_lanes}
server_durable_worker_max_inflight_receipts=${server_durable_worker_max_inflight_receipts}
server_durable_worker_max_inflight_receipts_global=${server_durable_worker_max_inflight_receipts_global}
server_durable_receipt_inflight_max=${server_durable_receipt_inflight_max}
server_durable_receipt_inflight_lanes_observed=${server_durable_receipt_inflight_lanes_observed}
server_durable_receipt_inflight_max_lane_max=${server_durable_receipt_inflight_max_lane_max}
server_durable_receipt_inflight_max_lane_min=${server_durable_receipt_inflight_max_lane_min}
server_durable_receipt_inflight_max_lane_avg=${server_durable_receipt_inflight_max_lane_avg}
server_durable_receipt_inflight_skew_ratio=${server_durable_receipt_inflight_skew_ratio}
server_durable_receipt_inflight_hot_lane_share=${server_durable_receipt_inflight_hot_lane_share}
gate_pass_completed_rps=${pass_completed_rps}
gate_pass_live_ack_p99=${pass_ack}
gate_pass_live_ack_accepted_p99=${pass_ack_accepted}
gate_pass_accepted_rate=${pass_rate}
gate_pass_durable_confirm_p99=${pass_durable_confirm}
warn_durable_confirm_p99=${warn_durable_confirm}
gate_pass_rejected_killed=${pass_killed}
gate_pass_loss_suspect=${pass_loss}
gate_pass_offered_rps_ratio=${pass_offered_ratio}
gate_pass_dropped_offer_ratio=${pass_drop_ratio}
gate_pass_unsent_total=${pass_unsent}
gate_pass_lane_topology=${pass_lane_topology}
gate_pass_lane_coverage=${pass_lane_coverage}
gate_pass_lane_inflight_cap=${pass_lane_inflight_cap}
gate_pass_global_inflight_cap=${pass_global_inflight_cap}
gate_pass_lane_inflight_skew=${pass_lane_inflight_skew}
gate_pass_lane_hot_lane_share=${pass_lane_hot_lane_share}
gate_pass_lane_checks=${pass_lane_checks}
warn_lane_inflight_skew=${warn_lane_inflight_skew}
warn_lane_hot_lane_share=${warn_lane_hot_lane_share}
gate_pass=${overall_pass}
load_out=${LOAD_OUT}
metrics_out=${METRICS_OUT}
gateway_log=${LOG_FILE}
EOF

echo "[summary] offered_rps=${offered_rps} completed_rps=${completed_rps} client_accepted_rate=${client_accepted_rate} server_accepted_rate=${server_accepted_rate}"
echo "[summary] server_live_ack_p99_us=${server_live_ack_p99_us} server_live_ack_accepted_p99_us=${server_live_ack_accepted_p99_us}"
echo "[gate] completed_rps>=${TARGET_COMPLETED_RPS}:${pass_completed_rps} live_ack_p99<=${TARGET_ACK_P99_US}:${pass_ack} live_ack_accepted_p99<=${TARGET_ACK_ACCEPTED_P99_US}:${pass_ack_accepted} accepted_rate>=${TARGET_ACCEPTED_RATE}:${pass_rate} durable_confirm_p99<=${TARGET_DURABLE_CONFIRM_P99_US}:${pass_durable_confirm} rejected_killed<=${TARGET_REJECTED_KILLED_MAX}:${pass_killed} loss_suspect<=${TARGET_LOSS_SUSPECT_MAX}:${pass_loss} lane_topology:${pass_lane_topology} lane_checks:${pass_lane_checks}"
echo "[lane] observed_lanes=${server_durable_receipt_inflight_lanes_observed} skew_ratio=${server_durable_receipt_inflight_skew_ratio} hot_lane_share=${server_durable_receipt_inflight_hot_lane_share} max_lane_inflight=${server_durable_receipt_inflight_max_lane_max} max_global_inflight=${server_durable_receipt_inflight_max}"
echo "[gate_lane] coverage:${pass_lane_coverage} lane_cap:${pass_lane_inflight_cap} global_cap:${pass_global_inflight_cap} skew<=${TARGET_DURABLE_INFLIGHT_SKEW_RATIO_MAX}:${pass_lane_inflight_skew} hot_share<=${TARGET_DURABLE_INFLIGHT_HOT_LANE_SHARE_MAX}:${pass_lane_hot_lane_share}"
echo "[warn] durable_confirm_p99>${WARN_DURABLE_CONFIRM_P99_US}:${warn_durable_confirm} observed=${server_durable_confirm_p99_us}"
echo "[warn_lane] skew>${WARN_DURABLE_INFLIGHT_SKEW_RATIO}:${warn_lane_inflight_skew} hot_lane_share>${WARN_DURABLE_INFLIGHT_HOT_LANE_SHARE}:${warn_lane_hot_lane_share}"
echo "[gate_supply] offered_rps_ratio>=${TARGET_OFFERED_RPS_RATIO_MIN}:${pass_offered_ratio} dropped_offer_ratio<=${TARGET_DROPPED_OFFER_RATIO_MAX}:${pass_drop_ratio} unsent_total<=${TARGET_UNSENT_TOTAL_MAX}:${pass_unsent}"
echo "[artifacts] summary=${SUMMARY_OUT}"
echo "[artifacts] load=${LOAD_OUT}"
echo "[artifacts] metrics=${METRICS_OUT}"

if [[ "$warn_lane_inflight_skew" == "1" || "$warn_lane_hot_lane_share" == "1" ]]; then
  echo "[alert_lane_skew] WARN: skew_ratio=${server_durable_receipt_inflight_skew_ratio} (max=${TARGET_DURABLE_INFLIGHT_SKEW_RATIO_MAX}, warn=${WARN_DURABLE_INFLIGHT_SKEW_RATIO}) hot_lane_share=${server_durable_receipt_inflight_hot_lane_share} (max=${TARGET_DURABLE_INFLIGHT_HOT_LANE_SHARE_MAX}, warn=${WARN_DURABLE_INFLIGHT_HOT_LANE_SHARE})"
fi

if [[ "$ENFORCE_GATE" == "1" && "$overall_pass" != "1" ]]; then
  echo "FAIL: open-loop strict gate not satisfied"
  exit 1
fi
