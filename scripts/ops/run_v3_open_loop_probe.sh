#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
if [[ -n "${V3_OPS_PRESET:-}" ]]; then
  # shellcheck source=/dev/null
  source "$ROOT_DIR/scripts/ops/v3_preset_env.sh"
  apply_v3_ops_preset "$V3_OPS_PRESET"
fi
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-29001}"
# Legacy gateway TCP listener (non-v3 ingress) stays disabled by default.
TCP_PORT="${TCP_PORT:-0}"
V3_INGRESS_TRANSPORT="${V3_INGRESS_TRANSPORT:-tcp}"
V3_TCP_PORT="${V3_TCP_PORT:-39001}"
TARGET_RPS="${TARGET_RPS:-10000}"
DURATION="${DURATION:-30}"
ACCOUNT_PREFIX="${ACCOUNT_PREFIX:-openloop}"
REQUEST_TIMEOUT_SEC="${REQUEST_TIMEOUT_SEC:-2}"
DRAIN_TIMEOUT_SEC="${DRAIN_TIMEOUT_SEC:-5}"
BUILD_RELEASE="${BUILD_RELEASE:-0}"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/var/results}"
TARGET_COMPLETED_RPS="${TARGET_COMPLETED_RPS:-$TARGET_RPS}"
TARGET_COMPLETED_RPS_EPSILON="${TARGET_COMPLETED_RPS_EPSILON:-0.0005}"
TARGET_ACK_P99_US="${TARGET_ACK_P99_US:-100}"
TARGET_ACK_ACCEPTED_P99_US="${TARGET_ACK_ACCEPTED_P99_US:-40}"
TARGET_ACK_ACCEPTED_P99_NS="${TARGET_ACK_ACCEPTED_P99_NS:-0}"
TARGET_ACK_ACCEPTED_TSC_P99_NS="${TARGET_ACK_ACCEPTED_TSC_P99_NS:-0}"
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
ENABLE_TIMESERIES="${ENABLE_TIMESERIES:-1}"
TIMESERIES_INTERVAL_MS="${TIMESERIES_INTERVAL_MS:-500}"
TIMESERIES_TIMEOUT_SEC="${TIMESERIES_TIMEOUT_SEC:-1.0}"
TIMESERIES_METRICS="${TIMESERIES_METRICS:-gateway_v3_accepted_rate,gateway_v3_hotpath_accepted_p99_ns,gateway_live_ack_accepted_p99_ns,gateway_live_ack_accepted_p99_us,gateway_v3_rejected_soft_total,gateway_v3_rejected_hard_total,gateway_v3_rejected_killed_total,gateway_v3_loss_suspect_total,gateway_v3_durable_queue_depth,gateway_v3_durable_queue_utilization_pct,gateway_v3_durable_queue_utilization_pct_max,gateway_v3_durable_backlog_growth_per_sec,gateway_v3_durable_backpressure_soft_total,gateway_v3_durable_backpressure_hard_total,gateway_v3_durable_confirm_p99_us}"

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
V3_DURABLE_WORKER_BATCH_MAX="${V3_DURABLE_WORKER_BATCH_MAX:-12}"
V3_DURABLE_WORKER_BATCH_MIN="${V3_DURABLE_WORKER_BATCH_MIN:-12}"
V3_DURABLE_WORKER_BATCH_WAIT_US="${V3_DURABLE_WORKER_BATCH_WAIT_US:-20}"
V3_DURABLE_WORKER_BATCH_WAIT_MIN_US="${V3_DURABLE_WORKER_BATCH_WAIT_MIN_US:-20}"
V3_DURABLE_WORKER_RECEIPT_TIMEOUT_US="${V3_DURABLE_WORKER_RECEIPT_TIMEOUT_US:-5000000}"
V3_DURABLE_WORKER_MAX_INFLIGHT_RECEIPTS="${V3_DURABLE_WORKER_MAX_INFLIGHT_RECEIPTS:-4096}"
V3_DURABLE_WORKER_INFLIGHT_SOFT_CAP_PCT="${V3_DURABLE_WORKER_INFLIGHT_SOFT_CAP_PCT:-50}"
V3_DURABLE_WORKER_INFLIGHT_HARD_CAP_PCT="${V3_DURABLE_WORKER_INFLIGHT_HARD_CAP_PCT:-25}"
V3_DURABLE_CONTROL_PRESET="${V3_DURABLE_CONTROL_PRESET:-hft_stable}"
V3_DURABLE_WORKER_DYNAMIC_INFLIGHT="${V3_DURABLE_WORKER_DYNAMIC_INFLIGHT:-false}"
V3_DURABLE_WORKER_DYNAMIC_INFLIGHT_MIN_CAP_PCT="${V3_DURABLE_WORKER_DYNAMIC_INFLIGHT_MIN_CAP_PCT:-10}"
V3_DURABLE_WORKER_DYNAMIC_INFLIGHT_MAX_CAP_PCT="${V3_DURABLE_WORKER_DYNAMIC_INFLIGHT_MAX_CAP_PCT:-40}"
V3_DURABLE_WORKER_DYNAMIC_INFLIGHT_STRICT_MAX_CAP_PCT="${V3_DURABLE_WORKER_DYNAMIC_INFLIGHT_STRICT_MAX_CAP_PCT:-40}"
V3_DURABLE_PRESSURE_EWMA_ALPHA_PCT="${V3_DURABLE_PRESSURE_EWMA_ALPHA_PCT:-30}"
V3_DURABLE_DYNAMIC_CAP_SLEW_STEP_PCT="${V3_DURABLE_DYNAMIC_CAP_SLEW_STEP_PCT:-8}"
V3_DURABLE_WORKER_BATCH_ADAPTIVE="${V3_DURABLE_WORKER_BATCH_ADAPTIVE:-false}"
V3_DURABLE_WORKER_BATCH_ADAPTIVE_LOW_UTIL_PCT="${V3_DURABLE_WORKER_BATCH_ADAPTIVE_LOW_UTIL_PCT:-15}"
V3_DURABLE_WORKER_BATCH_ADAPTIVE_HIGH_UTIL_PCT="${V3_DURABLE_WORKER_BATCH_ADAPTIVE_HIGH_UTIL_PCT:-70}"
V3_DURABLE_AGE_SOFT_INFLIGHT_CAP_PCT="${V3_DURABLE_AGE_SOFT_INFLIGHT_CAP_PCT:-35}"
V3_DURABLE_AGE_HARD_INFLIGHT_CAP_PCT="${V3_DURABLE_AGE_HARD_INFLIGHT_CAP_PCT:-15}"
V3_DURABLE_SLO_STAGE="${V3_DURABLE_SLO_STAGE:-1}"
V3_DURABLE_SLO_EARLY_SOFT_AGE_US="${V3_DURABLE_SLO_EARLY_SOFT_AGE_US:-100000}"
V3_DURABLE_SLO_EARLY_HARD_AGE_US="${V3_DURABLE_SLO_EARLY_HARD_AGE_US:-220000}"
V3_DURABLE_ADMISSION_CONTROLLER_ENABLED="${V3_DURABLE_ADMISSION_CONTROLLER_ENABLED:-true}"
V3_DURABLE_ADMISSION_SUSTAIN_TICKS="${V3_DURABLE_ADMISSION_SUSTAIN_TICKS:-4}"
V3_DURABLE_ADMISSION_RECOVER_TICKS="${V3_DURABLE_ADMISSION_RECOVER_TICKS:-8}"
V3_DURABLE_ADMISSION_SOFT_FSYNC_P99_US="${V3_DURABLE_ADMISSION_SOFT_FSYNC_P99_US:-4000}"
V3_DURABLE_ADMISSION_HARD_FSYNC_P99_US="${V3_DURABLE_ADMISSION_HARD_FSYNC_P99_US:-8000}"
V3_DURABLE_ADMISSION_FSYNC_PRESIGNAL_PCT="${V3_DURABLE_ADMISSION_FSYNC_PRESIGNAL_PCT:-0.75}"
V3_DURABLE_ADMISSION_FSYNC_ONLY_SOFT_SUSTAIN_TICKS="${V3_DURABLE_ADMISSION_FSYNC_ONLY_SOFT_SUSTAIN_TICKS:-0}"
V3_DURABLE_ADMISSION_FSYNC_ONLY_HARD_SUSTAIN_TICKS="${V3_DURABLE_ADMISSION_FSYNC_ONLY_HARD_SUSTAIN_TICKS:-0}"
V3_DURABLE_FSYNC_EWMA_ALPHA_PCT="${V3_DURABLE_FSYNC_EWMA_ALPHA_PCT:-30}"
V3_DURABLE_FSYNC_SOFT_INFLIGHT_CAP_PCT="${V3_DURABLE_FSYNC_SOFT_INFLIGHT_CAP_PCT:-50}"
V3_DURABLE_FSYNC_HARD_INFLIGHT_CAP_PCT="${V3_DURABLE_FSYNC_HARD_INFLIGHT_CAP_PCT:-20}"
V3_DURABLE_FSYNC_SOFT_TRIGGER_US="${V3_DURABLE_FSYNC_SOFT_TRIGGER_US:-$V3_DURABLE_ADMISSION_SOFT_FSYNC_P99_US}"
V3_DURABLE_FSYNC_HARD_TRIGGER_US="${V3_DURABLE_FSYNC_HARD_TRIGGER_US:-$V3_DURABLE_ADMISSION_HARD_FSYNC_P99_US}"
V3_DURABLE_SOFT_REJECT_PCT="${V3_DURABLE_SOFT_REJECT_PCT:-$V3_SOFT_REJECT_PCT}"
V3_DURABLE_HARD_REJECT_PCT="${V3_DURABLE_HARD_REJECT_PCT:-$V3_HARD_REJECT_PCT}"
V3_DURABLE_BACKLOG_SIGNAL_MIN_QUEUE_PCT="${V3_DURABLE_BACKLOG_SIGNAL_MIN_QUEUE_PCT:-30}"
V3_DURABLE_CONFIRM_SOFT_REJECT_AGE_US="${V3_DURABLE_CONFIRM_SOFT_REJECT_AGE_US:-180000}"
V3_DURABLE_CONFIRM_HARD_REJECT_AGE_US="${V3_DURABLE_CONFIRM_HARD_REJECT_AGE_US:-260000}"
V3_DURABLE_CONFIRM_AGE_FSYNC_LINKED="${V3_DURABLE_CONFIRM_AGE_FSYNC_LINKED:-true}"
V3_DURABLE_CONFIRM_AGE_FSYNC_SOFT_REF_US="${V3_DURABLE_CONFIRM_AGE_FSYNC_SOFT_REF_US:-$V3_DURABLE_ADMISSION_SOFT_FSYNC_P99_US}"
V3_DURABLE_CONFIRM_AGE_FSYNC_HARD_REF_US="${V3_DURABLE_CONFIRM_AGE_FSYNC_HARD_REF_US:-$V3_DURABLE_ADMISSION_HARD_FSYNC_P99_US}"
V3_DURABLE_CONFIRM_AGE_FSYNC_MAX_RELAX_PCT="${V3_DURABLE_CONFIRM_AGE_FSYNC_MAX_RELAX_PCT:-100}"
V3_DURABLE_CONFIRM_GUARD_SOFT_SLACK_PCT="${V3_DURABLE_CONFIRM_GUARD_SOFT_SLACK_PCT:-20}"
V3_DURABLE_CONFIRM_GUARD_HARD_SLACK_PCT="${V3_DURABLE_CONFIRM_GUARD_HARD_SLACK_PCT:-40}"
V3_DURABLE_CONFIRM_GUARD_SOFT_REQUIRES_ADMISSION="${V3_DURABLE_CONFIRM_GUARD_SOFT_REQUIRES_ADMISSION:-false}"
V3_DURABLE_CONFIRM_GUARD_HARD_REQUIRES_ADMISSION="${V3_DURABLE_CONFIRM_GUARD_HARD_REQUIRES_ADMISSION:-false}"
V3_DURABLE_CONFIRM_GUARD_SECONDARY_REQUIRED="${V3_DURABLE_CONFIRM_GUARD_SECONDARY_REQUIRED:-false}"
V3_DURABLE_CONFIRM_GUARD_MIN_QUEUE_PCT="${V3_DURABLE_CONFIRM_GUARD_MIN_QUEUE_PCT:-0}"
V3_DURABLE_CONFIRM_GUARD_MIN_INFLIGHT_PCT="${V3_DURABLE_CONFIRM_GUARD_MIN_INFLIGHT_PCT:-0}"
V3_DURABLE_CONFIRM_GUARD_MIN_BACKLOG_PER_SEC="${V3_DURABLE_CONFIRM_GUARD_MIN_BACKLOG_PER_SEC:-0}"
V3_DURABLE_CONFIRM_GUARD_SOFT_SUSTAIN_TICKS="${V3_DURABLE_CONFIRM_GUARD_SOFT_SUSTAIN_TICKS:-2}"
V3_DURABLE_CONFIRM_GUARD_HARD_SUSTAIN_TICKS="${V3_DURABLE_CONFIRM_GUARD_HARD_SUSTAIN_TICKS:-2}"
V3_DURABLE_CONFIRM_GUARD_RECOVER_TICKS="${V3_DURABLE_CONFIRM_GUARD_RECOVER_TICKS:-3}"
V3_DURABLE_CONFIRM_GUARD_RECOVER_HYSTERESIS_PCT="${V3_DURABLE_CONFIRM_GUARD_RECOVER_HYSTERESIS_PCT:-85}"
V3_DURABLE_CONFIRM_GUARD_AUTOTUNE="${V3_DURABLE_CONFIRM_GUARD_AUTOTUNE:-true}"
V3_DURABLE_CONFIRM_GUARD_AUTOTUNE_LOW_PRESSURE_PCT="${V3_DURABLE_CONFIRM_GUARD_AUTOTUNE_LOW_PRESSURE_PCT:-35}"
V3_DURABLE_CONFIRM_GUARD_AUTOTUNE_HIGH_PRESSURE_PCT="${V3_DURABLE_CONFIRM_GUARD_AUTOTUNE_HIGH_PRESSURE_PCT:-80}"
V3_DURABLE_ACK_PATH_GUARD_ENABLED="${V3_DURABLE_ACK_PATH_GUARD_ENABLED:-true}"
V3_CONFIRM_REBUILD_ON_START="${V3_CONFIRM_REBUILD_ON_START:-false}"
V3_CONFIRM_REBUILD_MAX_LINES="${V3_CONFIRM_REBUILD_MAX_LINES:-500000}"
FASTPATH_DRAIN_WORKERS="${FASTPATH_DRAIN_WORKERS:-4}"
V3_TCP_BUSY_POLL_US="${V3_TCP_BUSY_POLL_US:-0}"
V3_TCP_AUTH_STICKY_CONTEXT="${V3_TCP_AUTH_STICKY_CONTEXT:-true}"
V3_TCP_STICKY_ACCOUNT_PER_WORKER="${V3_TCP_STICKY_ACCOUNT_PER_WORKER:-true}"
V3_SHARD_AFFINITY_CPUS="${V3_SHARD_AFFINITY_CPUS:-}"
V3_DURABLE_AFFINITY_CPUS="${V3_DURABLE_AFFINITY_CPUS:-}"
V3_TCP_SERVER_AFFINITY_CPUS="${V3_TCP_SERVER_AFFINITY_CPUS:-}"
V3_TSC_TIMING_ENABLE="${V3_TSC_TIMING_ENABLE:-false}"
V3_TSC_MISMATCH_THRESHOLD_PCT="${V3_TSC_MISMATCH_THRESHOLD_PCT:-20}"
V3_DEDICATED_WORKER_RUNTIME="${V3_DEDICATED_WORKER_RUNTIME:-true}"
V3_HOTPATH_HISTOGRAM_SAMPLE_RATE="${V3_HOTPATH_HISTOGRAM_SAMPLE_RATE:-1}"
V3_GATEWAY_TASKSET_CPUS="${V3_GATEWAY_TASKSET_CPUS:-}"
AUDIT_FDATASYNC_MAX_WAIT_US="${AUDIT_FDATASYNC_MAX_WAIT_US:-80}"
AUDIT_FDATASYNC_MAX_BATCH="${AUDIT_FDATASYNC_MAX_BATCH:-32}"
AUDIT_FDATASYNC_MAX_DEFER_US="${AUDIT_FDATASYNC_MAX_DEFER_US:-0}"
AUDIT_FDATASYNC_MAX_INFLIGHT_AGE_US="${AUDIT_FDATASYNC_MAX_INFLIGHT_AGE_US:-1200}"
AUDIT_FDATASYNC_MAX_PENDING_BYTES="${AUDIT_FDATASYNC_MAX_PENDING_BYTES:-131072}"
AUDIT_FDATASYNC_SERIALIZE="${AUDIT_FDATASYNC_SERIALIZE:-false}"
if [[ -z "${AUDIT_WAL_PREALLOC_BYTES:-}" ]]; then
  if [[ "$(uname -s)" == "Linux" ]]; then
    AUDIT_WAL_PREALLOC_BYTES=67108864
  else
    AUDIT_WAL_PREALLOC_BYTES=0
  fi
fi
AUDIT_WAL_PREALLOC_THRESHOLD_BYTES="${AUDIT_WAL_PREALLOC_THRESHOLD_BYTES:-8388608}"

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
    if [[ "$(uname -s)" == "Darwin" ]]; then
      V3_SHARD_COUNT=6
    else
      V3_SHARD_COUNT=8
    fi
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
TIMESERIES_OUT="$OUT_DIR/v3_open_loop_${STAMP}.timeseries.jsonl"
AUDIT_LOG_PATH="${AUDIT_LOG_PATH:-$OUT_DIR/v3_open_loop_${STAMP}.audit.log}"
GATEWAY_WAL_PATH="${GATEWAY_WAL_PATH:-$AUDIT_LOG_PATH}"
GATEWAY_AUDIT_PATH="${GATEWAY_AUDIT_PATH:-$GATEWAY_WAL_PATH}"

if [[ "$BUILD_RELEASE" == "1" ]]; then
  echo "[build] building release binary..."
  scripts/ops/build_gateway_rust_release.sh >/tmp/v3_open_loop_build.log 2>&1
elif [[ ! -x ./gateway-rust/target/release/gateway-rust ]]; then
  echo "[build] release binary not found. building..."
  scripts/ops/build_gateway_rust_release.sh >/tmp/v3_open_loop_build.log 2>&1
fi

cleanup() {
  if [[ -n "${TIMESERIES_PID:-}" ]]; then
    kill "${TIMESERIES_PID}" >/dev/null 2>&1 || true
    wait "${TIMESERIES_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${GATEWAY_PID:-}" ]]; then
    kill "${GATEWAY_PID}" >/dev/null 2>&1 || true
    wait "${GATEWAY_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

stop_timeseries_sampler() {
  if [[ -n "${TIMESERIES_PID:-}" ]]; then
    kill "${TIMESERIES_PID}" >/dev/null 2>&1 || true
    wait "${TIMESERIES_PID}" >/dev/null 2>&1 || true
    TIMESERIES_PID=""
  fi
}

echo "[start] gateway-rust on ${HOST}:${PORT}"
rm -f "$AUDIT_LOG_PATH"
rm -f "$GATEWAY_WAL_PATH"
rm -f "${GATEWAY_WAL_PATH}".v3.lane*.log >/dev/null 2>&1 || true
gateway_cmd=(./gateway-rust/target/release/gateway-rust)
if [[ "$(uname -s)" == "Linux" && -n "$V3_GATEWAY_TASKSET_CPUS" ]]; then
  if command -v taskset >/dev/null 2>&1; then
    gateway_cmd=(taskset -c "$V3_GATEWAY_TASKSET_CPUS" "${gateway_cmd[@]}")
    echo "[start] taskset cores=${V3_GATEWAY_TASKSET_CPUS}"
  else
    echo "[start] WARN: V3_GATEWAY_TASKSET_CPUS is set but taskset is not installed"
  fi
fi
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
V3_DURABLE_CONTROL_PRESET="$V3_DURABLE_CONTROL_PRESET" \
V3_DURABLE_WORKER_DYNAMIC_INFLIGHT="$V3_DURABLE_WORKER_DYNAMIC_INFLIGHT" \
V3_DURABLE_WORKER_DYNAMIC_INFLIGHT_MIN_CAP_PCT="$V3_DURABLE_WORKER_DYNAMIC_INFLIGHT_MIN_CAP_PCT" \
V3_DURABLE_WORKER_DYNAMIC_INFLIGHT_MAX_CAP_PCT="$V3_DURABLE_WORKER_DYNAMIC_INFLIGHT_MAX_CAP_PCT" \
V3_DURABLE_WORKER_DYNAMIC_INFLIGHT_STRICT_MAX_CAP_PCT="$V3_DURABLE_WORKER_DYNAMIC_INFLIGHT_STRICT_MAX_CAP_PCT" \
V3_DURABLE_PRESSURE_EWMA_ALPHA_PCT="$V3_DURABLE_PRESSURE_EWMA_ALPHA_PCT" \
V3_DURABLE_DYNAMIC_CAP_SLEW_STEP_PCT="$V3_DURABLE_DYNAMIC_CAP_SLEW_STEP_PCT" \
V3_DURABLE_WORKER_BATCH_ADAPTIVE="$V3_DURABLE_WORKER_BATCH_ADAPTIVE" \
V3_DURABLE_WORKER_BATCH_ADAPTIVE_LOW_UTIL_PCT="$V3_DURABLE_WORKER_BATCH_ADAPTIVE_LOW_UTIL_PCT" \
V3_DURABLE_WORKER_BATCH_ADAPTIVE_HIGH_UTIL_PCT="$V3_DURABLE_WORKER_BATCH_ADAPTIVE_HIGH_UTIL_PCT" \
V3_DURABLE_AGE_SOFT_INFLIGHT_CAP_PCT="$V3_DURABLE_AGE_SOFT_INFLIGHT_CAP_PCT" \
V3_DURABLE_AGE_HARD_INFLIGHT_CAP_PCT="$V3_DURABLE_AGE_HARD_INFLIGHT_CAP_PCT" \
V3_DURABLE_SLO_STAGE="$V3_DURABLE_SLO_STAGE" \
V3_DURABLE_SLO_EARLY_SOFT_AGE_US="$V3_DURABLE_SLO_EARLY_SOFT_AGE_US" \
V3_DURABLE_SLO_EARLY_HARD_AGE_US="$V3_DURABLE_SLO_EARLY_HARD_AGE_US" \
V3_DURABLE_ADMISSION_CONTROLLER_ENABLED="$V3_DURABLE_ADMISSION_CONTROLLER_ENABLED" \
V3_DURABLE_ADMISSION_SUSTAIN_TICKS="$V3_DURABLE_ADMISSION_SUSTAIN_TICKS" \
V3_DURABLE_ADMISSION_RECOVER_TICKS="$V3_DURABLE_ADMISSION_RECOVER_TICKS" \
V3_DURABLE_ADMISSION_SOFT_FSYNC_P99_US="$V3_DURABLE_ADMISSION_SOFT_FSYNC_P99_US" \
V3_DURABLE_ADMISSION_HARD_FSYNC_P99_US="$V3_DURABLE_ADMISSION_HARD_FSYNC_P99_US" \
V3_DURABLE_ADMISSION_FSYNC_PRESIGNAL_PCT="$V3_DURABLE_ADMISSION_FSYNC_PRESIGNAL_PCT" \
V3_DURABLE_ADMISSION_FSYNC_ONLY_SOFT_SUSTAIN_TICKS="$V3_DURABLE_ADMISSION_FSYNC_ONLY_SOFT_SUSTAIN_TICKS" \
V3_DURABLE_ADMISSION_FSYNC_ONLY_HARD_SUSTAIN_TICKS="$V3_DURABLE_ADMISSION_FSYNC_ONLY_HARD_SUSTAIN_TICKS" \
V3_DURABLE_FSYNC_EWMA_ALPHA_PCT="$V3_DURABLE_FSYNC_EWMA_ALPHA_PCT" \
V3_DURABLE_FSYNC_SOFT_INFLIGHT_CAP_PCT="$V3_DURABLE_FSYNC_SOFT_INFLIGHT_CAP_PCT" \
V3_DURABLE_FSYNC_HARD_INFLIGHT_CAP_PCT="$V3_DURABLE_FSYNC_HARD_INFLIGHT_CAP_PCT" \
V3_DURABLE_FSYNC_SOFT_TRIGGER_US="$V3_DURABLE_FSYNC_SOFT_TRIGGER_US" \
V3_DURABLE_FSYNC_HARD_TRIGGER_US="$V3_DURABLE_FSYNC_HARD_TRIGGER_US" \
V3_DURABLE_SOFT_REJECT_PCT="$V3_DURABLE_SOFT_REJECT_PCT" \
V3_DURABLE_HARD_REJECT_PCT="$V3_DURABLE_HARD_REJECT_PCT" \
V3_DURABLE_BACKLOG_SIGNAL_MIN_QUEUE_PCT="$V3_DURABLE_BACKLOG_SIGNAL_MIN_QUEUE_PCT" \
V3_DURABLE_CONFIRM_SOFT_REJECT_AGE_US="$V3_DURABLE_CONFIRM_SOFT_REJECT_AGE_US" \
V3_DURABLE_CONFIRM_HARD_REJECT_AGE_US="$V3_DURABLE_CONFIRM_HARD_REJECT_AGE_US" \
V3_DURABLE_CONFIRM_AGE_FSYNC_LINKED="$V3_DURABLE_CONFIRM_AGE_FSYNC_LINKED" \
V3_DURABLE_CONFIRM_AGE_FSYNC_SOFT_REF_US="$V3_DURABLE_CONFIRM_AGE_FSYNC_SOFT_REF_US" \
V3_DURABLE_CONFIRM_AGE_FSYNC_HARD_REF_US="$V3_DURABLE_CONFIRM_AGE_FSYNC_HARD_REF_US" \
V3_DURABLE_CONFIRM_AGE_FSYNC_MAX_RELAX_PCT="$V3_DURABLE_CONFIRM_AGE_FSYNC_MAX_RELAX_PCT" \
V3_DURABLE_CONFIRM_GUARD_SOFT_SLACK_PCT="$V3_DURABLE_CONFIRM_GUARD_SOFT_SLACK_PCT" \
V3_DURABLE_CONFIRM_GUARD_HARD_SLACK_PCT="$V3_DURABLE_CONFIRM_GUARD_HARD_SLACK_PCT" \
V3_DURABLE_CONFIRM_GUARD_SOFT_REQUIRES_ADMISSION="$V3_DURABLE_CONFIRM_GUARD_SOFT_REQUIRES_ADMISSION" \
V3_DURABLE_CONFIRM_GUARD_HARD_REQUIRES_ADMISSION="$V3_DURABLE_CONFIRM_GUARD_HARD_REQUIRES_ADMISSION" \
V3_DURABLE_CONFIRM_GUARD_SECONDARY_REQUIRED="$V3_DURABLE_CONFIRM_GUARD_SECONDARY_REQUIRED" \
V3_DURABLE_CONFIRM_GUARD_MIN_QUEUE_PCT="$V3_DURABLE_CONFIRM_GUARD_MIN_QUEUE_PCT" \
V3_DURABLE_CONFIRM_GUARD_MIN_INFLIGHT_PCT="$V3_DURABLE_CONFIRM_GUARD_MIN_INFLIGHT_PCT" \
V3_DURABLE_CONFIRM_GUARD_MIN_BACKLOG_PER_SEC="$V3_DURABLE_CONFIRM_GUARD_MIN_BACKLOG_PER_SEC" \
V3_DURABLE_CONFIRM_GUARD_SOFT_SUSTAIN_TICKS="$V3_DURABLE_CONFIRM_GUARD_SOFT_SUSTAIN_TICKS" \
V3_DURABLE_CONFIRM_GUARD_HARD_SUSTAIN_TICKS="$V3_DURABLE_CONFIRM_GUARD_HARD_SUSTAIN_TICKS" \
V3_DURABLE_CONFIRM_GUARD_RECOVER_TICKS="$V3_DURABLE_CONFIRM_GUARD_RECOVER_TICKS" \
V3_DURABLE_CONFIRM_GUARD_RECOVER_HYSTERESIS_PCT="$V3_DURABLE_CONFIRM_GUARD_RECOVER_HYSTERESIS_PCT" \
V3_DURABLE_CONFIRM_GUARD_AUTOTUNE="$V3_DURABLE_CONFIRM_GUARD_AUTOTUNE" \
V3_DURABLE_CONFIRM_GUARD_AUTOTUNE_LOW_PRESSURE_PCT="$V3_DURABLE_CONFIRM_GUARD_AUTOTUNE_LOW_PRESSURE_PCT" \
V3_DURABLE_CONFIRM_GUARD_AUTOTUNE_HIGH_PRESSURE_PCT="$V3_DURABLE_CONFIRM_GUARD_AUTOTUNE_HIGH_PRESSURE_PCT" \
V3_DURABLE_ACK_PATH_GUARD_ENABLED="$V3_DURABLE_ACK_PATH_GUARD_ENABLED" \
V3_CONFIRM_REBUILD_ON_START="$V3_CONFIRM_REBUILD_ON_START" \
V3_CONFIRM_REBUILD_MAX_LINES="$V3_CONFIRM_REBUILD_MAX_LINES" \
AUDIT_FDATASYNC_MAX_WAIT_US="$AUDIT_FDATASYNC_MAX_WAIT_US" \
AUDIT_FDATASYNC_MAX_BATCH="$AUDIT_FDATASYNC_MAX_BATCH" \
AUDIT_FDATASYNC_MAX_DEFER_US="$AUDIT_FDATASYNC_MAX_DEFER_US" \
AUDIT_FDATASYNC_MAX_INFLIGHT_AGE_US="$AUDIT_FDATASYNC_MAX_INFLIGHT_AGE_US" \
AUDIT_FDATASYNC_MAX_PENDING_BYTES="$AUDIT_FDATASYNC_MAX_PENDING_BYTES" \
AUDIT_FDATASYNC_SERIALIZE="$AUDIT_FDATASYNC_SERIALIZE" \
AUDIT_WAL_PREALLOC_BYTES="$AUDIT_WAL_PREALLOC_BYTES" \
AUDIT_WAL_PREALLOC_THRESHOLD_BYTES="$AUDIT_WAL_PREALLOC_THRESHOLD_BYTES" \
AUDIT_LOG_PATH="$AUDIT_LOG_PATH" \
GATEWAY_WAL_PATH="$GATEWAY_WAL_PATH" \
GATEWAY_AUDIT_PATH="$GATEWAY_AUDIT_PATH" \
V3_HTTP_ENABLE="$V3_HTTP_ENABLE" \
V3_HTTP_INGRESS_ENABLE="$V3_HTTP_INGRESS_ENABLE" \
V3_HTTP_CONFIRM_ENABLE="$V3_HTTP_CONFIRM_ENABLE" \
V3_TCP_ENABLE="$V3_TCP_ENABLE" \
V3_TCP_PORT="$V3_TCP_PORT" \
V3_TCP_BUSY_POLL_US="$V3_TCP_BUSY_POLL_US" \
V3_TCP_AUTH_STICKY_CONTEXT="$V3_TCP_AUTH_STICKY_CONTEXT" \
V3_SHARD_AFFINITY_CPUS="$V3_SHARD_AFFINITY_CPUS" \
V3_DURABLE_AFFINITY_CPUS="$V3_DURABLE_AFFINITY_CPUS" \
V3_TCP_SERVER_AFFINITY_CPUS="$V3_TCP_SERVER_AFFINITY_CPUS" \
V3_TSC_TIMING_ENABLE="$V3_TSC_TIMING_ENABLE" \
V3_TSC_MISMATCH_THRESHOLD_PCT="$V3_TSC_MISMATCH_THRESHOLD_PCT" \
V3_DEDICATED_WORKER_RUNTIME="$V3_DEDICATED_WORKER_RUNTIME" \
V3_HOTPATH_HISTOGRAM_SAMPLE_RATE="$V3_HOTPATH_HISTOGRAM_SAMPLE_RATE" \
GATEWAY_PORT="$PORT" \
GATEWAY_TCP_PORT="$TCP_PORT" \
JWT_HS256_SECRET=secret123 \
KAFKA_ENABLE=0 \
FASTPATH_DRAIN_ENABLE=1 \
FASTPATH_DRAIN_WORKERS="$FASTPATH_DRAIN_WORKERS" \
"${gateway_cmd[@]}" >"$LOG_FILE" 2>&1 &
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
timeseries_started=0
timeseries_samples=0
if [[ "$ENABLE_TIMESERIES" == "1" || "$ENABLE_TIMESERIES" == "true" || "$ENABLE_TIMESERIES" == "TRUE" ]]; then
  if command -v python3 >/dev/null 2>&1; then
    python3 scripts/ops/collect_metrics_timeseries.py \
      --url "http://${HOST}:${PORT}/metrics" \
      --out "$TIMESERIES_OUT" \
      --interval-ms "$TIMESERIES_INTERVAL_MS" \
      --metrics "$TIMESERIES_METRICS" \
      --timeout-sec "$TIMESERIES_TIMEOUT_SEC" >/dev/null 2>&1 &
    TIMESERIES_PID=$!
    timeseries_started=1
  else
    echo "[timeseries] skipped: python3 not found"
  fi
fi
load_final_catchup_flag=()
if [[ "${LOAD_FINAL_CATCHUP}" == "1" || "${LOAD_FINAL_CATCHUP}" == "true" || "${LOAD_FINAL_CATCHUP}" == "TRUE" ]]; then
  load_final_catchup_flag+=(--final-catchup)
fi
load_sticky_account_flag=()
if [[ "${V3_TCP_STICKY_ACCOUNT_PER_WORKER}" == "1" || "${V3_TCP_STICKY_ACCOUNT_PER_WORKER}" == "true" || "${V3_TCP_STICKY_ACCOUNT_PER_WORKER}" == "TRUE" ]]; then
  load_sticky_account_flag+=(--sticky-account-per-worker)
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
    "${load_sticky_account_flag[@]}" \
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
stop_timeseries_sampler
if [[ "$timeseries_started" == "1" && -f "$TIMESERIES_OUT" ]]; then
  timeseries_samples="$(wc -l <"$TIMESERIES_OUT" | awk '{print $1}')"
fi

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
client_ack_p999_us="$(load_value client_ack_p999_us)"
client_ack_accepted_p99_us="$(load_value client_ack_accepted_p99_us)"
client_ack_accepted_p999_us="$(load_value client_ack_accepted_p999_us)"
client_e2e_p99_us="$(load_value client_e2e_p99_us)"
client_e2e_p999_us="$(load_value client_e2e_p999_us)"
client_e2e_accepted_p99_us="$(load_value client_e2e_accepted_p99_us)"
client_e2e_accepted_p999_us="$(load_value client_e2e_accepted_p999_us)"
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
server_live_ack_accepted_p99_ns="$(metric_value gateway_live_ack_accepted_p99_ns)"
server_hotpath_accepted_p99_ns="$(metric_value gateway_v3_hotpath_accepted_p99_ns)"
server_hotpath_accepted_tsc_p99_ns="$(metric_value gateway_v3_hotpath_accepted_tsc_p99_ns)"
server_tsc_timing_enabled="$(metric_value gateway_v3_tsc_timing_enabled)"
server_tsc_fallback_total="$(metric_value gateway_v3_tsc_fallback_total)"
server_tsc_cross_core_total="$(metric_value gateway_v3_tsc_cross_core_total)"
server_tsc_mismatch_total="$(metric_value gateway_v3_tsc_mismatch_total)"
server_thread_affinity_apply_success_total="$(metric_value gateway_v3_thread_affinity_apply_success_total)"
server_thread_affinity_apply_failure_total="$(metric_value gateway_v3_thread_affinity_apply_failure_total)"
server_live_ack_accepted_p999_us="$(metric_value gateway_v3_live_ack_accepted_p999_us)"
server_durable_ack_path_guard_enabled="$(metric_value gateway_v3_durable_ack_path_guard_enabled)"
server_accepted_rate="$(metric_value gateway_v3_accepted_rate)"
server_accepted_total="$(metric_value gateway_v3_accepted_total)"
server_rejected_soft_total="$(metric_value gateway_v3_rejected_soft_total)"
server_rejected_hard_total="$(metric_value gateway_v3_rejected_hard_total)"
server_rejected_killed_total="$(metric_value gateway_v3_rejected_killed_total)"
server_loss_suspect_total="$(metric_value gateway_v3_loss_suspect_total)"
server_durable_confirm_p99_us="$(metric_value gateway_v3_durable_confirm_p99_us)"
server_durable_confirm_p999_us="$(metric_value gateway_v3_durable_confirm_p999_us)"
server_durable_queue_depth="$(metric_value gateway_v3_durable_queue_depth)"
server_durable_queue_capacity="$(metric_value gateway_v3_durable_queue_capacity)"
server_durable_queue_utilization_pct="$(metric_value gateway_v3_durable_queue_utilization_pct)"
server_durable_queue_utilization_pct_max="$(metric_value gateway_v3_durable_queue_utilization_pct_max)"
server_durable_backlog_growth_per_sec="$(metric_value gateway_v3_durable_backlog_growth_per_sec)"
server_durable_queue_full_total="$(metric_value gateway_v3_durable_queue_full_total)"
server_durable_queue_closed_total="$(metric_value gateway_v3_durable_queue_closed_total)"
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
client_ack_p999_us="${client_ack_p999_us:-0}"
client_ack_accepted_p999_us="${client_ack_accepted_p999_us:-0}"
if [[ -z "${client_e2e_p99_us}" ]]; then
  client_e2e_p99_us="$client_ack_p99_us"
fi
if [[ -z "${client_e2e_p999_us}" ]]; then
  client_e2e_p999_us="$client_ack_p999_us"
fi
if [[ -z "${client_e2e_accepted_p99_us}" ]]; then
  client_e2e_accepted_p99_us="$client_ack_accepted_p99_us"
fi
if [[ -z "${client_e2e_accepted_p999_us}" ]]; then
  client_e2e_accepted_p999_us="$client_ack_accepted_p999_us"
fi
server_live_ack_p99_us="${server_live_ack_p99_us:-999999}"
server_live_ack_accepted_p99_us="${server_live_ack_accepted_p99_us:-999999}"
if [[ -z "${server_live_ack_accepted_p99_ns}" ]]; then
  server_live_ack_accepted_p99_ns="$(awk -v v="$server_live_ack_accepted_p99_us" 'BEGIN{printf "%.0f", (v+0)*1000.0}')"
fi
if [[ -z "${server_hotpath_accepted_p99_ns}" ]]; then
  server_hotpath_accepted_p99_ns="$server_live_ack_accepted_p99_ns"
fi
server_tsc_timing_enabled="${server_tsc_timing_enabled:-0}"
if [[ -z "${server_hotpath_accepted_tsc_p99_ns}" ]]; then
  server_hotpath_accepted_tsc_p99_ns="$server_hotpath_accepted_p99_ns"
fi
if ! awk -v v="$server_hotpath_accepted_tsc_p99_ns" 'BEGIN{exit ((v+0)>0)?0:1}'; then
  server_hotpath_accepted_tsc_p99_ns="$server_hotpath_accepted_p99_ns"
fi
if [[ "$server_tsc_timing_enabled" != "1" ]]; then
  server_hotpath_accepted_tsc_p99_ns="$server_hotpath_accepted_p99_ns"
fi
server_live_ack_accepted_p99_ns="${server_live_ack_accepted_p99_ns:-999999999}"
server_hotpath_accepted_p99_ns="${server_hotpath_accepted_p99_ns:-999999999}"
server_hotpath_accepted_tsc_p99_ns="${server_hotpath_accepted_tsc_p99_ns:-999999999}"
server_tsc_fallback_total="${server_tsc_fallback_total:-0}"
server_tsc_cross_core_total="${server_tsc_cross_core_total:-0}"
server_tsc_mismatch_total="${server_tsc_mismatch_total:-0}"
server_thread_affinity_apply_success_total="${server_thread_affinity_apply_success_total:-0}"
server_thread_affinity_apply_failure_total="${server_thread_affinity_apply_failure_total:-0}"
server_live_ack_accepted_p999_us="${server_live_ack_accepted_p999_us:-999999}"
server_durable_ack_path_guard_enabled="${server_durable_ack_path_guard_enabled:-1}"
server_accepted_rate="${server_accepted_rate:-0}"
server_rejected_killed_total="${server_rejected_killed_total:-0}"
server_loss_suspect_total="${server_loss_suspect_total:-0}"
server_durable_confirm_p99_us="${server_durable_confirm_p99_us:-0}"
server_durable_confirm_p999_us="${server_durable_confirm_p999_us:-0}"
server_durable_queue_depth="${server_durable_queue_depth:-0}"
server_durable_queue_capacity="${server_durable_queue_capacity:-0}"
server_durable_queue_utilization_pct="${server_durable_queue_utilization_pct:-0}"
server_durable_queue_utilization_pct_max="${server_durable_queue_utilization_pct_max:-0}"
server_durable_backlog_growth_per_sec="${server_durable_backlog_growth_per_sec:-0}"
server_durable_queue_full_total="${server_durable_queue_full_total:-0}"
server_durable_queue_closed_total="${server_durable_queue_closed_total:-0}"
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
completed_rps_floor="$(awk -v t="$TARGET_COMPLETED_RPS" -v e="$TARGET_COMPLETED_RPS_EPSILON" 'BEGIN{v=(t+0)*(1.0-(e+0)); if (v<0) v=0; printf "%.6f", v}')"
pass_completed_rps="$(awk -v v="$completed_rps" -v f="$completed_rps_floor" 'BEGIN{print (v+0>=f+0) ? 1 : 0}')"
pass_ack="$(awk -v v="$server_live_ack_p99_us" -v t="$TARGET_ACK_P99_US" 'BEGIN{print (v+0<=t+0) ? 1 : 0}')"
pass_ack_accepted="$(awk -v v="$server_live_ack_accepted_p99_us" -v t="$TARGET_ACK_ACCEPTED_P99_US" 'BEGIN{print (v+0<=t+0) ? 1 : 0}')"
pass_ack_accepted_ns="$(awk -v v="$server_hotpath_accepted_p99_ns" -v t="$TARGET_ACK_ACCEPTED_P99_NS" 'BEGIN{if ((t+0)<=0) print 1; else print (v+0<=t+0) ? 1 : 0}')"
pass_ack_accepted_tsc_ns="$(awk -v v="$server_hotpath_accepted_tsc_p99_ns" -v t="$TARGET_ACK_ACCEPTED_TSC_P99_NS" 'BEGIN{if ((t+0)<=0) print 1; else print (v+0<=t+0) ? 1 : 0}')"
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
overall_pass=$((pass_completed_rps & pass_ack & pass_ack_accepted & pass_ack_accepted_ns & pass_ack_accepted_tsc_ns & pass_rate & pass_durable_confirm & pass_killed & pass_loss & pass_offered_ratio & pass_drop_ratio & pass_unsent & pass_lane_topology & pass_lane_checks))

cat >"$SUMMARY_OUT" <<EOF
v3_open_loop_probe
date=${STAMP}
host=${HOST}:${PORT}
duration_sec=${DURATION}
target_rps=${TARGET_RPS}
target_completed_rps=${TARGET_COMPLETED_RPS}
target_completed_rps_epsilon=${TARGET_COMPLETED_RPS_EPSILON}
target_completed_rps_floor=${completed_rps_floor}
target_live_ack_p99_us=${TARGET_ACK_P99_US}
target_live_ack_accepted_p99_us=${TARGET_ACK_ACCEPTED_P99_US}
target_live_ack_accepted_p99_ns=${TARGET_ACK_ACCEPTED_P99_NS}
target_live_ack_accepted_tsc_p99_ns=${TARGET_ACK_ACCEPTED_TSC_P99_NS}
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
timeseries_enabled=${ENABLE_TIMESERIES}
timeseries_interval_ms=${TIMESERIES_INTERVAL_MS}
timeseries_samples=${timeseries_samples}
timeseries_metrics=${TIMESERIES_METRICS}
ingress_transport=${V3_INGRESS_TRANSPORT}
v3_http_ingress_enable=${V3_HTTP_INGRESS_ENABLE}
v3_http_confirm_enable=${V3_HTTP_CONFIRM_ENABLE}
v3_tcp_enable=${V3_TCP_ENABLE}
v3_tcp_port=${V3_TCP_PORT}
v3_tcp_busy_poll_us=${V3_TCP_BUSY_POLL_US}
v3_tcp_auth_sticky_context=${V3_TCP_AUTH_STICKY_CONTEXT}
v3_shard_affinity_cpus=${V3_SHARD_AFFINITY_CPUS}
v3_durable_affinity_cpus=${V3_DURABLE_AFFINITY_CPUS}
v3_tcp_server_affinity_cpus=${V3_TCP_SERVER_AFFINITY_CPUS}
v3_tsc_timing_enable=${V3_TSC_TIMING_ENABLE}
v3_tsc_mismatch_threshold_pct=${V3_TSC_MISMATCH_THRESHOLD_PCT}
v3_dedicated_worker_runtime=${V3_DEDICATED_WORKER_RUNTIME}
v3_hotpath_histogram_sample_rate=${V3_HOTPATH_HISTOGRAM_SAMPLE_RATE}
v3_gateway_taskset_cpus=${V3_GATEWAY_TASKSET_CPUS}
load_workers=${LOAD_WORKERS}
load_accounts=${LOAD_ACCOUNTS}
load_queue_capacity=${LOAD_QUEUE_CAPACITY}
load_sticky_account_per_worker=${V3_TCP_STICKY_ACCOUNT_PER_WORKER}
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
v3_durable_control_preset=${V3_DURABLE_CONTROL_PRESET}
v3_durable_worker_dynamic_inflight=${V3_DURABLE_WORKER_DYNAMIC_INFLIGHT}
v3_durable_worker_dynamic_inflight_min_cap_pct=${V3_DURABLE_WORKER_DYNAMIC_INFLIGHT_MIN_CAP_PCT}
v3_durable_worker_dynamic_inflight_max_cap_pct=${V3_DURABLE_WORKER_DYNAMIC_INFLIGHT_MAX_CAP_PCT}
v3_durable_worker_dynamic_inflight_strict_max_cap_pct=${V3_DURABLE_WORKER_DYNAMIC_INFLIGHT_STRICT_MAX_CAP_PCT}
v3_durable_pressure_ewma_alpha_pct=${V3_DURABLE_PRESSURE_EWMA_ALPHA_PCT}
v3_durable_dynamic_cap_slew_step_pct=${V3_DURABLE_DYNAMIC_CAP_SLEW_STEP_PCT}
v3_durable_worker_batch_adaptive=${V3_DURABLE_WORKER_BATCH_ADAPTIVE}
v3_durable_worker_batch_adaptive_low_util_pct=${V3_DURABLE_WORKER_BATCH_ADAPTIVE_LOW_UTIL_PCT}
v3_durable_worker_batch_adaptive_high_util_pct=${V3_DURABLE_WORKER_BATCH_ADAPTIVE_HIGH_UTIL_PCT}
v3_durable_age_soft_inflight_cap_pct=${V3_DURABLE_AGE_SOFT_INFLIGHT_CAP_PCT}
v3_durable_age_hard_inflight_cap_pct=${V3_DURABLE_AGE_HARD_INFLIGHT_CAP_PCT}
v3_durable_slo_stage=${V3_DURABLE_SLO_STAGE}
v3_durable_slo_early_soft_age_us=${V3_DURABLE_SLO_EARLY_SOFT_AGE_US}
v3_durable_slo_early_hard_age_us=${V3_DURABLE_SLO_EARLY_HARD_AGE_US}
v3_durable_admission_controller_enabled=${V3_DURABLE_ADMISSION_CONTROLLER_ENABLED}
v3_durable_admission_sustain_ticks=${V3_DURABLE_ADMISSION_SUSTAIN_TICKS}
v3_durable_admission_recover_ticks=${V3_DURABLE_ADMISSION_RECOVER_TICKS}
v3_durable_admission_soft_fsync_p99_us=${V3_DURABLE_ADMISSION_SOFT_FSYNC_P99_US}
v3_durable_admission_hard_fsync_p99_us=${V3_DURABLE_ADMISSION_HARD_FSYNC_P99_US}
v3_durable_admission_fsync_presignal_pct=${V3_DURABLE_ADMISSION_FSYNC_PRESIGNAL_PCT}
v3_durable_admission_fsync_only_soft_sustain_ticks=${V3_DURABLE_ADMISSION_FSYNC_ONLY_SOFT_SUSTAIN_TICKS}
v3_durable_admission_fsync_only_hard_sustain_ticks=${V3_DURABLE_ADMISSION_FSYNC_ONLY_HARD_SUSTAIN_TICKS}
v3_durable_soft_reject_pct=${V3_DURABLE_SOFT_REJECT_PCT}
v3_durable_hard_reject_pct=${V3_DURABLE_HARD_REJECT_PCT}
v3_durable_backlog_signal_min_queue_pct=${V3_DURABLE_BACKLOG_SIGNAL_MIN_QUEUE_PCT}
v3_durable_confirm_soft_reject_age_us=${V3_DURABLE_CONFIRM_SOFT_REJECT_AGE_US}
v3_durable_confirm_hard_reject_age_us=${V3_DURABLE_CONFIRM_HARD_REJECT_AGE_US}
v3_durable_confirm_guard_soft_slack_pct=${V3_DURABLE_CONFIRM_GUARD_SOFT_SLACK_PCT}
v3_durable_confirm_guard_hard_slack_pct=${V3_DURABLE_CONFIRM_GUARD_HARD_SLACK_PCT}
v3_durable_confirm_guard_soft_requires_admission=${V3_DURABLE_CONFIRM_GUARD_SOFT_REQUIRES_ADMISSION}
v3_durable_confirm_guard_hard_requires_admission=${V3_DURABLE_CONFIRM_GUARD_HARD_REQUIRES_ADMISSION}
v3_durable_confirm_guard_secondary_required=${V3_DURABLE_CONFIRM_GUARD_SECONDARY_REQUIRED}
v3_durable_confirm_guard_min_queue_pct=${V3_DURABLE_CONFIRM_GUARD_MIN_QUEUE_PCT}
v3_durable_confirm_guard_min_inflight_pct=${V3_DURABLE_CONFIRM_GUARD_MIN_INFLIGHT_PCT}
v3_durable_confirm_guard_min_backlog_per_sec=${V3_DURABLE_CONFIRM_GUARD_MIN_BACKLOG_PER_SEC}
v3_durable_confirm_guard_soft_sustain_ticks=${V3_DURABLE_CONFIRM_GUARD_SOFT_SUSTAIN_TICKS}
v3_durable_confirm_guard_hard_sustain_ticks=${V3_DURABLE_CONFIRM_GUARD_HARD_SUSTAIN_TICKS}
v3_durable_confirm_guard_recover_ticks=${V3_DURABLE_CONFIRM_GUARD_RECOVER_TICKS}
v3_durable_confirm_guard_recover_hysteresis_pct=${V3_DURABLE_CONFIRM_GUARD_RECOVER_HYSTERESIS_PCT}
v3_durable_confirm_guard_autotune=${V3_DURABLE_CONFIRM_GUARD_AUTOTUNE}
v3_durable_confirm_guard_autotune_low_pressure_pct=${V3_DURABLE_CONFIRM_GUARD_AUTOTUNE_LOW_PRESSURE_PCT}
v3_durable_confirm_guard_autotune_high_pressure_pct=${V3_DURABLE_CONFIRM_GUARD_AUTOTUNE_HIGH_PRESSURE_PCT}
v3_durable_ack_path_guard_enabled=${V3_DURABLE_ACK_PATH_GUARD_ENABLED}
audit_fdatasync_max_wait_us=${AUDIT_FDATASYNC_MAX_WAIT_US}
audit_fdatasync_max_batch=${AUDIT_FDATASYNC_MAX_BATCH}
audit_fdatasync_max_defer_us=${AUDIT_FDATASYNC_MAX_DEFER_US}
audit_fdatasync_max_inflight_age_us=${AUDIT_FDATASYNC_MAX_INFLIGHT_AGE_US}
audit_fdatasync_max_pending_bytes=${AUDIT_FDATASYNC_MAX_PENDING_BYTES}
audit_fdatasync_serialize=${AUDIT_FDATASYNC_SERIALIZE}
audit_wal_prealloc_bytes=${AUDIT_WAL_PREALLOC_BYTES}
audit_wal_prealloc_threshold_bytes=${AUDIT_WAL_PREALLOC_THRESHOLD_BYTES}
offered_total=${offered_total}
offered_rps=${offered_rps}
offered_rps_ratio=${offered_rps_ratio}
completed_rps=${completed_rps}
client_accepted_rate=${client_accepted_rate}
client_ack_p99_us=${client_ack_p99_us}
client_ack_p999_us=${client_ack_p999_us}
client_ack_accepted_p99_us=${client_ack_accepted_p99_us}
client_ack_accepted_p999_us=${client_ack_accepted_p999_us}
client_e2e_p99_us=${client_e2e_p99_us}
client_e2e_p999_us=${client_e2e_p999_us}
client_e2e_accepted_p99_us=${client_e2e_accepted_p99_us}
client_e2e_accepted_p999_us=${client_e2e_accepted_p999_us}
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
server_live_ack_accepted_p99_ns=${server_live_ack_accepted_p99_ns}
server_hotpath_accepted_p99_ns=${server_hotpath_accepted_p99_ns}
server_hotpath_accepted_tsc_p99_ns=${server_hotpath_accepted_tsc_p99_ns}
server_tsc_timing_enabled=${server_tsc_timing_enabled}
server_tsc_fallback_total=${server_tsc_fallback_total}
server_tsc_cross_core_total=${server_tsc_cross_core_total}
server_tsc_mismatch_total=${server_tsc_mismatch_total}
server_thread_affinity_apply_success_total=${server_thread_affinity_apply_success_total}
server_thread_affinity_apply_failure_total=${server_thread_affinity_apply_failure_total}
server_live_ack_accepted_p999_us=${server_live_ack_accepted_p999_us}
server_durable_ack_path_guard_enabled=${server_durable_ack_path_guard_enabled}
server_accepted_rate=${server_accepted_rate}
server_durable_confirm_p99_us=${server_durable_confirm_p99_us}
server_durable_confirm_p999_us=${server_durable_confirm_p999_us}
server_durable_queue_depth=${server_durable_queue_depth}
server_durable_queue_capacity=${server_durable_queue_capacity}
server_durable_queue_utilization_pct=${server_durable_queue_utilization_pct}
server_durable_queue_utilization_pct_max=${server_durable_queue_utilization_pct_max}
server_durable_backlog_growth_per_sec=${server_durable_backlog_growth_per_sec}
server_durable_queue_full_total=${server_durable_queue_full_total}
server_durable_queue_closed_total=${server_durable_queue_closed_total}
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
gate_pass_live_ack_accepted_p99_ns=${pass_ack_accepted_ns}
gate_pass_live_ack_accepted_tsc_p99_ns=${pass_ack_accepted_tsc_ns}
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
timeseries_out=${TIMESERIES_OUT}
gateway_log=${LOG_FILE}
EOF

echo "[summary] offered_rps=${offered_rps} completed_rps=${completed_rps} client_accepted_rate=${client_accepted_rate} server_accepted_rate=${server_accepted_rate}"
echo "[summary] client_e2e_accepted_p99_us=${client_e2e_accepted_p99_us} server_live_ack_p99_us=${server_live_ack_p99_us} server_live_ack_accepted_p99_us=${server_live_ack_accepted_p99_us} server_hotpath_accepted_p99_ns=${server_hotpath_accepted_p99_ns}"
echo "[summary_durable] queue_util_max=${server_durable_queue_utilization_pct_max} backlog_growth_per_sec=${server_durable_backlog_growth_per_sec} backpressure_soft_total=${server_durable_backpressure_soft_total} backpressure_hard_total=${server_durable_backpressure_hard_total}"
echo "[gate] completed_rps>=${completed_rps_floor} (target=${TARGET_COMPLETED_RPS}, eps=${TARGET_COMPLETED_RPS_EPSILON}):${pass_completed_rps} live_ack_p99<=${TARGET_ACK_P99_US}:${pass_ack} live_ack_accepted_p99<=${TARGET_ACK_ACCEPTED_P99_US}:${pass_ack_accepted} hotpath_accepted_p99_ns<=${TARGET_ACK_ACCEPTED_P99_NS}:${pass_ack_accepted_ns} hotpath_accepted_tsc_p99_ns<=${TARGET_ACK_ACCEPTED_TSC_P99_NS}:${pass_ack_accepted_tsc_ns} accepted_rate>=${TARGET_ACCEPTED_RATE}:${pass_rate} durable_confirm_p99<=${TARGET_DURABLE_CONFIRM_P99_US}:${pass_durable_confirm} rejected_killed<=${TARGET_REJECTED_KILLED_MAX}:${pass_killed} loss_suspect<=${TARGET_LOSS_SUSPECT_MAX}:${pass_loss} lane_topology:${pass_lane_topology} lane_checks:${pass_lane_checks}"
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
  # Auto-triage: run AI incident agent on gate failure.
  RUN_NAME="v3_open_loop_${STAMP}"
  TRIAGE_OUT="$OUT_DIR/${RUN_NAME}.triage.json"
  AI_TRIAGE_PROVIDER="${AI_TRIAGE_PROVIDER:-mock}"
  AI_LLM_NETWORK_ENABLED="${AI_LLM_NETWORK_ENABLED:-0}"
  if [[ "$AI_LLM_NETWORK_ENABLED" != "1" && "$AI_TRIAGE_PROVIDER" != "mock" ]]; then
    echo "[triage] paid LLM disabled (AI_LLM_NETWORK_ENABLED=${AI_LLM_NETWORK_ENABLED}); forcing provider=mock"
    AI_TRIAGE_PROVIDER="mock"
  fi
  if command -v python3 >/dev/null 2>&1; then
    echo "[triage] running AI triage agent (provider=${AI_TRIAGE_PROVIDER})..."
    python3 "$ROOT_DIR/scripts/ops/ai_incident_agent.py" \
      --run-name "$RUN_NAME" \
      --results-dir "$OUT_DIR" \
      --provider "$AI_TRIAGE_PROVIDER" \
      --out "$TRIAGE_OUT" 2>&1 || echo "[triage] agent failed (non-fatal)"
    if [[ -f "$TRIAGE_OUT" ]]; then
      echo "[artifacts] triage=${TRIAGE_OUT}"
      # Post triage to Grafana as annotation.
      if [[ "${AI_TRIAGE_GRAFANA:-1}" == "1" ]]; then
        python3 "$ROOT_DIR/scripts/ops/ai_triage_notify.py" \
          --triage-json "$TRIAGE_OUT" 2>&1 || echo "[grafana] annotation failed (non-fatal)"
      fi
    fi
  else
    echo "[triage] skipped: python3 not found"
  fi
  exit 1
fi
