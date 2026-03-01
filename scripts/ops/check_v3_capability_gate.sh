#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/var/results}"

# Capability gate: upper-band throughput line with quality constraints.
DURATION="${DURATION:-120}"
TARGET_RPS="${TARGET_RPS:-45000}"
TARGET_COMPLETED_RPS="${TARGET_COMPLETED_RPS:-$TARGET_RPS}"
TARGET_ACK_P99_US="${TARGET_ACK_P99_US:-100}"
TARGET_ACK_ACCEPTED_P99_US="${TARGET_ACK_ACCEPTED_P99_US:-40}"
TARGET_ACCEPTED_RATE="${TARGET_ACCEPTED_RATE:-0.95}"
TARGET_REJECTED_KILLED_MAX="${TARGET_REJECTED_KILLED_MAX:-0}"
TARGET_LOSS_SUSPECT_MAX="${TARGET_LOSS_SUSPECT_MAX:-0}"
TARGET_OFFERED_RPS_RATIO_MIN="${TARGET_OFFERED_RPS_RATIO_MIN:-0.99}"
TARGET_DROPPED_OFFER_RATIO_MAX="${TARGET_DROPPED_OFFER_RATIO_MAX:-0.001}"
TARGET_UNSENT_TOTAL_MAX="${TARGET_UNSENT_TOTAL_MAX:-0}"
TARGET_STRICT_LANE_TOPOLOGY="${TARGET_STRICT_LANE_TOPOLOGY:-1}"
V3_INGRESS_TRANSPORT="${V3_INGRESS_TRANSPORT:-tcp}"
V3_TCP_PORT="${V3_TCP_PORT:-39001}"

LOAD_WORKERS="${LOAD_WORKERS:-512}"
LOAD_ACCOUNTS="${LOAD_ACCOUNTS:-64}"
LOAD_QUEUE_CAPACITY="${LOAD_QUEUE_CAPACITY:-1500000}"
REQUEST_TIMEOUT_SEC="${REQUEST_TIMEOUT_SEC:-2}"
DRAIN_TIMEOUT_SEC="${DRAIN_TIMEOUT_SEC:-30}"
BUILD_RELEASE="${BUILD_RELEASE:-0}"

# Recommended local profile.
V3_SOFT_REJECT_PCT="${V3_SOFT_REJECT_PCT:-85}"
V3_HARD_REJECT_PCT="${V3_HARD_REJECT_PCT:-92}"
V3_KILL_REJECT_PCT="${V3_KILL_REJECT_PCT:-98}"
V3_KILL_AUTO_RECOVER="${V3_KILL_AUTO_RECOVER:-false}"
V3_INGRESS_QUEUE_CAPACITY="${V3_INGRESS_QUEUE_CAPACITY:-131072}"
V3_DURABILITY_QUEUE_CAPACITY="${V3_DURABILITY_QUEUE_CAPACITY:-4000000}"
V3_SHARD_COUNT="${V3_SHARD_COUNT:-8}"
V3_LOSS_GAP_TIMEOUT_MS="${V3_LOSS_GAP_TIMEOUT_MS:-360000}"
V3_SESSION_LOSS_SUSPECT_THRESHOLD="${V3_SESSION_LOSS_SUSPECT_THRESHOLD:-4096}"
V3_SHARD_LOSS_SUSPECT_THRESHOLD="${V3_SHARD_LOSS_SUSPECT_THRESHOLD:-32768}"
V3_GLOBAL_LOSS_SUSPECT_THRESHOLD="${V3_GLOBAL_LOSS_SUSPECT_THRESHOLD:-131072}"
V3_DURABLE_WORKER_BATCH_MAX="${V3_DURABLE_WORKER_BATCH_MAX:-12}"
V3_DURABLE_WORKER_BATCH_MIN="${V3_DURABLE_WORKER_BATCH_MIN:-6}"
V3_DURABLE_WORKER_BATCH_WAIT_US="${V3_DURABLE_WORKER_BATCH_WAIT_US:-80}"
V3_DURABLE_WORKER_BATCH_WAIT_MIN_US="${V3_DURABLE_WORKER_BATCH_WAIT_MIN_US:-40}"
V3_DURABLE_WORKER_BATCH_ADAPTIVE="${V3_DURABLE_WORKER_BATCH_ADAPTIVE:-false}"
V3_DURABLE_WORKER_BATCH_ADAPTIVE_LOW_UTIL_PCT="${V3_DURABLE_WORKER_BATCH_ADAPTIVE_LOW_UTIL_PCT:-10}"
V3_DURABLE_WORKER_BATCH_ADAPTIVE_HIGH_UTIL_PCT="${V3_DURABLE_WORKER_BATCH_ADAPTIVE_HIGH_UTIL_PCT:-60}"
V3_CONFIRM_REBUILD_ON_START="${V3_CONFIRM_REBUILD_ON_START:-false}"
V3_CONFIRM_REBUILD_MAX_LINES="${V3_CONFIRM_REBUILD_MAX_LINES:-200000}"
FASTPATH_DRAIN_WORKERS="${FASTPATH_DRAIN_WORKERS:-4}"

STAMP="$(date +%Y%m%d_%H%M%S)"
SUMMARY_OUT="$OUT_DIR/v3_capability_gate_${STAMP}.summary.txt"
TMP_OUT="/tmp/v3_capability_gate_${STAMP}.out"

mkdir -p "$OUT_DIR"
cd "$ROOT_DIR"

echo "[run] open-loop probe for capability gate"
HOST="${HOST:-127.0.0.1}" \
PORT="${PORT:-29001}" \
DURATION="$DURATION" \
TARGET_RPS="$TARGET_RPS" \
TARGET_COMPLETED_RPS="$TARGET_COMPLETED_RPS" \
TARGET_ACK_P99_US="$TARGET_ACK_P99_US" \
TARGET_ACK_ACCEPTED_P99_US="$TARGET_ACK_ACCEPTED_P99_US" \
TARGET_ACCEPTED_RATE="$TARGET_ACCEPTED_RATE" \
TARGET_REJECTED_KILLED_MAX="$TARGET_REJECTED_KILLED_MAX" \
TARGET_LOSS_SUSPECT_MAX="$TARGET_LOSS_SUSPECT_MAX" \
TARGET_OFFERED_RPS_RATIO_MIN="$TARGET_OFFERED_RPS_RATIO_MIN" \
TARGET_DROPPED_OFFER_RATIO_MAX="$TARGET_DROPPED_OFFER_RATIO_MAX" \
TARGET_UNSENT_TOTAL_MAX="$TARGET_UNSENT_TOTAL_MAX" \
TARGET_STRICT_LANE_TOPOLOGY="$TARGET_STRICT_LANE_TOPOLOGY" \
V3_INGRESS_TRANSPORT="$V3_INGRESS_TRANSPORT" \
V3_TCP_PORT="$V3_TCP_PORT" \
LOAD_WORKERS="$LOAD_WORKERS" \
LOAD_ACCOUNTS="$LOAD_ACCOUNTS" \
LOAD_QUEUE_CAPACITY="$LOAD_QUEUE_CAPACITY" \
REQUEST_TIMEOUT_SEC="$REQUEST_TIMEOUT_SEC" \
DRAIN_TIMEOUT_SEC="$DRAIN_TIMEOUT_SEC" \
BUILD_RELEASE="$BUILD_RELEASE" \
OUT_DIR="$OUT_DIR" \
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
V3_DURABLE_WORKER_BATCH_ADAPTIVE="$V3_DURABLE_WORKER_BATCH_ADAPTIVE" \
V3_DURABLE_WORKER_BATCH_ADAPTIVE_LOW_UTIL_PCT="$V3_DURABLE_WORKER_BATCH_ADAPTIVE_LOW_UTIL_PCT" \
V3_DURABLE_WORKER_BATCH_ADAPTIVE_HIGH_UTIL_PCT="$V3_DURABLE_WORKER_BATCH_ADAPTIVE_HIGH_UTIL_PCT" \
V3_CONFIRM_REBUILD_ON_START="$V3_CONFIRM_REBUILD_ON_START" \
V3_CONFIRM_REBUILD_MAX_LINES="$V3_CONFIRM_REBUILD_MAX_LINES" \
FASTPATH_DRAIN_WORKERS="$FASTPATH_DRAIN_WORKERS" \
ENFORCE_GATE=0 \
scripts/ops/run_v3_open_loop_probe.sh | tee "$TMP_OUT"

openloop_summary="$(awk -F= '/^\[artifacts\] summary=/{print $2}' "$TMP_OUT" | tail -n1)"
if [[ -z "$openloop_summary" || ! -f "$openloop_summary" ]]; then
  echo "FAIL: open-loop summary not found"
  exit 1
fi

read_value() {
  local key="$1"
  awk -F= -v k="$key" '$1==k {print $2}' "$openloop_summary" | tail -n1
}

completed_rps="$(read_value completed_rps)"
accepted_rate="$(read_value server_accepted_rate)"
ack_accepted_p99_us="$(read_value server_live_ack_accepted_p99_us)"
ack_p99_us="$(read_value server_live_ack_p99_us)"
offered_rps_ratio="$(read_value offered_rps_ratio)"
dropped_offer_ratio="$(read_value client_dropped_offer_ratio)"
unsent_total="$(read_value client_unsent_total)"
rejected_killed_total="$(read_value server_rejected_killed_total)"
loss_suspect_total="$(read_value server_loss_suspect_total)"

pass_rps="$(read_value gate_pass_completed_rps)"
pass_rate="$(read_value gate_pass_accepted_rate)"
pass_ack_accepted="$(read_value gate_pass_live_ack_accepted_p99)"
pass_ack="$(read_value gate_pass_live_ack_p99)"
pass_killed="$(read_value gate_pass_rejected_killed)"
pass_loss="$(read_value gate_pass_loss_suspect)"
pass_offered="$(read_value gate_pass_offered_rps_ratio)"
pass_drop="$(read_value gate_pass_dropped_offer_ratio)"
pass_unsent="$(read_value gate_pass_unsent_total)"
pass_all="$(read_value gate_pass)"

cat >"$SUMMARY_OUT" <<EOF
v3_capability_gate
date=${STAMP}
target_rps=${TARGET_RPS}
target_completed_rps=${TARGET_COMPLETED_RPS}
target_ack_p99_us=${TARGET_ACK_P99_US}
target_ack_accepted_p99_us=${TARGET_ACK_ACCEPTED_P99_US}
target_accepted_rate=${TARGET_ACCEPTED_RATE}
target_rejected_killed_max=${TARGET_REJECTED_KILLED_MAX}
target_loss_suspect_max=${TARGET_LOSS_SUSPECT_MAX}
target_offered_rps_ratio_min=${TARGET_OFFERED_RPS_RATIO_MIN}
target_dropped_offer_ratio_max=${TARGET_DROPPED_OFFER_RATIO_MAX}
target_unsent_total_max=${TARGET_UNSENT_TOTAL_MAX}
duration_sec=${DURATION}
load_workers=${LOAD_WORKERS}
load_accounts=${LOAD_ACCOUNTS}
load_queue_capacity=${LOAD_QUEUE_CAPACITY}
observed_completed_rps=${completed_rps}
observed_accepted_rate=${accepted_rate}
observed_ack_accepted_p99_us=${ack_accepted_p99_us}
observed_ack_p99_us=${ack_p99_us}
observed_offered_rps_ratio=${offered_rps_ratio}
observed_dropped_offer_ratio=${dropped_offer_ratio}
observed_unsent_total=${unsent_total}
observed_rejected_killed_total=${rejected_killed_total}
observed_loss_suspect_total=${loss_suspect_total}
gate_pass_rps=${pass_rps}
gate_pass_accepted_rate=${pass_rate}
gate_pass_ack_accepted_p99=${pass_ack_accepted}
gate_pass_ack_p99=${pass_ack}
gate_pass_rejected_killed=${pass_killed}
gate_pass_loss_suspect=${pass_loss}
gate_pass_offered_rps_ratio=${pass_offered}
gate_pass_dropped_offer_ratio=${pass_drop}
gate_pass_unsent_total=${pass_unsent}
gate_pass=${pass_all}
openloop_summary=${openloop_summary}
EOF

echo "[summary] completed_rps=${completed_rps} accepted_rate=${accepted_rate} ack_accepted_p99_us=${ack_accepted_p99_us} ack_p99_us=${ack_p99_us}"
echo "[summary] offered_ratio=${offered_rps_ratio} dropped_offer_ratio=${dropped_offer_ratio} unsent_total=${unsent_total}"
echo "[artifacts] ${SUMMARY_OUT}"

if [[ "$pass_all" == "1" ]]; then
  echo "PASS: capability gate satisfied"
  exit 0
fi

echo "FAIL: capability gate not satisfied"
echo "  - completed_rps >= ${TARGET_COMPLETED_RPS}: ${pass_rps}"
echo "  - accepted_rate >= ${TARGET_ACCEPTED_RATE}: ${pass_rate}"
echo "  - ack_accepted_p99_us <= ${TARGET_ACK_ACCEPTED_P99_US}: ${pass_ack_accepted}"
echo "  - ack_p99_us <= ${TARGET_ACK_P99_US}: ${pass_ack}"
echo "  - rejected_killed_total <= ${TARGET_REJECTED_KILLED_MAX}: ${pass_killed}"
echo "  - loss_suspect_total <= ${TARGET_LOSS_SUSPECT_MAX}: ${pass_loss}"
echo "  - offered_rps_ratio >= ${TARGET_OFFERED_RPS_RATIO_MIN}: ${pass_offered}"
echo "  - dropped_offer_ratio <= ${TARGET_DROPPED_OFFER_RATIO_MAX}: ${pass_drop}"
echo "  - unsent_total <= ${TARGET_UNSENT_TOTAL_MAX}: ${pass_unsent}"
exit 1
