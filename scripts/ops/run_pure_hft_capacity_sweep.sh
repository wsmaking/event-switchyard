#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/var/results}"
STAMP="$(date +%Y%m%d_%H%M%S)"

DURATION="${DURATION:-8}"
RUNS="${RUNS:-3}"
WARMUP_RUNS="${WARMUP_RUNS:-1}"
WRK_ACCOUNT_COUNT="${WRK_ACCOUNT_COUNT:-4}"
BASE_MARGIN_MODE="${BASE_MARGIN_MODE:-legacy}"
INJ_MARGIN_MODE="${INJ_MARGIN_MODE:-legacy}"
LEVELS="${LEVELS:-10k:40:4,20k:80:4,40k:160:8,80k:320:8}"
CAPACITY_PRESET="${CAPACITY_PRESET:-raised}"

WAIT_MAX_SEC="${WAIT_MAX_SEC:-120}"
POLL_SEC="${POLL_SEC:-2}"
STABLE_POLLS="${STABLE_POLLS:-5}"
MDS_CPU_MAX="${MDS_CPU_MAX:-20}"
CHROME_CPU_MAX="${CHROME_CPU_MAX:-120}"
RUN_ON_TIMEOUT="${RUN_ON_TIMEOUT:-1}"

case "${CAPACITY_PRESET}" in
  default)
    preset_soft=70
    preset_hard=85
    preset_kill=95
    preset_queue=65536
    preset_durable_queue=200000
    preset_loss_gap_ms=500
    preset_session_loss_threshold=1
    preset_shard_loss_threshold=3
    preset_global_loss_threshold=6
    ;;
  raised|tuned)
    preset_soft=85
    preset_hard=92
    preset_kill=98
    preset_queue=131072
    preset_durable_queue=200000
    preset_loss_gap_ms=2000
    preset_session_loss_threshold=8
    preset_shard_loss_threshold=64
    preset_global_loss_threshold=256
    ;;
  *)
    echo "FAIL: CAPACITY_PRESET must be one of: default, raised, tuned (current: ${CAPACITY_PRESET})"
    exit 1
    ;;
esac

V3_SOFT_REJECT_PCT="${V3_SOFT_REJECT_PCT:-$preset_soft}"
V3_HARD_REJECT_PCT="${V3_HARD_REJECT_PCT:-$preset_hard}"
V3_KILL_REJECT_PCT="${V3_KILL_REJECT_PCT:-$preset_kill}"
V3_INGRESS_QUEUE_CAPACITY="${V3_INGRESS_QUEUE_CAPACITY:-$preset_queue}"
V3_DURABILITY_QUEUE_CAPACITY="${V3_DURABILITY_QUEUE_CAPACITY:-$preset_durable_queue}"
V3_LOSS_GAP_TIMEOUT_MS="${V3_LOSS_GAP_TIMEOUT_MS:-$preset_loss_gap_ms}"
V3_SESSION_LOSS_SUSPECT_THRESHOLD="${V3_SESSION_LOSS_SUSPECT_THRESHOLD:-$preset_session_loss_threshold}"
V3_SHARD_LOSS_SUSPECT_THRESHOLD="${V3_SHARD_LOSS_SUSPECT_THRESHOLD:-$preset_shard_loss_threshold}"
V3_GLOBAL_LOSS_SUSPECT_THRESHOLD="${V3_GLOBAL_LOSS_SUSPECT_THRESHOLD:-$preset_global_loss_threshold}"
V3_KILL_AUTO_RECOVER="${V3_KILL_AUTO_RECOVER:-false}"
V3_SHARD_COUNT="${V3_SHARD_COUNT:-4}"
FASTPATH_DRAIN_WORKERS="${FASTPATH_DRAIN_WORKERS:-4}"

mkdir -p "$OUT_DIR"
cd "$ROOT_DIR"

OUT_TSV="$OUT_DIR/pure_hft_capacity_sweep_${STAMP}.tsv"
OUT_SUMMARY="$OUT_DIR/pure_hft_capacity_sweep_${STAMP}.summary.txt"
echo -e "level\tconnections\tthreads\trc\tcase\tack_p99_us\tack_accepted_p99_us\trps\taccept_ratio\taccepted_rps\tsummary_path" >"$OUT_TSV"

extract_metric() {
  local line="$1"
  local key="$2"
  printf "%s\n" "$line" | tr '\t' '\n' | awk -F= -v k="$key" '$1==k{print $2}'
}

for spec in ${LEVELS//,/ }; do
  IFS=':' read -r level conn thr <<<"$spec"
  if [[ -z "${level}" || -z "${conn}" || -z "${thr}" ]]; then
    echo "FAIL: LEVELS entry must be label:connections:threads (current: $spec)"
    exit 1
  fi

  run_out_dir="$OUT_DIR/pure_hft_capacity_${STAMP}_${level}"
  run_log="$run_out_dir/run.log"
  mkdir -p "$run_out_dir"
  echo "[run] level=${level} connections=${conn} threads=${thr} out=${run_out_dir}"

  set +e
  WAIT_MAX_SEC="$WAIT_MAX_SEC" \
  POLL_SEC="$POLL_SEC" \
  STABLE_POLLS="$STABLE_POLLS" \
  MDS_CPU_MAX="$MDS_CPU_MAX" \
  CHROME_CPU_MAX="$CHROME_CPU_MAX" \
  RUN_ON_TIMEOUT="$RUN_ON_TIMEOUT" \
  BUILD_RELEASE=0 \
  RUNS="$RUNS" \
  WARMUP_RUNS="$WARMUP_RUNS" \
  DURATION="$DURATION" \
  CONNECTIONS="$conn" \
  THREADS="$thr" \
  WRK_ACCOUNT_COUNT="$WRK_ACCOUNT_COUNT" \
  BASE_MARGIN_MODE="$BASE_MARGIN_MODE" \
  INJ_MARGIN_MODE="$INJ_MARGIN_MODE" \
  V3_SOFT_REJECT_PCT="$V3_SOFT_REJECT_PCT" \
  V3_HARD_REJECT_PCT="$V3_HARD_REJECT_PCT" \
  V3_KILL_REJECT_PCT="$V3_KILL_REJECT_PCT" \
  V3_INGRESS_QUEUE_CAPACITY="$V3_INGRESS_QUEUE_CAPACITY" \
  V3_DURABILITY_QUEUE_CAPACITY="$V3_DURABILITY_QUEUE_CAPACITY" \
  V3_LOSS_GAP_TIMEOUT_MS="$V3_LOSS_GAP_TIMEOUT_MS" \
  V3_SESSION_LOSS_SUSPECT_THRESHOLD="$V3_SESSION_LOSS_SUSPECT_THRESHOLD" \
  V3_SHARD_LOSS_SUSPECT_THRESHOLD="$V3_SHARD_LOSS_SUSPECT_THRESHOLD" \
  V3_GLOBAL_LOSS_SUSPECT_THRESHOLD="$V3_GLOBAL_LOSS_SUSPECT_THRESHOLD" \
  V3_KILL_AUTO_RECOVER="$V3_KILL_AUTO_RECOVER" \
  V3_SHARD_COUNT="$V3_SHARD_COUNT" \
  FASTPATH_DRAIN_WORKERS="$FASTPATH_DRAIN_WORKERS" \
  OUT_DIR="$run_out_dir" \
  scripts/ops/run_pure_hft_phase2_compare_when_quiet.sh >"$run_log" 2>&1
  rc=$?
  set -e

  summary_path="$(ls -1t "$run_out_dir"/pure_hft_phase2_compare_*.summary.txt 2>/dev/null | head -n1 || true)"
  if [[ -z "$summary_path" ]]; then
    echo -e "${level}\t${conn}\t${thr}\t${rc}\tbaseline\tNA\tNA\tNA\tNA\tNA\t-" >>"$OUT_TSV"
    echo -e "${level}\t${conn}\t${thr}\t${rc}\tinjected\tNA\tNA\tNA\tNA\tNA\t-" >>"$OUT_TSV"
    continue
  fi

  baseline_line="$(awk -F'\t' '/^SUMMARY\tbaseline_/ {print; exit}' "$summary_path")"
  injected_line="$(awk -F'\t' '/^SUMMARY\tinjected_/ {print; exit}' "$summary_path")"

  if [[ -n "$baseline_line" ]]; then
    echo -e "${level}\t${conn}\t${thr}\t${rc}\tbaseline\t$(extract_metric "$baseline_line" "ack_p99_med")\t$(extract_metric "$baseline_line" "ack_accepted_p99_med")\t$(extract_metric "$baseline_line" "rps_med")\t$(extract_metric "$baseline_line" "accept_ratio_med")\t$(extract_metric "$baseline_line" "accepted_rps_med")\t${summary_path}" >>"$OUT_TSV"
  else
    echo -e "${level}\t${conn}\t${thr}\t${rc}\tbaseline\tNA\tNA\tNA\tNA\tNA\t${summary_path}" >>"$OUT_TSV"
  fi

  if [[ -n "$injected_line" ]]; then
    echo -e "${level}\t${conn}\t${thr}\t${rc}\tinjected\t$(extract_metric "$injected_line" "ack_p99_med")\t$(extract_metric "$injected_line" "ack_accepted_p99_med")\t$(extract_metric "$injected_line" "rps_med")\t$(extract_metric "$injected_line" "accept_ratio_med")\t$(extract_metric "$injected_line" "accepted_rps_med")\t${summary_path}" >>"$OUT_TSV"
  else
    echo -e "${level}\t${conn}\t${thr}\t${rc}\tinjected\tNA\tNA\tNA\tNA\tNA\t${summary_path}" >>"$OUT_TSV"
  fi
done

cat >"$OUT_SUMMARY" <<EOF
pure_hft_capacity_sweep
date=${STAMP}
capacity_preset=${CAPACITY_PRESET}
v3_soft_reject_pct=${V3_SOFT_REJECT_PCT}
v3_hard_reject_pct=${V3_HARD_REJECT_PCT}
v3_kill_reject_pct=${V3_KILL_REJECT_PCT}
v3_ingress_queue_capacity=${V3_INGRESS_QUEUE_CAPACITY}
v3_durability_queue_capacity=${V3_DURABILITY_QUEUE_CAPACITY}
v3_loss_gap_timeout_ms=${V3_LOSS_GAP_TIMEOUT_MS}
v3_session_loss_suspect_threshold=${V3_SESSION_LOSS_SUSPECT_THRESHOLD}
v3_shard_loss_suspect_threshold=${V3_SHARD_LOSS_SUSPECT_THRESHOLD}
v3_global_loss_suspect_threshold=${V3_GLOBAL_LOSS_SUSPECT_THRESHOLD}
wrk_account_count=${WRK_ACCOUNT_COUNT}
duration_sec=${DURATION}
runs=${RUNS}
warmup_runs=${WARMUP_RUNS}
levels=${LEVELS}
tsv=${OUT_TSV}
EOF

echo "[artifacts] tsv=${OUT_TSV}"
echo "[artifacts] summary=${OUT_SUMMARY}"
cat "$OUT_SUMMARY"
