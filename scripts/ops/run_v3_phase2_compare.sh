#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
PORT="${PORT:-29001}"
HOST="${HOST:-127.0.0.1}"
RUNS="${RUNS:-5}"
WARMUP_RUNS="${WARMUP_RUNS:-1}"
DURATION="${DURATION:-8}"
CONNECTIONS="${CONNECTIONS:-300}"
THREADS="${THREADS:-8}"
WRK_ACCOUNT_COUNT="${WRK_ACCOUNT_COUNT:-1}"
WRK_ACCOUNT_PREFIX="${WRK_ACCOUNT_PREFIX:-benchacct}"
BUILD_RELEASE="${BUILD_RELEASE:-1}"
NOISE_GUARD_STRICT="${NOISE_GUARD_STRICT:-0}"
NOISE_CPU_THRESHOLD="${NOISE_CPU_THRESHOLD:-35}"
V3_SOFT_REJECT_PCT="${V3_SOFT_REJECT_PCT:-70}"
V3_HARD_REJECT_PCT="${V3_HARD_REJECT_PCT:-85}"
V3_KILL_REJECT_PCT="${V3_KILL_REJECT_PCT:-95}"
V3_KILL_AUTO_RECOVER="${V3_KILL_AUTO_RECOVER:-false}"
V3_INGRESS_QUEUE_CAPACITY="${V3_INGRESS_QUEUE_CAPACITY:-65536}"
V3_DURABILITY_QUEUE_CAPACITY="${V3_DURABILITY_QUEUE_CAPACITY:-200000}"
V3_SHARD_COUNT="${V3_SHARD_COUNT:-8}"
V3_LOSS_GAP_TIMEOUT_MS="${V3_LOSS_GAP_TIMEOUT_MS:-500}"
V3_SESSION_LOSS_SUSPECT_THRESHOLD="${V3_SESSION_LOSS_SUSPECT_THRESHOLD:-1}"
V3_SHARD_LOSS_SUSPECT_THRESHOLD="${V3_SHARD_LOSS_SUSPECT_THRESHOLD:-3}"
V3_GLOBAL_LOSS_SUSPECT_THRESHOLD="${V3_GLOBAL_LOSS_SUSPECT_THRESHOLD:-6}"
V3_DURABLE_WORKER_BATCH_MAX="${V3_DURABLE_WORKER_BATCH_MAX:-12}"
V3_DURABLE_WORKER_BATCH_MIN="${V3_DURABLE_WORKER_BATCH_MIN:-6}"
V3_DURABLE_WORKER_BATCH_WAIT_US="${V3_DURABLE_WORKER_BATCH_WAIT_US:-80}"
V3_DURABLE_WORKER_BATCH_WAIT_MIN_US="${V3_DURABLE_WORKER_BATCH_WAIT_MIN_US:-40}"
V3_DURABLE_WORKER_BATCH_ADAPTIVE="${V3_DURABLE_WORKER_BATCH_ADAPTIVE:-false}"
V3_DURABLE_WORKER_BATCH_ADAPTIVE_LOW_UTIL_PCT="${V3_DURABLE_WORKER_BATCH_ADAPTIVE_LOW_UTIL_PCT:-10}"
V3_DURABLE_WORKER_BATCH_ADAPTIVE_HIGH_UTIL_PCT="${V3_DURABLE_WORKER_BATCH_ADAPTIVE_HIGH_UTIL_PCT:-60}"
FASTPATH_DRAIN_WORKERS="${FASTPATH_DRAIN_WORKERS:-4}"
V3_CONFIRM_REBUILD_ON_START="${V3_CONFIRM_REBUILD_ON_START:-false}"
V3_CONFIRM_REBUILD_MAX_LINES="${V3_CONFIRM_REBUILD_MAX_LINES:-200000}"

BASE_PROFILE="${BASE_PROFILE:-light}"
BASE_LOOPS="${BASE_LOOPS:-16}"
INJ_PROFILE="${INJ_PROFILE:-medium}"
INJ_LOOPS="${INJ_LOOPS:-8192}"
BASE_MARGIN_MODE="${BASE_MARGIN_MODE:-legacy}"
INJ_MARGIN_MODE="${INJ_MARGIN_MODE:-legacy}"

OUT_DIR="${OUT_DIR:-$ROOT_DIR/var/results}"
STAMP="$(date +%Y%m%d_%H%M%S)"
OUT_TSV="$OUT_DIR/v3_phase2_compare_${STAMP}.tsv"
OUT_SUMMARY="$OUT_DIR/v3_phase2_compare_${STAMP}.summary.txt"
NOISE_OUT="$OUT_DIR/v3_phase2_compare_${STAMP}.noise.txt"

mkdir -p "$OUT_DIR"

cd "$ROOT_DIR"

if ! [[ "$RUNS" =~ ^[0-9]+$ ]] || [[ "$RUNS" -le 0 ]]; then
  echo "FAIL: RUNS must be positive integer (current: $RUNS)"
  exit 1
fi
if ! [[ "$WARMUP_RUNS" =~ ^[0-9]+$ ]]; then
  echo "FAIL: WARMUP_RUNS must be integer >= 0 (current: $WARMUP_RUNS)"
  exit 1
fi
if ! [[ "$WRK_ACCOUNT_COUNT" =~ ^[0-9]+$ ]] || [[ "$WRK_ACCOUNT_COUNT" -le 0 ]]; then
  echo "FAIL: WRK_ACCOUNT_COUNT must be positive integer (current: $WRK_ACCOUNT_COUNT)"
  exit 1
fi

if [[ "$BUILD_RELEASE" == "1" ]]; then
  echo "[build] building release binary with fixed perf flags..."
  scripts/ops/build_gateway_rust_release.sh >/tmp/v3_phase2_compare_build.log 2>&1
fi

STRICT="$NOISE_GUARD_STRICT" CPU_THRESHOLD="$NOISE_CPU_THRESHOLD" \
  scripts/ops/perf_noise_guard.sh "$NOISE_OUT"

echo -e "case\trun\tsample\tack_p99_us\tack_accepted_p99_us\trisk_p99_us\trisk_position_p99_us\trisk_margin_p99_us\trisk_limits_p99_us\tparse_p99_us\tenqueue_p99_us\tserialize_p99_us\trisk_profile_level\trisk_profile_loops\trisk_margin_mode\trps\tv3_accepted\tv3_rejected_soft\tv3_rejected_killed\taccept_ratio\taccepted_rps" > "$OUT_TSV"

run_case() {
  local name="$1" profile="$2" loops="$3" margin_mode="$4" run="$5" sample="$6"
  local log_file="/tmp/${name}_${run}.log"
  local wrk_out="/tmp/wrk_${name}_${run}.txt"

  V3_RISK_PROFILE="$profile" \
  V3_RISK_LOOPS="$loops" \
  V3_RISK_MARGIN_MODE="$margin_mode" \
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
  GATEWAY_PORT="$PORT" \
  GATEWAY_TCP_PORT=0 \
  JWT_HS256_SECRET=secret123 \
  KAFKA_ENABLE=0 \
  FASTPATH_DRAIN_ENABLE=1 \
  FASTPATH_DRAIN_WORKERS="$FASTPATH_DRAIN_WORKERS" \
  V3_CONFIRM_REBUILD_ON_START="$V3_CONFIRM_REBUILD_ON_START" \
  V3_CONFIRM_REBUILD_MAX_LINES="$V3_CONFIRM_REBUILD_MAX_LINES" \
  ./gateway-rust/target/release/gateway-rust >"$log_file" 2>&1 &
  local pid=$!

  local ready=0
  for _ in $(seq 1 80); do
    if curl -sS "http://${HOST}:${PORT}/health" >/dev/null 2>&1; then
      ready=1
      break
    fi
    sleep 0.2
  done

  if [[ "$ready" != "1" ]]; then
    echo -e "${name}\t${run}\t${sample}\tSTART_FAILED\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-" | tee -a "$OUT_TSV"
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" >/dev/null 2>&1 || true
    return
  fi

  local rps
  if [[ "$WRK_ACCOUNT_COUNT" -le 1 ]]; then
    HOST="$HOST" \
    PORT="$PORT" \
    DURATION="$DURATION" \
    CONNECTIONS="$CONNECTIONS" \
    THREADS="$THREADS" \
    LATENCY=1 \
    WRK_PATH=/v3/orders \
    JWT_HS256_SECRET=secret123 \
    scripts/ops/wrk_gateway_rust.sh >"$wrk_out" 2>&1

    rps="$(awk '/Requests\/sec:/{print $2}' "$wrk_out" | tail -n1)"
  else
    local wrk_accounts="$WRK_ACCOUNT_COUNT"
    if [[ "$wrk_accounts" -gt "$CONNECTIONS" ]]; then
      wrk_accounts="$CONNECTIONS"
    fi
    if [[ "$wrk_accounts" -gt "$THREADS" ]]; then
      wrk_accounts="$THREADS"
    fi
    if [[ "$wrk_accounts" -le 0 ]]; then
      wrk_accounts=1
    fi

    local base_conn=$((CONNECTIONS / wrk_accounts))
    local extra_conn=$((CONNECTIONS % wrk_accounts))
    local base_thr=$((THREADS / wrk_accounts))
    local extra_thr=$((THREADS % wrk_accounts))
    local -a wrk_pids=()
    local -a wrk_out_files=()

    echo "# distributed_wrk accounts=${wrk_accounts} total_connections=${CONNECTIONS} total_threads=${THREADS}" >"$wrk_out"
    for idx in $(seq 1 "$wrk_accounts"); do
      local account_id="${WRK_ACCOUNT_PREFIX}_${idx}"
      local conn="$base_conn"
      local thr="$base_thr"
      if [[ "$idx" -le "$extra_conn" ]]; then
        conn=$((conn + 1))
      fi
      if [[ "$idx" -le "$extra_thr" ]]; then
        thr=$((thr + 1))
      fi
      if [[ "$conn" -le 0 ]]; then
        conn=1
      fi
      if [[ "$thr" -le 0 ]]; then
        thr=1
      fi

      local shard_out="/tmp/wrk_${name}_${run}_acct${idx}.txt"
      wrk_out_files+=("$shard_out")
      (
        HOST="$HOST" \
        PORT="$PORT" \
        DURATION="$DURATION" \
        CONNECTIONS="$conn" \
        THREADS="$thr" \
        LATENCY=1 \
        WRK_PATH=/v3/orders \
        JWT_HS256_SECRET=secret123 \
        ACCOUNT_ID="$account_id" \
        scripts/ops/wrk_gateway_rust.sh >"$shard_out" 2>&1
      ) &
      wrk_pids+=("$!")
    done

    local wrk_failed=0
    for wrk_pid in "${wrk_pids[@]}"; do
      if ! wait "$wrk_pid"; then
        wrk_failed=1
      fi
    done

    for shard_out in "${wrk_out_files[@]}"; do
      echo "" >>"$wrk_out"
      echo "## $(basename "$shard_out")" >>"$wrk_out"
      cat "$shard_out" >>"$wrk_out"
    done

    if [[ "$wrk_failed" == "1" ]]; then
      rps="NA"
    else
      rps="$(awk '/Requests\/sec:/{sum+=$2} END{if (sum>0) printf "%.2f", sum; else printf "0.00"}' "${wrk_out_files[@]}")"
    fi
  fi

  local metrics
  metrics="$(curl -sS "http://${HOST}:${PORT}/metrics")"
  local ack ack_accepted risk risk_pos risk_margin risk_limits parse enqueue ser level lps margin_mode_level acc soft killed
  ack="$(printf "%s\n" "$metrics" | awk '/^gateway_live_ack_p99_us /{print $2}')"
  if [[ -z "${ack}" ]]; then
    ack="$(printf "%s\n" "$metrics" | awk '/^gateway_ack_p99_us /{print $2}')"
  fi
  ack_accepted="$(printf "%s\n" "$metrics" | awk '/^gateway_live_ack_accepted_p99_us /{print $2}')"
  risk="$(printf "%s\n" "$metrics" | awk '/^gateway_v3_stage_risk_p99_us /{print $2}')"
  risk_pos="$(printf "%s\n" "$metrics" | awk '/^gateway_v3_stage_risk_position_p99_us /{print $2}')"
  risk_margin="$(printf "%s\n" "$metrics" | awk '/^gateway_v3_stage_risk_margin_p99_us /{print $2}')"
  risk_limits="$(printf "%s\n" "$metrics" | awk '/^gateway_v3_stage_risk_limits_p99_us /{print $2}')"
  parse="$(printf "%s\n" "$metrics" | awk '/^gateway_v3_stage_parse_p99_us /{print $2}')"
  enqueue="$(printf "%s\n" "$metrics" | awk '/^gateway_v3_stage_enqueue_p99_us /{print $2}')"
  ser="$(printf "%s\n" "$metrics" | awk '/^gateway_v3_stage_serialize_p99_us /{print $2}')"
  level="$(printf "%s\n" "$metrics" | awk '/^gateway_v3_risk_profile_level /{print $2}')"
  lps="$(printf "%s\n" "$metrics" | awk '/^gateway_v3_risk_profile_loops /{print $2}')"
  margin_mode_level="$(printf "%s\n" "$metrics" | awk '/^gateway_v3_risk_margin_mode /{print $2}')"
  acc="$(printf "%s\n" "$metrics" | awk '/^gateway_v3_accepted_total /{print $2}')"
  soft="$(printf "%s\n" "$metrics" | awk '/^gateway_v3_rejected_soft_total /{print $2}')"
  killed="$(printf "%s\n" "$metrics" | awk '/^gateway_v3_rejected_killed_total /{print $2}')"

  local accept_ratio accepted_rps
  accept_ratio="$(awk -v a="$acc" -v s="$soft" -v k="$killed" 'BEGIN{t=a+s+k; if(t>0) printf "%.6f", a/t; else printf "0.000000"}')"
  accepted_rps="$(awk -v r="$rps" -v ratio="$accept_ratio" 'BEGIN{printf "%.2f", r*ratio}')"

  echo -e "${name}\t${run}\t${sample}\t${ack:-NA}\t${ack_accepted:-NA}\t${risk:-NA}\t${risk_pos:-NA}\t${risk_margin:-NA}\t${risk_limits:-NA}\t${parse:-NA}\t${enqueue:-NA}\t${ser:-NA}\t${level:-NA}\t${lps:-NA}\t${margin_mode_level:-NA}\t${rps:-NA}\t${acc:-NA}\t${soft:-NA}\t${killed:-NA}\t${accept_ratio:-NA}\t${accepted_rps:-NA}" | tee -a "$OUT_TSV"

  kill "$pid" >/dev/null 2>&1 || true
  wait "$pid" >/dev/null 2>&1 || true
  sleep 1
}

TOTAL_RUNS=$((RUNS + WARMUP_RUNS))

for run in $(seq 1 "$TOTAL_RUNS"); do
  sample="measure"
  if [[ "$run" -le "$WARMUP_RUNS" ]]; then
    sample="warmup"
  fi
  run_case "baseline_${BASE_PROFILE}_l${BASE_LOOPS}_m${BASE_MARGIN_MODE}" "$BASE_PROFILE" "$BASE_LOOPS" "$BASE_MARGIN_MODE" "$run" "$sample"
done

for run in $(seq 1 "$TOTAL_RUNS"); do
  sample="measure"
  if [[ "$run" -le "$WARMUP_RUNS" ]]; then
    sample="warmup"
  fi
  run_case "injected_${INJ_PROFILE}_l${INJ_LOOPS}_m${INJ_MARGIN_MODE}" "$INJ_PROFILE" "$INJ_LOOPS" "$INJ_MARGIN_MODE" "$run" "$sample"
done

python3 - "$OUT_TSV" "$OUT_SUMMARY" "$WARMUP_RUNS" "$RUNS" "$NOISE_OUT" <<'PY'
import csv
import statistics
import sys

tsv_path = sys.argv[1]
summary_path = sys.argv[2]
warmup_runs = int(sys.argv[3])
measure_runs = int(sys.argv[4])
noise_path = sys.argv[5]
rows = list(csv.DictReader(open(tsv_path), delimiter="\t"))

cases = sorted({r["case"] for r in rows if r["case"]})
lines = [
    f"warmup_runs={warmup_runs}",
    f"measure_runs={measure_runs}",
    f"noise_snapshot={noise_path}",
]
for case in cases:
    sub = [
        r for r in rows
        if r["case"] == case
        and r["ack_p99_us"] not in ("START_FAILED", "NA", "")
        and int(r["run"]) > warmup_runs
    ]
    if not sub:
        lines.append(f"SUMMARY\t{case}\tno_valid_samples")
        continue

    def med(key):
        vals = [float(r[key]) for r in sub if r[key] not in ("NA", "", "-")]
        return statistics.median(vals) if vals else float("nan")

    lines.append(
        "SUMMARY\t{}\tack_p99_med={:.1f}\tack_accepted_p99_med={:.1f}\trisk_p99_med={:.1f}\trisk_position_p99_med={:.1f}\trisk_margin_p99_med={:.1f}\trisk_limits_p99_med={:.1f}\tparse_p99_med={:.1f}\tenqueue_p99_med={:.1f}\tserialize_p99_med={:.1f}\trps_med={:.1f}\taccept_ratio_med={:.6f}\taccepted_rps_med={:.1f}".format(
            case,
            med("ack_p99_us"),
            med("ack_accepted_p99_us"),
            med("risk_p99_us"),
            med("risk_position_p99_us"),
            med("risk_margin_p99_us"),
            med("risk_limits_p99_us"),
            med("parse_p99_us"),
            med("enqueue_p99_us"),
            med("serialize_p99_us"),
            med("rps"),
            med("accept_ratio"),
            med("accepted_rps"),
        )
    )

with open(summary_path, "w") as f:
    f.write("\n".join(lines) + "\n")
print("\n".join(lines))
PY

echo "saved_tsv=$OUT_TSV"
echo "saved_summary=$OUT_SUMMARY"
