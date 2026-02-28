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
V2_DURABLE_WAIT_TIMEOUT_MS="${V2_DURABLE_WAIT_TIMEOUT_MS:-200}"
BUILD_RELEASE="${BUILD_RELEASE:-1}"
NOISE_GUARD_STRICT="${NOISE_GUARD_STRICT:-0}"
NOISE_CPU_THRESHOLD="${NOISE_CPU_THRESHOLD:-35}"

OUT_DIR="${OUT_DIR:-$ROOT_DIR/var/results}"
STAMP="$(date +%Y%m%d_%H%M%S)"
OUT_TSV="$OUT_DIR/v2_contract_compare_${STAMP}.tsv"
OUT_SUMMARY="$OUT_DIR/v2_contract_compare_${STAMP}.summary.txt"
NOISE_OUT="$OUT_DIR/v2_contract_compare_${STAMP}.noise.txt"

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

if [[ "$BUILD_RELEASE" == "1" ]]; then
  echo "[build] building release binary with fixed perf flags..."
  scripts/ops/build_gateway_rust_release.sh >/tmp/v2_contract_compare_build.log 2>&1
fi

STRICT="$NOISE_GUARD_STRICT" CPU_THRESHOLD="$NOISE_CPU_THRESHOLD" \
  scripts/ops/perf_noise_guard.sh "$NOISE_OUT"

echo -e "case\trun\tsample\tpath\twait_timeout_ms\tack_p99_us\tdurable_ack_p99_us\tfdatasync_p99_us\twal_enqueue_p99_us\trps\taccepted\trejected\taccept_ratio\taccepted_rps\tv2_timeout_total\tv2_timeout_ratio" > "$OUT_TSV"

metric_value() {
  local metrics="$1"
  local key="$2"
  printf "%s\n" "$metrics" | awk -v k="$key" '$1==k{print $2}' | tail -n1
}

run_case() {
  local name="$1" path="$2" run="$3" sample="$4"
  local log_file="/tmp/${name}_${run}.log"
  local wrk_out="/tmp/wrk_${name}_${run}.txt"

  GATEWAY_PORT="$PORT" \
  GATEWAY_TCP_PORT=0 \
  JWT_HS256_SECRET=secret123 \
  KAFKA_ENABLE=0 \
  FASTPATH_DRAIN_ENABLE=1 \
  FASTPATH_DRAIN_WORKERS=4 \
  AUDIT_ASYNC_WAL=1 \
  AUDIT_FDATASYNC=1 \
  V2_DURABLE_WAIT_TIMEOUT_MS="$V2_DURABLE_WAIT_TIMEOUT_MS" \
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
    echo -e "${name}\t${run}\t${sample}\t${path}\t${V2_DURABLE_WAIT_TIMEOUT_MS}\tSTART_FAILED\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-" | tee -a "$OUT_TSV"
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" >/dev/null 2>&1 || true
    return
  fi

  HOST="$HOST" \
  PORT="$PORT" \
  DURATION="$DURATION" \
  CONNECTIONS="$CONNECTIONS" \
  THREADS="$THREADS" \
  LATENCY=1 \
  WRK_PATH="$path" \
  JWT_HS256_SECRET=secret123 \
  scripts/ops/wrk_gateway_rust.sh >"$wrk_out" 2>&1

  local rps
  rps="$(awk '/Requests\/sec:/{print $2}' "$wrk_out" | tail -n1)"
  local metrics
  metrics="$(curl -sS "http://${HOST}:${PORT}/metrics")"

  local ack_p99 durable_ack_p99 fdatasync_p99 wal_enqueue_p99
  ack_p99="$(metric_value "$metrics" gateway_ack_p99_us)"
  durable_ack_p99="$(metric_value "$metrics" gateway_durable_ack_p99_us)"
  fdatasync_p99="$(metric_value "$metrics" gateway_fdatasync_p99_us)"
  wal_enqueue_p99="$(metric_value "$metrics" gateway_wal_enqueue_p99_us)"
  local v2_timeout_total v2_timeout_ratio
  v2_timeout_total="$(metric_value "$metrics" gateway_v2_durable_wait_timeout_total)"
  v2_timeout_ratio="$(metric_value "$metrics" gateway_v2_durable_wait_timeout_ratio)"

  local creates hits rej_invalid_qty rej_rate_limit rej_risk rej_invalid_symbol rej_queue_full
  local bp_soft_wal_age bp_soft_rate_decline bp_inflight bp_wal_bytes bp_wal_age bp_disk_free
  creates="$(metric_value "$metrics" gateway_idempotency_creates_total)"
  hits="$(metric_value "$metrics" gateway_idempotency_hits_total)"
  rej_invalid_qty="$(metric_value "$metrics" gateway_reject_invalid_qty_total)"
  rej_rate_limit="$(metric_value "$metrics" gateway_reject_rate_limit_total)"
  rej_risk="$(metric_value "$metrics" gateway_reject_risk_total)"
  rej_invalid_symbol="$(metric_value "$metrics" gateway_reject_invalid_symbol_total)"
  rej_queue_full="$(metric_value "$metrics" gateway_reject_queue_full_total)"
  bp_soft_wal_age="$(metric_value "$metrics" gateway_backpressure_soft_wal_age_total)"
  bp_soft_rate_decline="$(metric_value "$metrics" gateway_backpressure_soft_rate_decline_total)"
  bp_inflight="$(metric_value "$metrics" gateway_backpressure_inflight_total)"
  bp_wal_bytes="$(metric_value "$metrics" gateway_backpressure_wal_bytes_total)"
  bp_wal_age="$(metric_value "$metrics" gateway_backpressure_wal_age_total)"
  bp_disk_free="$(metric_value "$metrics" gateway_backpressure_disk_free_total)"

  local accepted rejected accept_ratio accepted_rps
  accepted="$(awk -v c="${creates:-0}" -v h="${hits:-0}" 'BEGIN{printf "%.0f", c+h}')"
  rejected="$(awk \
    -v a="${rej_invalid_qty:-0}" \
    -v b="${rej_rate_limit:-0}" \
    -v c="${rej_risk:-0}" \
    -v d="${rej_invalid_symbol:-0}" \
    -v e="${rej_queue_full:-0}" \
    -v f="${bp_soft_wal_age:-0}" \
    -v g="${bp_soft_rate_decline:-0}" \
    -v h="${bp_inflight:-0}" \
    -v i="${bp_wal_bytes:-0}" \
    -v j="${bp_wal_age:-0}" \
    -v k="${bp_disk_free:-0}" \
    'BEGIN{printf "%.0f", a+b+c+d+e+f+g+h+i+j+k}')"
  if [[ "$path" == "/v2/orders" ]]; then
    accepted="$(awk -v a="$accepted" -v t="${v2_timeout_total:-0}" 'BEGIN{v=a-t; if(v<0) v=0; printf "%.0f", v}')"
    rejected="$(awk -v r="$rejected" -v t="${v2_timeout_total:-0}" 'BEGIN{printf "%.0f", r+t}')"
  fi
  accept_ratio="$(awk -v a="$accepted" -v r="$rejected" 'BEGIN{t=a+r; if(t>0) printf "%.6f", a/t; else printf "0.000000"}')"
  accepted_rps="$(awk -v p="${rps:-0}" -v ratio="$accept_ratio" 'BEGIN{printf "%.2f", p*ratio}')"

  echo -e "${name}\t${run}\t${sample}\t${path}\t${V2_DURABLE_WAIT_TIMEOUT_MS}\t${ack_p99:-NA}\t${durable_ack_p99:-NA}\t${fdatasync_p99:-NA}\t${wal_enqueue_p99:-NA}\t${rps:-NA}\t${accepted:-NA}\t${rejected:-NA}\t${accept_ratio:-NA}\t${accepted_rps:-NA}\t${v2_timeout_total:-NA}\t${v2_timeout_ratio:-NA}" | tee -a "$OUT_TSV"

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
  run_case "legacy_orders" "/orders" "$run" "$sample"
done

for run in $(seq 1 "$TOTAL_RUNS"); do
  sample="measure"
  if [[ "$run" -le "$WARMUP_RUNS" ]]; then
    sample="warmup"
  fi
  run_case "v2_orders" "/v2/orders" "$run" "$sample"
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
        "SUMMARY\t{}\tack_p99_med={:.1f}\tdurable_ack_p99_med={:.1f}\tfdatasync_p99_med={:.1f}\twal_enqueue_p99_med={:.1f}\trps_med={:.1f}\taccept_ratio_med={:.6f}\taccepted_rps_med={:.1f}\tv2_timeout_total_med={:.1f}\tv2_timeout_ratio_med={:.6f}".format(
            case,
            med("ack_p99_us"),
            med("durable_ack_p99_us"),
            med("fdatasync_p99_us"),
            med("wal_enqueue_p99_us"),
            med("rps"),
            med("accept_ratio"),
            med("accepted_rps"),
            med("v2_timeout_total"),
            med("v2_timeout_ratio"),
        )
    )

with open(summary_path, "w") as f:
    f.write("\n".join(lines) + "\n")
print("\n".join(lines))
PY

echo "saved_tsv=$OUT_TSV"
echo "saved_summary=$OUT_SUMMARY"
