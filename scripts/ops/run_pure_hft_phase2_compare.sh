#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
PORT="${PORT:-29001}"
HOST="${HOST:-127.0.0.1}"
RUNS="${RUNS:-5}"
DURATION="${DURATION:-8}"
CONNECTIONS="${CONNECTIONS:-300}"
THREADS="${THREADS:-8}"

BASE_PROFILE="${BASE_PROFILE:-light}"
BASE_LOOPS="${BASE_LOOPS:-16}"
INJ_PROFILE="${INJ_PROFILE:-medium}"
INJ_LOOPS="${INJ_LOOPS:-8192}"
BASE_MARGIN_MODE="${BASE_MARGIN_MODE:-legacy}"
INJ_MARGIN_MODE="${INJ_MARGIN_MODE:-legacy}"

OUT_DIR="${OUT_DIR:-$ROOT_DIR/var/results}"
STAMP="$(date +%Y%m%d_%H%M%S)"
OUT_TSV="$OUT_DIR/pure_hft_phase2_compare_${STAMP}.tsv"
OUT_SUMMARY="$OUT_DIR/pure_hft_phase2_compare_${STAMP}.summary.txt"

mkdir -p "$OUT_DIR"

cd "$ROOT_DIR"

echo -e "case\trun\tack_p99_us\trisk_p99_us\trisk_position_p99_us\trisk_margin_p99_us\trisk_limits_p99_us\tparse_p99_us\tenqueue_p99_us\tserialize_p99_us\trisk_profile_level\trisk_profile_loops\trisk_margin_mode\trps\tv3_accepted\tv3_rejected_soft\tv3_rejected_killed\taccept_ratio\taccepted_rps" > "$OUT_TSV"

run_case() {
  local name="$1" profile="$2" loops="$3" margin_mode="$4" run="$5"
  local log_file="/tmp/${name}_${run}.log"
  local wrk_out="/tmp/wrk_${name}_${run}.txt"

  V3_RISK_PROFILE="$profile" \
  V3_RISK_LOOPS="$loops" \
  V3_RISK_MARGIN_MODE="$margin_mode" \
  GATEWAY_PORT="$PORT" \
  GATEWAY_TCP_PORT=0 \
  JWT_HS256_SECRET=secret123 \
  KAFKA_ENABLE=0 \
  FASTPATH_DRAIN_ENABLE=1 \
  FASTPATH_DRAIN_WORKERS=4 \
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
    echo -e "${name}\t${run}\tSTART_FAILED\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-" | tee -a "$OUT_TSV"
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
  WRK_PATH=/v3/orders \
  JWT_HS256_SECRET=secret123 \
  scripts/ops/wrk_gateway_rust.sh >"$wrk_out" 2>&1

  local rps
  rps="$(awk '/Requests\/sec:/{print $2}' "$wrk_out" | tail -n1)"

  local metrics
  metrics="$(curl -sS "http://${HOST}:${PORT}/metrics")"
  local ack risk risk_pos risk_margin risk_limits parse enqueue ser level lps margin_mode_level acc soft killed
  ack="$(printf "%s\n" "$metrics" | awk '/^gateway_ack_p99_us /{print $2}')"
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

  echo -e "${name}\t${run}\t${ack:-NA}\t${risk:-NA}\t${risk_pos:-NA}\t${risk_margin:-NA}\t${risk_limits:-NA}\t${parse:-NA}\t${enqueue:-NA}\t${ser:-NA}\t${level:-NA}\t${lps:-NA}\t${margin_mode_level:-NA}\t${rps:-NA}\t${acc:-NA}\t${soft:-NA}\t${killed:-NA}\t${accept_ratio:-NA}\t${accepted_rps:-NA}" | tee -a "$OUT_TSV"

  kill "$pid" >/dev/null 2>&1 || true
  wait "$pid" >/dev/null 2>&1 || true
  sleep 1
}

for run in $(seq 1 "$RUNS"); do
  run_case "baseline_${BASE_PROFILE}_l${BASE_LOOPS}_m${BASE_MARGIN_MODE}" "$BASE_PROFILE" "$BASE_LOOPS" "$BASE_MARGIN_MODE" "$run"
done

for run in $(seq 1 "$RUNS"); do
  run_case "injected_${INJ_PROFILE}_l${INJ_LOOPS}_m${INJ_MARGIN_MODE}" "$INJ_PROFILE" "$INJ_LOOPS" "$INJ_MARGIN_MODE" "$run"
done

python3 - "$OUT_TSV" "$OUT_SUMMARY" <<'PY'
import csv
import statistics
import sys

tsv_path = sys.argv[1]
summary_path = sys.argv[2]
rows = list(csv.DictReader(open(tsv_path), delimiter="\t"))

cases = sorted({r["case"] for r in rows if r["case"]})
lines = []
for case in cases:
    sub = [r for r in rows if r["case"] == case and r["ack_p99_us"] not in ("START_FAILED", "NA", "")]
    if not sub:
        lines.append(f"SUMMARY\t{case}\tno_valid_samples")
        continue

    def med(key):
        vals = [float(r[key]) for r in sub if r[key] not in ("NA", "", "-")]
        return statistics.median(vals) if vals else float("nan")

    lines.append(
        "SUMMARY\t{}\tack_p99_med={:.1f}\trisk_p99_med={:.1f}\trisk_position_p99_med={:.1f}\trisk_margin_p99_med={:.1f}\trisk_limits_p99_med={:.1f}\tparse_p99_med={:.1f}\tenqueue_p99_med={:.1f}\tserialize_p99_med={:.1f}\trps_med={:.1f}\taccept_ratio_med={:.6f}\taccepted_rps_med={:.1f}".format(
            case,
            med("ack_p99_us"),
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
