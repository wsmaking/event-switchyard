#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/var/results}"
RUNS="${RUNS:-6}"
INTERVAL_SEC="${INTERVAL_SEC:-600}"
MIN_PASS_RATIO="${MIN_PASS_RATIO:-1.0}"

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "FAIL: pure HFT absolute gate loop requires Linux (current: $(uname -s))"
  exit 2
fi

if ! [[ "$RUNS" =~ ^[0-9]+$ ]] || [[ "$RUNS" -le 0 ]]; then
  echo "FAIL: RUNS must be a positive integer (current: $RUNS)"
  exit 1
fi
if ! [[ "$INTERVAL_SEC" =~ ^[0-9]+$ ]]; then
  echo "FAIL: INTERVAL_SEC must be an integer >= 0 (current: $INTERVAL_SEC)"
  exit 1
fi

mkdir -p "$OUT_DIR"
STAMP="$(date +%Y%m%d_%H%M%S)"
RAW_TSV="$OUT_DIR/v3_gate_loop_${STAMP}.tsv"
SUMMARY_OUT="$OUT_DIR/v3_gate_loop_${STAMP}.summary.txt"

echo -e "run\trc\trps\tlive_ack_p99_us\taccepted_rate\tsummary_path" >"$RAW_TSV"

for run in $(seq 1 "$RUNS"); do
  run_out_dir="$OUT_DIR/v3_gate_loop_${STAMP}_run${run}"
  run_log="$run_out_dir/run.log"
  mkdir -p "$run_out_dir"

  echo "[run ${run}/${RUNS}] OUT_DIR=${run_out_dir}"
  set +e
  OUT_DIR="$run_out_dir" scripts/ops/check_v3_absolute_gate.sh >"$run_log" 2>&1
  rc=$?
  set -e

  summary_path="$(ls -1t "$run_out_dir"/v3_gate_*.summary.txt 2>/dev/null | head -n1 || true)"
  rps="NA"
  live_ack_p99_us="NA"
  accepted_rate="NA"
  if [[ -n "$summary_path" ]]; then
    rps="$(awk -F= '/^observed_rps=/{print $2}' "$summary_path" | tail -n1)"
    live_ack_p99_us="$(awk -F= '/^observed_live_ack_p99_us=/{print $2}' "$summary_path" | tail -n1)"
    accepted_rate="$(awk -F= '/^observed_accepted_rate=/{print $2}' "$summary_path" | tail -n1)"
    rps="${rps:-NA}"
    live_ack_p99_us="${live_ack_p99_us:-NA}"
    accepted_rate="${accepted_rate:-NA}"
  fi

  echo -e "${run}\t${rc}\t${rps}\t${live_ack_p99_us}\t${accepted_rate}\t${summary_path}" >>"$RAW_TSV"
  echo "[run ${run}/${RUNS}] rc=${rc} rps=${rps} ack_p99=${live_ack_p99_us} accepted_rate=${accepted_rate}"

  if [[ "$run" -lt "$RUNS" && "$INTERVAL_SEC" -gt 0 ]]; then
    sleep "$INTERVAL_SEC"
  fi
done

python3 - "$RAW_TSV" "$SUMMARY_OUT" "$RUNS" "$MIN_PASS_RATIO" <<'PY'
import csv
import statistics
import sys

raw_tsv, summary_path, runs_s, min_ratio_s = sys.argv[1:]
runs = int(runs_s)
min_ratio = float(min_ratio_s)
rows = list(csv.DictReader(open(raw_tsv), delimiter="\t"))

pass_count = sum(1 for r in rows if r["rc"] == "0")
fail_count = len(rows) - pass_count
pass_ratio = (pass_count / len(rows)) if rows else 0.0

def median_numeric(field: str) -> float | None:
    vals = []
    for r in rows:
        v = r.get(field, "")
        if not v or v == "NA":
            continue
        try:
            vals.append(float(v))
        except ValueError:
            continue
    if not vals:
        return None
    return statistics.median(vals)

rps_median = median_numeric("rps")
ack_p99_median = median_numeric("live_ack_p99_us")
accepted_rate_median = median_numeric("accepted_rate")

lines = [
    "v3_absolute_gate_loop",
    f"runs={runs}",
    f"pass_count={pass_count}",
    f"fail_count={fail_count}",
    f"pass_ratio={pass_ratio:.6f}",
    f"required_pass_ratio={min_ratio:.6f}",
    f"median_observed_rps={rps_median if rps_median is not None else 'NA'}",
    f"median_observed_live_ack_p99_us={ack_p99_median if ack_p99_median is not None else 'NA'}",
    f"median_observed_accepted_rate={accepted_rate_median if accepted_rate_median is not None else 'NA'}",
    f"raw_tsv={raw_tsv}",
]
open(summary_path, "w").write("\n".join(lines) + "\n")
print("\n".join(lines))
PY

echo "[artifacts] raw=${RAW_TSV}"
echo "[artifacts] summary=${SUMMARY_OUT}"

pass_ratio="$(awk -F= '/^pass_ratio=/{print $2}' "$SUMMARY_OUT" | tail -n1)"
required_ratio="$(awk -F= '/^required_pass_ratio=/{print $2}' "$SUMMARY_OUT" | tail -n1)"
ok="$(awk -v v="$pass_ratio" -v t="$required_ratio" 'BEGIN{print (v+0>=t+0) ? 1 : 0}')"

if [[ "$ok" == "1" ]]; then
  echo "PASS: pure HFT absolute gate loop satisfied"
  exit 0
fi

echo "FAIL: pure HFT absolute gate loop failed (pass_ratio=${pass_ratio}, required=${required_ratio})"
exit 1
