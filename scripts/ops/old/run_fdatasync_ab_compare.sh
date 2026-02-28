#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

RUNS="${RUNS:-3}"
CASES="${CASES:-baseline_w200_b64,tuned_w200_b128,stress_w5000_b1}"
OUT_DIR="var/results"
STAMP="$(date +%Y%m%d_%H%M%S)"
SUMMARY_CSV="${OUT_DIR}/fdatasync_ab_summary_${STAMP}.csv"
SUMMARY_MD="${OUT_DIR}/fdatasync_ab_summary_${STAMP}.md"

mkdir -p "$OUT_DIR"

run_profile() {
  local profile="$1"
  local run_idx="$2"
  local log_file="${OUT_DIR}/fdatasync_ab_${profile}_run${run_idx}_${STAMP}.log"

  if [[ "$profile" == "fixed" ]]; then
    CASES="$CASES" \
    AUDIT_FDATASYNC_ADAPTIVE=0 \
    scripts/ops/run_durability_tuning_scan.sh >"$log_file" 2>&1
  else
    CASES="$CASES" \
    AUDIT_FDATASYNC_ADAPTIVE=1 \
    AUDIT_FDATASYNC_ADAPTIVE_MIN_WAIT_US=50 \
    AUDIT_FDATASYNC_ADAPTIVE_MAX_WAIT_US=200 \
    AUDIT_FDATASYNC_ADAPTIVE_MIN_BATCH=16 \
    AUDIT_FDATASYNC_ADAPTIVE_MAX_BATCH=128 \
    AUDIT_FDATASYNC_ADAPTIVE_TARGET_SYNC_US=4000 \
    scripts/ops/run_durability_tuning_scan.sh >"$log_file" 2>&1
  fi

  local csv_path
  csv_path="$(awk -F= '/^OUT=/{print $2}' "$log_file" | tail -n1)"
  if [[ -z "$csv_path" || ! -f "$csv_path" ]]; then
    echo "ERROR: failed to find output csv for ${profile} run ${run_idx}" >&2
    tail -n 80 "$log_file" >&2 || true
    exit 1
  fi
  echo "$csv_path"
}

fixed_csvs=()
adaptive_csvs=()

for i in $(seq 1 "$RUNS"); do
  echo "==> fixed run ${i}/${RUNS}"
  fixed_csvs+=("$(run_profile fixed "$i")")
done

for i in $(seq 1 "$RUNS"); do
  echo "==> adaptive run ${i}/${RUNS}"
  adaptive_csvs+=("$(run_profile adaptive "$i")")
done

python3 - "$SUMMARY_CSV" "${fixed_csvs[@]}" -- "${adaptive_csvs[@]}" <<'PY'
import csv
import statistics
import sys

out = sys.argv[1]
args = sys.argv[2:]
sep = args.index("--")
fixed_files = args[:sep]
adaptive_files = args[sep + 1 :]
profiles = [("fixed", fixed_files), ("adaptive", adaptive_files)]

metrics = [
    "throughput_rps",
    "acc_p99_us",
    "wal_enqueue_p99_us",
    "fdatasync_p99_us",
    "durable_ack_p99_us",
    "durable_notify_p99_us",
]

rows = []
for profile, files in profiles:
    by_case = {}
    for path in files:
        for r in csv.DictReader(open(path)):
            by_case.setdefault(r["case"], []).append(r)
    for case, samples in sorted(by_case.items()):
        out_row = {"profile": profile, "case": case, "runs": len(samples)}
        for m in metrics:
            vals = [float(s[m]) for s in samples]
            out_row[m] = statistics.median(vals)
        rows.append(out_row)

with open(out, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["profile", "case", "runs"] + metrics)
    for r in rows:
        w.writerow([r["profile"], r["case"], r["runs"]] + [f"{r[m]:.2f}" for m in metrics])

print("WROTE", out)
PY

python3 - "$SUMMARY_CSV" "$SUMMARY_MD" <<'PY'
import csv
import sys

inp, md = sys.argv[1], sys.argv[2]
rows = list(csv.DictReader(open(inp)))
lines = []
lines.append("| profile | case | runs | throughput_rps | acc_p99_us | wal_enqueue_p99_us | fdatasync_p99_us | durable_ack_p99_us | durable_notify_p99_us |")
lines.append("|---|---|---:|---:|---:|---:|---:|---:|---:|")
for r in rows:
    lines.append(
        f"| {r['profile']} | {r['case']} | {r['runs']} | {r['throughput_rps']} | {r['acc_p99_us']} | {r['wal_enqueue_p99_us']} | {r['fdatasync_p99_us']} | {r['durable_ack_p99_us']} | {r['durable_notify_p99_us']} |"
    )
open(md, "w").write("\n".join(lines) + "\n")
print("\n".join(lines))
PY

echo "SUMMARY_CSV=${SUMMARY_CSV}"
echo "SUMMARY_MD=${SUMMARY_MD}"
