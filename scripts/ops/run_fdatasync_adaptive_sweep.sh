#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

OUT_DIR="var/results"
STAMP="$(date +%Y%m%d_%H%M%S)"
RUNS="${RUNS:-2}"
CASES="${CASES:-baseline_w200_b64,tuned_w200_b128}"
SUMMARY_CSV="${OUT_DIR}/fdatasync_adaptive_sweep_${STAMP}.csv"
SUMMARY_MD="${OUT_DIR}/fdatasync_adaptive_sweep_${STAMP}.md"

mkdir -p "$OUT_DIR"

PROFILES=(
  "fixed:AUDIT_FDATASYNC_ADAPTIVE=0"
  "adp_safe:AUDIT_FDATASYNC_ADAPTIVE=1 AUDIT_FDATASYNC_ADAPTIVE_MIN_WAIT_US=100 AUDIT_FDATASYNC_ADAPTIVE_MAX_WAIT_US=200 AUDIT_FDATASYNC_ADAPTIVE_MIN_BATCH=64 AUDIT_FDATASYNC_ADAPTIVE_MAX_BATCH=128 AUDIT_FDATASYNC_ADAPTIVE_TARGET_SYNC_US=5000"
  "adp_balanced:AUDIT_FDATASYNC_ADAPTIVE=1 AUDIT_FDATASYNC_ADAPTIVE_MIN_WAIT_US=80 AUDIT_FDATASYNC_ADAPTIVE_MAX_WAIT_US=220 AUDIT_FDATASYNC_ADAPTIVE_MIN_BATCH=48 AUDIT_FDATASYNC_ADAPTIVE_MAX_BATCH=160 AUDIT_FDATASYNC_ADAPTIVE_TARGET_SYNC_US=4000"
  "adp_aggressive:AUDIT_FDATASYNC_ADAPTIVE=1 AUDIT_FDATASYNC_ADAPTIVE_MIN_WAIT_US=50 AUDIT_FDATASYNC_ADAPTIVE_MAX_WAIT_US=220 AUDIT_FDATASYNC_ADAPTIVE_MIN_BATCH=32 AUDIT_FDATASYNC_ADAPTIVE_MAX_BATCH=192 AUDIT_FDATASYNC_ADAPTIVE_TARGET_SYNC_US=3500"
)

run_profile_once() {
  local profile="$1"
  local envs="$2"
  local run_idx="$3"
  local log_file="${OUT_DIR}/fdatasync_adaptive_${profile}_run${run_idx}_${STAMP}.log"
  # shellcheck disable=SC2086
  eval "CASES=\"$CASES\" $envs scripts/ops/run_durability_tuning_scan.sh >\"$log_file\" 2>&1"
  local csv_path
  csv_path="$(awk -F= '/^OUT=/{print $2}' "$log_file" | tail -n1)"
  if [[ -z "$csv_path" || ! -f "$csv_path" ]]; then
    echo "ERROR: missing csv for ${profile} run ${run_idx}" >&2
    tail -n 80 "$log_file" >&2 || true
    exit 1
  fi
  echo "$csv_path"
}

tmp_map="${OUT_DIR}/fdatasync_adaptive_map_${STAMP}.txt"
> "$tmp_map"

for spec in "${PROFILES[@]}"; do
  profile="${spec%%:*}"
  envs="${spec#*:}"
  for i in $(seq 1 "$RUNS"); do
    echo "==> ${profile} run ${i}/${RUNS}"
    csv="$(run_profile_once "$profile" "$envs" "$i")"
    echo "${profile},${csv}" >> "$tmp_map"
  done
done

python3 - "$tmp_map" "$SUMMARY_CSV" <<'PY'
import csv
import statistics
import sys

map_file, out_csv = sys.argv[1], sys.argv[2]
items = []
for line in open(map_file):
    profile, path = line.strip().split(",", 1)
    items.append((profile, path))

metrics = [
    "throughput_rps",
    "acc_p99_us",
    "wal_enqueue_p99_us",
    "fdatasync_p99_us",
    "durable_ack_p99_us",
    "durable_notify_p99_us",
]

group = {}
for profile, path in items:
    for r in csv.DictReader(open(path)):
        key = (profile, r["case"])
        group.setdefault(key, []).append(r)

with open(out_csv, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["profile", "case", "runs"] + metrics)
    for (profile, case), samples in sorted(group.items()):
        row = [profile, case, len(samples)]
        for m in metrics:
            vals = [float(s[m]) for s in samples]
            row.append(f"{statistics.median(vals):.2f}")
        w.writerow(row)

print("WROTE", out_csv)
PY

python3 - "$SUMMARY_CSV" "$SUMMARY_MD" <<'PY'
import csv
import sys

inp, out = sys.argv[1], sys.argv[2]
rows = list(csv.DictReader(open(inp)))
lines = []
lines.append("| profile | case | runs | throughput_rps | acc_p99_us | wal_enqueue_p99_us | fdatasync_p99_us | durable_ack_p99_us | durable_notify_p99_us |")
lines.append("|---|---|---:|---:|---:|---:|---:|---:|---:|")
for r in rows:
    lines.append(
        f"| {r['profile']} | {r['case']} | {r['runs']} | {r['throughput_rps']} | {r['acc_p99_us']} | {r['wal_enqueue_p99_us']} | {r['fdatasync_p99_us']} | {r['durable_ack_p99_us']} | {r['durable_notify_p99_us']} |"
    )
open(out, "w").write("\n".join(lines) + "\n")
print("\n".join(lines))
PY

echo "SUMMARY_CSV=${SUMMARY_CSV}"
echo "SUMMARY_MD=${SUMMARY_MD}"
