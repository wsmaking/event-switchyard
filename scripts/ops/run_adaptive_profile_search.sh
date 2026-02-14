#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

OUT_DIR="var/results"
STAMP="$(date +%Y%m%d_%H%M%S)"
RUNS="${RUNS:-1}"
DUR_CASES="${DUR_CASES:-baseline_w200_b64,tuned_w200_b128,stress_w5000_b1}"
SUMMARY_CSV="${OUT_DIR}/adaptive_profile_search_${STAMP}.csv"
SUMMARY_MD="${OUT_DIR}/adaptive_profile_search_${STAMP}.md"
mkdir -p "$OUT_DIR"

PROFILES=(
  "fixed:AUDIT_FDATASYNC_ADAPTIVE=0"
  "adp_safe:AUDIT_FDATASYNC_ADAPTIVE=1 AUDIT_FDATASYNC_ADAPTIVE_MIN_WAIT_US=100 AUDIT_FDATASYNC_ADAPTIVE_MAX_WAIT_US=200 AUDIT_FDATASYNC_ADAPTIVE_MIN_BATCH=64 AUDIT_FDATASYNC_ADAPTIVE_MAX_BATCH=128 AUDIT_FDATASYNC_ADAPTIVE_TARGET_SYNC_US=5000"
  "adp_balanced:AUDIT_FDATASYNC_ADAPTIVE=1 AUDIT_FDATASYNC_ADAPTIVE_MIN_WAIT_US=80 AUDIT_FDATASYNC_ADAPTIVE_MAX_WAIT_US=220 AUDIT_FDATASYNC_ADAPTIVE_MIN_BATCH=48 AUDIT_FDATASYNC_ADAPTIVE_MAX_BATCH=160 AUDIT_FDATASYNC_ADAPTIVE_TARGET_SYNC_US=4000"
  "adp_aggressive:AUDIT_FDATASYNC_ADAPTIVE=1 AUDIT_FDATASYNC_ADAPTIVE_MIN_WAIT_US=50 AUDIT_FDATASYNC_ADAPTIVE_MAX_WAIT_US=220 AUDIT_FDATASYNC_ADAPTIVE_MIN_BATCH=32 AUDIT_FDATASYNC_ADAPTIVE_MAX_BATCH=192 AUDIT_FDATASYNC_ADAPTIVE_TARGET_SYNC_US=3500"
)

run_durability() {
  local profile="$1"
  local envs="$2"
  local run_idx="$3"
  local log_file="${OUT_DIR}/adaptive_search_dur_${profile}_run${run_idx}_${STAMP}.log"
  # shellcheck disable=SC2086
  eval "CASES=\"$DUR_CASES\" $envs scripts/ops/run_durability_tuning_scan.sh >\"$log_file\" 2>&1"
  awk -F= '/^OUT=/{print $2}' "$log_file" | tail -n1
}

run_switch() {
  local profile="$1"
  local envs="$2"
  local run_idx="$3"
  local log_file="${OUT_DIR}/adaptive_search_switch_${profile}_run${run_idx}_${STAMP}.log"
  # shellcheck disable=SC2086
  eval "NORMAL_WAIT_US=200 NORMAL_BATCH=128 DURATION=8 CONCURRENCY=200 $envs scripts/ops/run_preset_switch_validation.sh >\"$log_file\" 2>&1"
  awk -F= '/^RAW_CSV=/{print $2}' "$log_file" | tail -n1
}

tmp_map="${OUT_DIR}/adaptive_profile_search_map_${STAMP}.txt"
> "$tmp_map"

for spec in "${PROFILES[@]}"; do
  profile="${spec%%:*}"
  envs="${spec#*:}"
  for i in $(seq 1 "$RUNS"); do
    echo "==> ${profile} run ${i}/${RUNS}: durability"
    dur_csv="$(run_durability "$profile" "$envs" "$i")"
    [[ -f "$dur_csv" ]] || { echo "missing durability csv for ${profile} run ${i}" >&2; exit 1; }
    echo "==> ${profile} run ${i}/${RUNS}: switch"
    sw_csv="$(run_switch "$profile" "$envs" "$i")"
    [[ -f "$sw_csv" ]] || { echo "missing switch csv for ${profile} run ${i}" >&2; exit 1; }
    echo "${profile},${i},${dur_csv},${sw_csv}" >> "$tmp_map"
  done
done

python3 - "$tmp_map" "$SUMMARY_CSV" <<'PY'
import csv
import statistics
import sys

map_file, out_csv = sys.argv[1], sys.argv[2]
items = []
for line in open(map_file):
    profile, run_idx, dur_csv, sw_csv = line.strip().split(",", 3)
    items.append((profile, int(run_idx), dur_csv, sw_csv))

per_run = []
for profile, run_idx, dur_csv, sw_csv in items:
    drows = {r["case"]: r for r in csv.DictReader(open(dur_csv))}
    srows = {r["phase"]: r for r in csv.DictReader(open(sw_csv))}

    base = drows["baseline_w200_b64"]
    tuned = drows["tuned_w200_b128"]
    stress = drows["stress_w5000_b1"]
    nbase = srows["normal_baseline"]
    nrec = srows["normal_recovery"]
    dgr = srows["degraded_injected"]

    def f(row, key): return float(row[key])
    gate_ok = (
        f(nbase, "queue_full_total") == 0
        and f(dgr, "queue_full_total") == 0
        and f(nrec, "queue_full_total") == 0
        and f(dgr, "wal_age_ms") >= 550
        and f(dgr, "limit_dynamic") < f(nbase, "limit_dynamic")
        and f(nrec, "wal_age_ms") <= 200
    )

    # 安定性込みの評価値（小さいほど良い）。
    stability_penalty = max(0.0, f(nrec, "thr_p99_us") - f(nbase, "thr_p99_us"))
    score = (
        f(tuned, "durable_ack_p99_us")
        + 0.5 * f(tuned, "acc_p99_us")
        + 0.3 * f(stress, "durable_ack_p99_us")
        + 0.8 * stability_penalty
    )
    if not gate_ok:
        score += 1e9

    per_run.append({
        "profile": profile,
        "run": run_idx,
        "gate_ok": 1 if gate_ok else 0,
        "score": score,
        "tuned_acc_p99_us": f(tuned, "acc_p99_us"),
        "tuned_durable_ack_p99_us": f(tuned, "durable_ack_p99_us"),
        "stress_durable_ack_p99_us": f(stress, "durable_ack_p99_us"),
        "normal_base_thr_p99_us": f(nbase, "thr_p99_us"),
        "normal_recovery_thr_p99_us": f(nrec, "thr_p99_us"),
    })

profiles = sorted(set(r["profile"] for r in per_run))
summary = []
for p in profiles:
    g = [r for r in per_run if r["profile"] == p]
    summary.append({
        "profile": p,
        "runs": len(g),
        "gate_pass_rate": sum(r["gate_ok"] for r in g) / len(g),
        "score_median": statistics.median(r["score"] for r in g),
        "tuned_acc_p99_median": statistics.median(r["tuned_acc_p99_us"] for r in g),
        "tuned_durable_ack_p99_median": statistics.median(r["tuned_durable_ack_p99_us"] for r in g),
        "stress_durable_ack_p99_median": statistics.median(r["stress_durable_ack_p99_us"] for r in g),
        "recovery_thr_p99_median": statistics.median(r["normal_recovery_thr_p99_us"] for r in g),
    })

summary.sort(key=lambda x: (-x["gate_pass_rate"], x["score_median"]))

with open(out_csv, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow([
        "profile","runs","gate_pass_rate","score_median",
        "tuned_acc_p99_median","tuned_durable_ack_p99_median",
        "stress_durable_ack_p99_median","recovery_thr_p99_median"
    ])
    for r in summary:
        w.writerow([
            r["profile"], r["runs"], f"{r['gate_pass_rate']:.2f}", f"{r['score_median']:.2f}",
            f"{r['tuned_acc_p99_median']:.2f}", f"{r['tuned_durable_ack_p99_median']:.2f}",
            f"{r['stress_durable_ack_p99_median']:.2f}", f"{r['recovery_thr_p99_median']:.2f}",
        ])

print("WROTE", out_csv)
for r in summary:
    print(r)
PY

python3 - "$SUMMARY_CSV" "$SUMMARY_MD" <<'PY'
import csv
import sys

inp, out = sys.argv[1], sys.argv[2]
rows = list(csv.DictReader(open(inp)))
lines = []
lines.append("| profile | runs | gate_pass_rate | score_median | tuned_acc_p99 | tuned_durable_ack_p99 | stress_durable_ack_p99 | recovery_thr_p99 |")
lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
for r in rows:
    lines.append(
        f"| {r['profile']} | {r['runs']} | {r['gate_pass_rate']} | {r['score_median']} | {r['tuned_acc_p99_median']} | {r['tuned_durable_ack_p99_median']} | {r['stress_durable_ack_p99_median']} | {r['recovery_thr_p99_median']} |"
    )
open(out, "w").write("\n".join(lines) + "\n")
print("\n".join(lines))
PY

echo "SUMMARY_CSV=${SUMMARY_CSV}"
echo "SUMMARY_MD=${SUMMARY_MD}"
