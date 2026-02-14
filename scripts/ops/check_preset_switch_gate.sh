#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

echo "==> running preset switch validation scenario"
scripts/ops/run_preset_switch_validation.sh

latest_csv="$(ls -t var/results/preset_switch_raw_*.csv | head -n1)"
echo "==> evaluating gate: ${latest_csv}"

python3 - "$latest_csv" <<'PY'
import csv
import sys

path = sys.argv[1]
rows = list(csv.DictReader(open(path)))
by_phase = {r["phase"]: r for r in rows}

required = ["normal_baseline", "degraded_injected", "normal_recovery"]
for p in required:
    if p not in by_phase:
        print(f"FAIL: missing phase {p}")
        sys.exit(1)

base = by_phase["normal_baseline"]
deg = by_phase["degraded_injected"]
rec = by_phase["normal_recovery"]

def f(row, key):
    return float(row[key])

checks = []
checks.append(("baseline queue_full == 0", f(base, "queue_full_total") == 0))
checks.append(("degraded queue_full == 0", f(deg, "queue_full_total") == 0))
checks.append(("recovery queue_full == 0", f(rec, "queue_full_total") == 0))
checks.append(("degraded wal_age_ms >= 550", f(deg, "wal_age_ms") >= 550))
checks.append(("degraded limit_dynamic < baseline", f(deg, "limit_dynamic") < f(base, "limit_dynamic")))
checks.append(("recovery wal_age_ms <= 200", f(rec, "wal_age_ms") <= 200))
checks.append(("recovery rejected <= baseline rejected", f(rec, "rejected") <= f(base, "rejected")))

failed = [name for name, ok in checks if not ok]
for name, ok in checks:
    print(f"{'PASS' if ok else 'FAIL'}: {name}")

if failed:
    print("Gate FAILED")
    sys.exit(1)

print("Gate PASSED")
PY
