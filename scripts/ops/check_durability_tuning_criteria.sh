#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

echo "==> run durability tuning scan"
scripts/ops/run_durability_tuning_scan.sh >/tmp/durability_tuning_scan_latest.log 2>&1

latest_csv="$(ls -t var/results/durability_tuning_scan_*.csv | head -n1)"
echo "==> evaluate criteria: ${latest_csv}"

CAND_CASE="${CAND_CASE:-tuned_w150_b64}"
echo "==> candidate case: ${CAND_CASE}"

python3 - "$latest_csv" "$CAND_CASE" <<'PY'
import csv
import sys

path = sys.argv[1]
cand_case = sys.argv[2]
rows = list(csv.DictReader(open(path)))
by = {r["case"]: r for r in rows}

required = ["baseline_w200_b64", cand_case]
for k in required:
    if k not in by:
        print(f"FAIL: missing case {k}")
        sys.exit(1)

base = by["baseline_w200_b64"]
cand = by[cand_case]

def f(r, k):
    return float(r[k])

checks = []
checks.append(("baseline queue_full == 0", f(base, "queue_full_total") == 0))
checks.append(("candidate queue_full == 0", f(cand, "queue_full_total") == 0))
base_rej_rate = f(base, "rejected") / max(f(base, "total"), 1.0)
cand_rej_rate = f(cand, "rejected") / max(f(cand, "total"), 1.0)
checks.append(("baseline rejected_rate <= 1.0%", base_rej_rate <= 0.01))
checks.append(("candidate rejected_rate <= baseline rejected_rate", cand_rej_rate <= base_rej_rate))
checks.append(("candidate acc_p99 improves >=5%", f(cand, "acc_p99_us") <= f(base, "acc_p99_us") * 0.95))
checks.append(("candidate durable_ack_p99 <= baseline * 1.10", f(cand, "durable_ack_p99_us") <= f(base, "durable_ack_p99_us") * 1.10))

failed = [name for name, ok in checks if not ok]
for name, ok in checks:
    print(f"{'PASS' if ok else 'FAIL'}: {name}")

print(f"baseline acc_p99_us={f(base, 'acc_p99_us'):.2f}, durable_ack_p99_us={f(base, 'durable_ack_p99_us'):.2f}")
print(f"candidate({cand_case}) acc_p99_us={f(cand, 'acc_p99_us'):.2f}, durable_ack_p99_us={f(cand, 'durable_ack_p99_us'):.2f}")
print(f"baseline rejected_rate={base_rej_rate*100:.3f}%, candidate rejected_rate={cand_rej_rate*100:.3f}%")

if failed:
    print("Durability criteria FAILED")
    sys.exit(1)

print("Durability criteria PASSED")
PY
