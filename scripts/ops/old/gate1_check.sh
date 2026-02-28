#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${ROOT_DIR}"

OUT="${OUT:-var/results/pr.json}"
MODE="${MODE:-quick}"
RUNS="${RUNS:-1}"
CASE="${CASE:-match_engine_hot}"
DIFF_BASE="${DIFF_BASE:-main}"
RISK_OUT="${RISK_OUT:-var/results/change_risk.json}"
RUNS_HIGH="${RUNS_HIGH:-2000}"
export RISK_OUT

python3 scripts/change_risk.py --diff-base "${DIFF_BASE}" --out "${RISK_OUT}"

risk_level="$(
  python3 - <<'PY'
import json, os
path = os.environ.get("RISK_OUT")
with open(path, "r") as f:
    data = json.load(f)
print(data.get("risk_level", "low"))
PY
)"

echo "==> Change risk: ${risk_level}"
if [[ "${risk_level}" == "high" || "${risk_level}" == "critical" ]]; then
  MODE="full"
  RUNS="${RUNS_HIGH}"
fi

OUT="${OUT}" MODE="${MODE}" RUNS="${RUNS}" CASE="${CASE}" tools/bench/run.sh

tools/gate/run.sh "${OUT}"
