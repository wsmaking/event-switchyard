set -euo pipefail
OUT="${OUT:-var/results/pr.json}"
MODE="${MODE:-quick}"
RUNS="${RUNS:-1}"
CASE="${CASE:-match_engine_hot}"

# 既存の中身を呼ぶ（tools/bench/bench.sh はそのまま利用）
tools/bench/bench.sh --mode "$MODE" --case "$CASE" --runs "$RUNS" --out "$OUT"
