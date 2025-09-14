set -euo pipefail
# まず計測
OUT="${OUT:-var/results/pr.json}"
MODE="${MODE:-quick}"
RUNS="${RUNS:-1}"
CASE="${CASE:-match_engine_hot}"

tools/bench/run.sh    # OUT/MODE/RUNS/CASE は環境変数で渡せる

# つぎにゲート（必要なら BASELINE を上書き）
tools/gate/run.sh "$OUT"
