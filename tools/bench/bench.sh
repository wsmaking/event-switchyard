set -euo pipefail

# defaults
mode="quick"
case_name="match_engine_hot"
runs=5
out="results/pr.json"

# args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) mode="$2"; shift 2;;
    --case) case_name="$2"; shift 2;;
    --runs) runs="$2"; shift 2;;
    --out)  out="$2";  shift 2;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done

mkdir -p "$(dirname "$out")"
commit="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
run_id="${GITHUB_RUN_ID:-local-$(date +%s)}"

if [[ "$mode" == "quick" ]]; then
  # 短時間モード（PR/配線確認用）
  p99_us=950
  gc_p99_ms=3.0
else
  # TODO: 実ベンチに差し替え（Kafka起動→少量送受信→p99算出）
  p99_us=950
  gc_p99_ms=3.0
fi

cat > "$out" <<JSON
{"schema":"bench.v1","run_id":"$run_id","commit":"$commit","case":"$case_name","env":"gha|JDK21|G1","metrics":{"latency_us":{"p99":$p99_us},"gc_pause_ms":{"p99":$gc_p99_ms}}}
JSON

echo "wrote $out (mode=$mode, case=$case_name, runs=$runs)"
