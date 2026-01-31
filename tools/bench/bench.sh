#!/usr/bin/env bash
set -euo pipefail

# デフォルト値
mode="quick"
case_name="match_engine_hot"
runs=5
out="results/pr.json"

# コマンドライン引数の解析
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) mode="$2"; shift 2;;
    --case) case_name="$2"; shift 2;;
    --runs) runs="$2"; shift 2;;
    --out)  out="$2";  shift 2;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done

# 出力ディレクトリ作成
mkdir -p "$(dirname "$out")"
commit="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
run_id="${GITHUB_RUN_ID:-local-$(date +%s)}"

if [[ "$mode" == "quick" ]]; then
  runs=500
fi

APP_PID=""
cleanup() {
  if [[ -n "${APP_PID}" ]]; then
    kill "${APP_PID}" >/dev/null 2>&1 || true
    wait "${APP_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "==> Starting app for bench (FAST_PATH_ENABLE=1)"
FAST_PATH_ENABLE=1 FAST_PATH_METRICS=1 ./gradlew :app:run >/tmp/bench_app.log 2>&1 &
APP_PID=$!

READY=0
for _ in $(seq 1 60); do
  if curl -s "http://localhost:8080/health" >/dev/null 2>&1; then
    READY=1
    break
  fi
  sleep 0.5
done
if [[ "${READY}" != "1" ]]; then
  echo "ERROR: app failed to start; see /tmp/bench_app.log" >&2
  exit 1
fi

tmp_json="$(dirname "$out")/orderbook_bench_tmp.json"
python3 scripts/bench_orderbook.py --runs "${runs}" --out "${tmp_json}" --endpoint "http://localhost:8080/events"

BENCH_OUT="$out" \
BENCH_CASE="$case_name" \
BENCH_COMMIT="$commit" \
BENCH_RUN_ID="$run_id" \
BENCH_TMP="$tmp_json" \
BENCH_MODE="$mode" \
BENCH_RUNS="$runs" \
python3 - <<'PY'
import json, os
out = os.environ.get("BENCH_OUT")
case_name = os.environ.get("BENCH_CASE")
commit = os.environ.get("BENCH_COMMIT")
tmp_json = os.environ.get("BENCH_TMP")
with open(tmp_json, "r") as f:
    data = json.load(f)
stats = data.get("stats", {})
p99_us = stats.get("fast_path_process_p99_us", 0)
gc_p99_ms = 0.0
payload = {
    "schema": "bench.v1",
    "run_id": os.environ.get("BENCH_RUN_ID"),
    "commit": commit,
    "case": case_name,
    "env": "gha|JDK21|G1",
    "metrics": {
        "latency_us": {"p99": p99_us},
        "gc_pause_ms": {"p99": gc_p99_ms}
    }
}
os.makedirs(os.path.dirname(out), exist_ok=True)
with open(out, "w") as f:
    json.dump(payload, f)
print(f"wrote {out} (mode={os.environ.get('BENCH_MODE')}, case={case_name}, runs={os.environ.get('BENCH_RUNS')})")
PY
