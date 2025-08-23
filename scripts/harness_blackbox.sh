#!/usr/bin/env bash
set -euo pipefail

case_name="blackbox_cmd"
runs=50
out="results/pr.json"
cmd=""
env_str="gha|JDK21|G1"

usage() {
  cat <<'USAGE'
Usage: scripts/harness_blackbox.sh --cmd 'echo test | sha256sum >/dev/null' [--case NAME] [--runs N] [--out PATH]
USAGE
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --case) case_name="$2"; shift 2;;
    --runs) runs="$2"; shift 2;;
    --out)  out="$2";  shift 2;;
    --cmd)  cmd="$2";  shift 2;;
    -h|--help) usage;;
    *) echo "unknown arg: $1"; usage;;
  esac
done

[[ -n "$cmd" ]] || { echo "need --cmd"; exit 2; }

mkdir -p "$(dirname "$out")"
commit="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"

python3 scripts/measure_cmd.py --cmd "$cmd" --runs "$runs" --name "$case_name" --out results/trace.ndjson
python3 scripts/agg_to_bench.py --trace results/trace.ndjson --case "$case_name" --env "$env_str" --commit "$commit" --out "$out"

echo "wrote $out (blackbox)"
