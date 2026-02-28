#!/usr/bin/env bash
set -euo pipefail

# 既定パス（必要ならここだけ直せば全員同じ条件で走る）
BENCH="${1:-var/results/pr.json}"
REQ="${REQ:-docs/specs/perf.md}"
BASELINE="${BASELINE:-baseline/seed.json}"

python tools/gate/gate.py \
  --bench "$BENCH" \
  --req   "$REQ" \
  --baseline "$BASELINE"

# 成果が良かったとき、BLESS=1 でベースラインを更新
if [[ "${BLESS:-0}" == "1" ]]; then
  echo "[gate] bless: updating baseline -> $BASELINE"
  cp -f "$BENCH" "$BASELINE"
fi
