#!/usr/bin/env zsh
set -euo pipefail

APP_BASE_URL="${APP_BASE_URL:-http://localhost:8080}"
ALLOW_CAUTION="${ALLOW_CAUTION:-0}"

echo "[check] go/no-go ${APP_BASE_URL}/api/ops/go-no-go"
payload="$(curl -fsS "${APP_BASE_URL}/api/ops/go-no-go")"
echo "${payload}"

if [[ "${payload}" == *'"state":"GO"'* ]]; then
  echo "[ok] go/no-go=GO"
  exit 0
fi

if [[ "${ALLOW_CAUTION}" == "1" && "${payload}" == *'"state":"CAUTION"'* ]]; then
  echo "[warn] go/no-go=CAUTION"
  exit 0
fi

echo "[fail] go/no-go is not acceptable" >&2
exit 1
