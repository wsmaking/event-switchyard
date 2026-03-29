#!/usr/bin/env zsh
set -euo pipefail

APP_BASE_URL="${APP_BASE_URL:-http://localhost:8080}"
ITERATIONS="${ITERATIONS:-20}"
SCENARIOS=("filled" "partial-fill" "canceled" "expired" "rejected")

payload='{"symbol":"7203","side":"BUY","type":"MARKET","quantity":100,"price":null,"timeInForce":"GTC","expireAt":null}'

echo "[soak] iterations=${ITERATIONS}"

for ((i = 1; i <= ITERATIONS; i++)); do
  scenario="${SCENARIOS[$((((i - 1) % ${#SCENARIOS[@]})) + 1)]}"
  echo "[run] iteration=${i} scenario=${scenario}"
  curl -fsS -X POST "${APP_BASE_URL}/api/demo/scenarios/${scenario}/run" \
    -H 'Content-Type: application/json' \
    -d "${payload}" >/dev/null

  go_no_go="$(curl -fsS "${APP_BASE_URL}/api/ops/go-no-go")"
  echo "${go_no_go}"
  if [[ "${go_no_go}" == *'"state":"NO_GO"'* ]]; then
    echo "[fail] go/no-go turned NO_GO at iteration=${i}" >&2
    exit 1
  fi
done

echo "[ok] soak completed"
