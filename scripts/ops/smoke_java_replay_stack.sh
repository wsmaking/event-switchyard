#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

wait_health() {
  local name="$1"
  local url="$2"
  local attempts="${3:-20}"

  for ((i = 1; i <= attempts; i++)); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      echo "[ready] ${name}"
      return 0
    fi
    sleep 1
  done

  echo "[fail] ${name} is not healthy: ${url}" >&2
  exit 1
}

wait_health "oms-java" "http://localhost:18081/health"
wait_health "backoffice-java" "http://localhost:18082/health"
wait_health "app-java" "http://localhost:8080/health"

echo "[run] replay partial-fill scenario"
response="$(curl -fsS -X POST http://localhost:8080/api/demo/scenarios/partial-fill/run \
  -H 'Content-Type: application/json' \
  -d '{"symbol":"6758","side":"BUY","type":"LIMIT","quantity":120,"price":13500,"timeInForce":"GTC","expireAt":null}')"
echo "${response}"

echo "[check] orders"
curl -fsS http://localhost:8080/api/orders
echo

echo "[check] account overview"
curl -fsS http://localhost:8080/api/accounts/acct_demo/overview
echo

echo "[check] positions"
curl -fsS http://localhost:8080/api/positions
echo

echo "[check] latest final-out"
order_id="$(printf '%s' "${response}" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')"
curl -fsS "http://localhost:8080/api/orders/${order_id}/final-out"
echo

echo "[ok] smoke completed"
