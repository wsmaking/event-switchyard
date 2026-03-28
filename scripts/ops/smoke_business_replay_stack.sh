#!/usr/bin/env zsh
set -euo pipefail

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

wait_health "gateway-rust" "http://localhost:8081/health"
wait_health "oms-java" "http://localhost:18081/health"
wait_health "backoffice-java" "http://localhost:18082/health"
wait_health "app-java" "http://localhost:8080/health"

echo "[run] submit real order through app-java -> gateway-rust"
real_response="$(curl -fsS -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' \
  -d '{"symbol":"7203","side":"BUY","type":"MARKET","quantity":100,"price":null,"timeInForce":"GTC","expireAt":null}')"
echo "${real_response}"
real_order_id="$(printf '%s' "${real_response}" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')"
sleep 2

echo "[check] real order final-out"
curl -fsS "http://localhost:8080/api/orders/${real_order_id}/final-out"
echo

echo "[run] replay filled scenario"
scenario_response="$(curl -fsS -X POST http://localhost:8080/api/demo/scenarios/filled/run \
  -H 'Content-Type: application/json' \
  -d '{"symbol":"6758","side":"BUY","type":"LIMIT","quantity":120,"price":13500,"timeInForce":"GTC","expireAt":null}')"
echo "${scenario_response}"
scenario_order_id="$(printf '%s' "${scenario_response}" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')"
sleep 2

echo "[check] replay final-out"
curl -fsS "http://localhost:8080/api/orders/${scenario_order_id}/final-out"
echo

echo "[check] ops overview"
curl -fsS "http://localhost:8080/api/ops/overview?orderId=${scenario_order_id}"
echo

echo "[ok] business smoke completed"
