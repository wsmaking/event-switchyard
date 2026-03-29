#!/usr/bin/env zsh
set -euo pipefail

APP_URL="${APP_URL:-http://localhost:8080}"
GATEWAY_URL="${GATEWAY_URL:-http://localhost:8081}"
OMS_URL="${OMS_URL:-http://localhost:18081}"
BACKOFFICE_URL="${BACKOFFICE_URL:-http://localhost:18082}"
ACCOUNT_ID="${ACCOUNT_ID:-acct_demo}"

wait_health() {
  local name="$1"
  local url="$2"
  local attempts="${3:-30}"

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

extract_order_id() {
  sed -n 's/.*"id":"\([^"]*\)".*/\1/p'
}

wait_health "gateway-rust" "${GATEWAY_URL}/health"
wait_health "oms-java" "${OMS_URL}/health"
wait_health "backoffice-java" "${BACKOFFICE_URL}/health"
wait_health "app-java" "${APP_URL}/health"

echo "[run] submit real market order"
submit_response="$(curl -fsS -X POST "${APP_URL}/api/orders" \
  -H 'Content-Type: application/json' \
  -d '{"symbol":"7203","side":"BUY","type":"MARKET","quantity":100,"price":null,"timeInForce":"GTC","expireAt":null}')"
echo "${submit_response}"

order_id="$(printf '%s' "${submit_response}" | extract_order_id)"
if [[ -z "${order_id}" ]]; then
  echo "[fail] order id not found in submit response" >&2
  exit 1
fi

echo "[wait] order final-out orderId=${order_id}"
final_out=""
for ((i = 1; i <= 40; i++)); do
  final_out="$(curl -fsS "${APP_URL}/api/orders/${order_id}/final-out" || true)"
  if [[ "${final_out}" == *'"status":"FILLED"'* && "${final_out}" == *'"fills":[{'* ]]; then
    break
  fi
  sleep 1
done

if [[ "${final_out}" != *'"status":"FILLED"'* || "${final_out}" != *'"fills":[{'* ]]; then
  echo "[fail] order did not reach FILLED with backoffice fills attached" >&2
  echo "${final_out}" >&2
  exit 1
fi

echo "[check] final-out"
echo "${final_out}"
echo

echo "[check] app-java order detail"
curl -fsS "${APP_URL}/api/orders/${order_id}"
echo

echo "[check] oms order events"
curl -fsS "${OMS_URL}/orders/${order_id}/events"
echo

echo "[check] backoffice overview"
curl -fsS "${BACKOFFICE_URL}/accounts/${ACCOUNT_ID}/overview"
echo

echo "[check] backoffice positions"
curl -fsS "${BACKOFFICE_URL}/positions?accountId=${ACCOUNT_ID}"
echo

echo "[check] backoffice fills"
curl -fsS "${BACKOFFICE_URL}/fills?orderId=${order_id}"
echo

echo "[check] backoffice ledger"
curl -fsS "${BACKOFFICE_URL}/ledger?accountId=${ACCOUNT_ID}&orderId=${order_id}&limit=20"
echo

echo "[ok] business debug smoke completed orderId=${order_id}"
