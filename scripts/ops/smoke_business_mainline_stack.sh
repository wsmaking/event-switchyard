#!/usr/bin/env zsh
set -euo pipefail

APP_BASE_URL="${APP_BASE_URL:-http://localhost:8080}"
OMS_BASE_URL="${OMS_BASE_URL:-http://localhost:18081}"
BACKOFFICE_BASE_URL="${BACKOFFICE_BASE_URL:-http://localhost:18082}"

echo "[check] health"
curl -fsS "${APP_BASE_URL}/health" >/dev/null
curl -fsS "${OMS_BASE_URL}/health" >/dev/null
curl -fsS "${BACKOFFICE_BASE_URL}/health" >/dev/null

echo "[check] ops overview"
ops_json="$(curl -fsS "${APP_BASE_URL}/api/ops/overview")"
echo "${ops_json}"

echo "[check] mainline ops gates"
"$(cd "$(dirname "$0")" && pwd)/check_business_mainline_ops.sh"

echo "[check] submit order"
submit_json="$(curl -fsS -X POST "${APP_BASE_URL}/api/orders" \
  -H 'content-type: application/json' \
  -d '{"symbol":"7203","side":"BUY","type":"LIMIT","qty":10,"price":2800,"timeInForce":"GTC"}')"
echo "${submit_json}"
order_id="$(printf '%s' "${submit_json}" | sed -n 's/.*"orderId":"\([^"]*\)".*/\1/p')"

if [[ -z "${order_id}" ]]; then
  echo "[fail] orderId not found in submit response" >&2
  exit 1
fi

echo "[check] final out orderId=${order_id}"
for _ in $(seq 1 30); do
  final_out_json="$(curl -fsS "${APP_BASE_URL}/api/orders/${order_id}/final-out")"
  if [[ "${final_out_json}" == *'"timeline"'* ]]; then
    echo "${final_out_json}"
    exit 0
  fi
  sleep 1
done

echo "[fail] final-out timeout orderId=${order_id}" >&2
exit 1
