#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/../.." && pwd)
APP_BASE_URL="${APP_JAVA_BASE_URL:-http://localhost:8080}"
ORDER_ID="${1:-}"

echo "[info] mobile operations incident drill"
echo "  app-java: ${APP_BASE_URL}"
if [[ -n "${ORDER_ID}" ]]; then
  echo "  orderId : ${ORDER_ID}"
fi

echo
echo "--- /api/mobile/operations ---"
curl -fsS "${APP_BASE_URL}/api/mobile/operations"

echo
echo
echo "--- /api/ops/overview ---"
if [[ -n "${ORDER_ID}" ]]; then
  curl -fsS "${APP_BASE_URL}/api/ops/overview?orderId=${ORDER_ID}"
else
  curl -fsS "${APP_BASE_URL}/api/ops/overview"
fi

echo
echo
echo "--- next questions ---"
echo "1. gap / pending / DLQ のどれが増えているか"
echo "2. reconcile issue は OMS 側か BackOffice 側か"
echo "3. final-out は利用者に何を見せているか"
echo "4. replay が必要か、reconcile だけで足りるか"
