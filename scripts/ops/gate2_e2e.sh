#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${ROOT_DIR}"

compose() {
  if command -v docker-compose >/dev/null 2>&1; then
    docker-compose "$@"
  else
    docker compose "$@"
  fi
}

cleanup() {
  compose --profile gateway down >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "==> Gate2 E2E: starting gateway stack"
compose --profile gateway up -d --build

echo "==> Waiting for gateway/backoffice health"
READY=0
for _ in $(seq 1 60); do
  if curl -s "http://localhost:8081/health" >/dev/null 2>&1 && \
     curl -s "http://localhost:8082/health" >/dev/null 2>&1; then
    READY=1
    break
  fi
  sleep 1
done
if [[ "${READY}" != "1" ]]; then
  echo "ERROR: gateway/backoffice not ready" >&2
  compose --profile gateway ps || true
  exit 1
fi

echo "==> Running gateway-backoffice e2e"
scripts/ops/gateway_backoffice_e2e.sh

echo "==> Fault injection: stop kafka and verify accept"
compose --profile gateway stop kafka

JWT_SECRET="${JWT_HS256_SECRET:-dev-secret-change-me}"
ACCOUNT_ID="${ACCOUNT_ID:-acct_demo}"
export JWT_SECRET ACCOUNT_ID
token="$(
  python3 - <<'PY'
import base64, json, hmac, hashlib, os, time

def b64url(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode().rstrip("=")

secret = os.environ["JWT_SECRET"].encode()
header = {"alg":"HS256","typ":"JWT"}
payload = {"accountId": os.environ["ACCOUNT_ID"], "exp": int(time.time()) + 3600}

msg = f"{b64url(json.dumps(header, separators=(',',':')).encode())}.{b64url(json.dumps(payload, separators=(',',':')).encode())}".encode()
sig = hmac.new(secret, msg, hashlib.sha256).digest()
print(msg.decode() + "." + b64url(sig))
PY
)"

fault_order_json="$(curl -fsS "http://localhost:8081/orders" \
  -H "Authorization: Bearer $token" \
  -H "Content-Type: application/json" \
  -d "{\"symbol\":\"BTC\",\"side\":\"BUY\",\"type\":\"LIMIT\",\"qty\":1,\"price\":101,\"clientOrderId\":\"fault-1\"}")"

FAULT_ORDER_JSON="$fault_order_json" python3 - <<'PY'
import json
import os
import sys

raw = os.environ.get("FAULT_ORDER_JSON", "")
try:
    data = json.loads(raw)
except Exception:
    print("fault order: invalid JSON", file=sys.stderr)
    sys.exit(1)

order_id = data.get("orderId")
status = data.get("status")
if not order_id:
    print("fault order: missing orderId", file=sys.stderr)
    sys.exit(1)
if status and status != "ACCEPTED":
    print(f"fault order: unexpected status {status}", file=sys.stderr)
    sys.exit(1)
print(f"fault order: accepted orderId={order_id}")
PY

echo "==> Restarting kafka"
compose --profile gateway start kafka
