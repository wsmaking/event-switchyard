#!/usr/bin/env bash
set -euo pipefail

GATEWAY_URL="${GATEWAY_URL:-http://localhost:8081}"
BACKOFFICE_URL="${BACKOFFICE_URL:-http://localhost:8082}"
JWT_SECRET="${JWT_HS256_SECRET:-dev-secret-change-me}"
ACCOUNT_ID="${ACCOUNT_ID:-acct_demo}"
SYMBOL="${SYMBOL:-BTC}"
SIDE="${SIDE:-BUY}"
ORDER_TYPE="${ORDER_TYPE:-LIMIT}"
QTY="${QTY:-1}"
PRICE="${PRICE:-100}"
CLIENT_ORDER_ID="${CLIENT_ORDER_ID:-c1}"
WAIT_SEC="${WAIT_SEC:-2}"

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

echo "==> POST /orders (gateway)"
order_json="$(curl -fsS "$GATEWAY_URL/orders" \
  -H "Authorization: Bearer $token" \
  -H "Content-Type: application/json" \
  -d "{\"symbol\":\"$SYMBOL\",\"side\":\"$SIDE\",\"type\":\"$ORDER_TYPE\",\"qty\":$QTY,\"price\":$PRICE,\"clientOrderId\":\"$CLIENT_ORDER_ID\"}")"
echo "$order_json"

ORDER_JSON="$order_json" python3 - <<'PY'
import json
import os
import sys

raw = os.environ.get("ORDER_JSON", "")
try:
    data = json.loads(raw)
except Exception:
    print("order: invalid JSON", file=sys.stderr)
    sys.exit(1)

order_id = data.get("orderId")
status = data.get("status")
if not order_id:
    print("order: missing orderId", file=sys.stderr)
    sys.exit(1)
if status and status != "ACCEPTED":
    print(f"order: unexpected status {status}", file=sys.stderr)
    sys.exit(1)
print(f"order: accepted orderId={order_id}")
PY

echo "==> Waiting for execution (${WAIT_SEC}s)"
sleep "$WAIT_SEC"

echo "==> BackOffice recovery check"
BACKOFFICE_URL="$BACKOFFICE_URL" \
BACKOFFICE_JWT="$token" \
BACKOFFICE_ACCOUNT_ID="$ACCOUNT_ID" \
  scripts/ops/backoffice_recovery_check.sh
