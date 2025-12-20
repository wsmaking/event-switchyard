#!/usr/bin/env bash
set -euo pipefail

GATEWAY_URL="${GATEWAY_URL:-http://localhost:8081}"
BACKOFFICE_URL="${BACKOFFICE_URL:-http://localhost:8082}"
JWT_SECRET="${JWT_HS256_SECRET:-dev-secret-change-me}"
ACCOUNT_ID="${ACCOUNT_ID:-acct_demo}"
SYMBOL="${SYMBOL:-BTC}"

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
  -d "{\"symbol\":\"$SYMBOL\",\"side\":\"BUY\",\"type\":\"LIMIT\",\"qty\":1,\"price\":100.0,\"clientOrderId\":\"c1\"}")"
echo "$order_json"

echo "==> Waiting for execution (2s)"
sleep 2

echo "==> GET /positions (backoffice)"
curl -fsS "$BACKOFFICE_URL/positions" \
  -H "Authorization: Bearer $token" | python3 -m json.tool
