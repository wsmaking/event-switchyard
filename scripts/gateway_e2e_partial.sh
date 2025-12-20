#!/usr/bin/env bash
set -euo pipefail

GATEWAY_URL="${GATEWAY_URL:-http://localhost:8081}"
JWT_SECRET="${JWT_HS256_SECRET:-dev-secret-change-me}"
ACCOUNT_ID="${ACCOUNT_ID:-acct_demo}"
SYMBOL="${SYMBOL:-BTC}"
QTY="${QTY:-5}"
PRICE="${PRICE:-100}"
WAIT_SEC="${WAIT_SEC:-2}"
EVENT_LIMIT="${EVENT_LIMIT:-50}"
ENABLE_SSE="${ENABLE_SSE:-0}"

token="$(
  JWT_SECRET="$JWT_SECRET" ACCOUNT_ID="$ACCOUNT_ID" python3 - <<'PY'
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

echo "==> Waiting for gateway /health"
ready=0
for _ in $(seq 1 30); do
  if curl -fsS "$GATEWAY_URL/health" >/dev/null; then
    ready=1
    break
  fi
  sleep 0.5
done
if [ "$ready" -ne 1 ]; then
  echo "Gateway is not ready at $GATEWAY_URL" >&2
  exit 1
fi

echo "==> POST /orders (partial fill expected)"
order_json="$(curl -fsS "$GATEWAY_URL/orders" \
  -H "Authorization: Bearer $token" \
  -H "Content-Type: application/json" \
  -d "{\"symbol\":\"$SYMBOL\",\"side\":\"BUY\",\"type\":\"LIMIT\",\"qty\":$QTY,\"price\":$PRICE,\"clientOrderId\":\"c-partial\"}")"
echo "$order_json"

order_id="$(printf '%s' "$order_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["orderId"])')"

tmp_sse=""
sse_pid=""
if [ "$ENABLE_SSE" = "1" ]; then
  tmp_sse="$(mktemp)"
  curl -fsS -N "$GATEWAY_URL/orders/$order_id/stream" \
    -H "Authorization: Bearer $token" > "$tmp_sse" &
  sse_pid=$!
fi

echo "==> Waiting for partial fills (${WAIT_SEC}s)"
sleep "$WAIT_SEC"

echo "==> GET /orders/$order_id/events"
events_json="$(curl -fsS "$GATEWAY_URL/orders/$order_id/events?limit=$EVENT_LIMIT" \
  -H "Authorization: Bearer $token")"
echo "$events_json"

printf '%s' "$events_json" | python3 -c 'import json,sys; data=json.load(sys.stdin); events=data.get("events", []); reports=[e for e in events if e.get("type")=="ExecutionReport"]; print(f"events={len(events)} execution_reports={len(reports)}"); sys.exit(1) if len(reports) < 2 else None'

echo "==> GET /orders/$order_id"
curl -fsS "$GATEWAY_URL/orders/$order_id" \
  -H "Authorization: Bearer $token" | python3 -m json.tool

if [ "$ENABLE_SSE" = "1" ] && [ -n "$sse_pid" ]; then
  kill "$sse_pid" >/dev/null 2>&1 || true
  sleep 0.2
  echo "==> SSE output (truncated)"
  head -n 50 "$tmp_sse" || true
  rm -f "$tmp_sse"
fi
