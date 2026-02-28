#!/usr/bin/env bash
set -euo pipefail

GATEWAY_URL="${GATEWAY_URL:-http://localhost:8081}"
ACCOUNT_ID="${ACCOUNT_ID:-acct_demo}"
JWT_SECRET="${JWT_HS256_SECRET:-}"
JWT_TOKEN="${GATEWAY_JWT:-}"
TIMEOUT_SEC="${KAFKA_SMOKE_TIMEOUT_SEC:-30}"
POLL_SEC="${KAFKA_SMOKE_POLL_SEC:-2}"

if [[ -z "$JWT_TOKEN" && -n "$JWT_SECRET" ]]; then
  JWT_TOKEN="$(
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
fi

if [[ -z "$JWT_TOKEN" ]]; then
  echo "GATEWAY_JWT or JWT_HS256_SECRET required" >&2
  exit 2
fi

echo "==> Kafka smoke check"
echo "gateway: $GATEWAY_URL"

order_json="$(curl -fsS "${GATEWAY_URL}/orders" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"symbol\":\"BTC\",\"side\":\"BUY\",\"type\":\"LIMIT\",\"qty\":1,\"price\":100,\"clientOrderId\":\"kafka-smoke\"}")"

ORDER_JSON="$order_json" python3 - <<'PY'
import json, os, sys
data = json.loads(os.environ.get("ORDER_JSON", ""))
if not data.get("orderId"):
    print("order missing orderId", file=sys.stderr)
    sys.exit(1)
status = data.get("status")
if status and status != "ACCEPTED":
    print(f"order unexpected status {status}", file=sys.stderr)
    sys.exit(1)
PY

deadline=$(( $(date +%s) + TIMEOUT_SEC ))
while [[ "$(date +%s)" -lt "$deadline" ]]; do
  metrics="$(curl -fsS "${GATEWAY_URL}/metrics" || true)"
  enabled="$(printf '%s\n' "$metrics" | awk '/gateway_kafka_publisher_enabled/{print $2}' | tail -n 1)"
  depth="$(printf '%s\n' "$metrics" | awk '/gateway_kafka_publisher_queue_depth/{print $2}' | tail -n 1)"
  errors="$(printf '%s\n' "$metrics" | awk '/gateway_kafka_publisher_errors_total/{print $2}' | tail -n 1)"
  if [[ "$enabled" == "1" && "$depth" == "0" && "$errors" == "0" ]]; then
    echo "kafka smoke: PASS (queue_depth=0, errors=0)"
    exit 0
  fi
  sleep "$POLL_SEC"
done

echo "kafka smoke: FAIL (enabled=$enabled depth=$depth errors=$errors)" >&2
exit 1
