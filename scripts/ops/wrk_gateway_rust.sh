#!/usr/bin/env bash
set -euo pipefail

HOST="${HOST:-localhost}"
PORT="${PORT:-8081}"
DURATION="${DURATION:-10}"
CONNECTIONS="${CONNECTIONS:-400}"
THREADS="${THREADS:-8}"
LATENCY="${LATENCY:-0}"
ACCOUNT_ID="${ACCOUNT_ID:-12345}"
JWT_SECRET="${JWT_HS256_SECRET:-secret123}"
CLIENT_ID_LEN="${CLIENT_ID_LEN:-0}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LUA_SCRIPT="${SCRIPT_DIR}/wrk_orders.lua"
URL="http://${HOST}:${PORT}/orders"

if [[ -z "${JWT_TOKEN:-}" ]]; then
  JWT_TOKEN="$(
    python3 - <<'PY'
import hmac, hashlib, base64, json, os, time
secret = os.environ.get("JWT_SECRET", "secret123")
account_id = os.environ.get("ACCOUNT_ID", "12345")
now = int(time.time())
header = {"alg": "HS256", "typ": "JWT"}
payload = {"sub": f"user_{account_id}", "account_id": account_id, "iat": now, "exp": now + 86400}
def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()
h = b64url(json.dumps(header).encode())
p = b64url(json.dumps(payload).encode())
msg = f"{h}.{p}"
sig = hmac.new(secret.encode(), msg.encode(), hashlib.sha256).digest()
s = b64url(sig)
print(f"{h}.{p}.{s}")
PY
  )"
fi

echo "wrk gateway-rust"
echo "  url=${URL}"
echo "  duration=${DURATION}s threads=${THREADS} connections=${CONNECTIONS} latency=${LATENCY}"
echo "  accountId=${ACCOUNT_ID} clientIdLen=${CLIENT_ID_LEN}"

JWT_SECRET="${JWT_SECRET}" \
ACCOUNT_ID="${ACCOUNT_ID}" \
JWT_TOKEN="${JWT_TOKEN}" \
CLIENT_ID_LEN="${CLIENT_ID_LEN}" \
WRK_ARGS=( -t "${THREADS}" -c "${CONNECTIONS}" -d "${DURATION}s" -s "${LUA_SCRIPT}" )
if [[ "${LATENCY}" == "1" ]]; then
  WRK_ARGS+=( --latency )
fi
wrk "${WRK_ARGS[@]}" "${URL}"
