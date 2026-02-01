#!/usr/bin/env bash
set -euo pipefail

GATEWAY_URL="${GATEWAY_URL:-http://localhost:8081}"
GATEWAY_ACCOUNT_ID="${GATEWAY_ACCOUNT_ID:-acct_demo}"
JWT_SECRET="${JWT_HS256_SECRET:-}"
GATEWAY_JWT="${GATEWAY_JWT:-}"
FROM_SEQ="${AUDIT_VERIFY_FROM_SEQ:-0}"
LIMIT="${AUDIT_VERIFY_LIMIT:-1000}"
PASS_FAIL="${AUDIT_VERIFY_PASSFAIL:-1}"

if [[ -z "$GATEWAY_JWT" && -n "$JWT_SECRET" && -n "$GATEWAY_ACCOUNT_ID" ]]; then
  GATEWAY_JWT="$(
    JWT_SECRET="$JWT_SECRET" ACCOUNT_ID="$GATEWAY_ACCOUNT_ID" python3 - <<'PY'
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

if [[ -z "$GATEWAY_JWT" ]]; then
  echo "GATEWAY_JWT or JWT_HS256_SECRET + GATEWAY_ACCOUNT_ID required" >&2
  exit 2
fi

if [[ "${PASS_FAIL}" == "1" ]]; then
  trap 'echo "FAIL" >&2' ERR
fi

verify_url="${GATEWAY_URL}/audit/verify?fromSeq=${FROM_SEQ}&limit=${LIMIT}"
anchor_url="${GATEWAY_URL}/audit/anchor"

echo "==> Audit hash-chain verify"
echo "url:   ${verify_url}"

curl -fsS -H "Authorization: Bearer ${GATEWAY_JWT}" "$verify_url" \
  | python3 - <<'PY'
import json, sys
data = json.load(sys.stdin)
if not data.get("ok", False):
    print(f"audit verify: FAIL error={data.get('error')}", file=sys.stderr)
    sys.exit(1)
print(f"audit verify: PASS checked={data.get('checked')} lastSeq={data.get('lastSeq')}")
PY

echo "==> Audit anchor snapshot"
curl -fsS -H "Authorization: Bearer ${GATEWAY_JWT}" "$anchor_url" \
  | python3 - <<'PY'
import json, sys
data = json.load(sys.stdin)
print(f"anchor: seq={data.get('seq')} keyId={data.get('keyId')} hash={data.get('hash')}")
PY

if [[ "${PASS_FAIL}" == "1" ]]; then
  echo "PASS"
fi
