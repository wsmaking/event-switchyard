#!/usr/bin/env bash
set -euo pipefail

LEDGER_PATH="${BACKOFFICE_LEDGER_PATH:-var/backoffice/ledger.log}"
BACKOFFICE_PORT="${BACKOFFICE_PORT:-8082}"
BACKOFFICE_URL="${BACKOFFICE_URL:-http://localhost:${BACKOFFICE_PORT}}"
BACKOFFICE_ACCOUNT_ID="${BACKOFFICE_ACCOUNT_ID:-}"
JWT_SECRET="${JWT_HS256_SECRET:-}"
BACKOFFICE_JWT="${BACKOFFICE_JWT:-}"
WAIT_SEC="${BACKOFFICE_REPLAY_WAIT_SEC:-3}"
PASS_FAIL="${BACKOFFICE_PASSFAIL:-1}"

if [[ ! -f "$LEDGER_PATH" ]]; then
  echo "ledger not found: $LEDGER_PATH" >&2
  exit 2
fi

if [[ -z "$BACKOFFICE_JWT" && -n "$JWT_SECRET" && -n "$BACKOFFICE_ACCOUNT_ID" ]]; then
  BACKOFFICE_JWT="$(
    JWT_SECRET="$JWT_SECRET" ACCOUNT_ID="$BACKOFFICE_ACCOUNT_ID" python3 - <<'PY'
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

if [[ -z "$BACKOFFICE_JWT" ]]; then
  echo "BACKOFFICE_JWT or JWT_HS256_SECRET + BACKOFFICE_ACCOUNT_ID required" >&2
  exit 2
fi

echo "==> BackOffice replay verification"
echo "ledger: $LEDGER_PATH"
echo "url:    $BACKOFFICE_URL"
echo "wait:   ${WAIT_SEC}s"

echo "==> Starting backoffice (fresh replay)"
BACKOFFICE_LEDGER_PATH="$LEDGER_PATH" BACKOFFICE_PORT="$BACKOFFICE_PORT" ./gradlew :backoffice:run >/tmp/backoffice_replay.log 2>&1 &
pid=$!
trap 'kill "$pid" >/dev/null 2>&1 || true' EXIT
if [[ "${PASS_FAIL}" == "1" ]]; then
  trap 'echo "FAIL" >&2; kill "$pid" >/dev/null 2>&1 || true' ERR
fi

sleep "$WAIT_SEC"

echo "==> Reconcile"
account_qs=""
if [[ -n "${BACKOFFICE_ACCOUNT_ID:-}" ]]; then
  account_qs="accountId=${BACKOFFICE_ACCOUNT_ID}"
fi
reconcile_url="${BACKOFFICE_URL}/reconcile"
if [[ -n "$account_qs" ]]; then
  reconcile_url="${reconcile_url}?${account_qs}"
fi
curl -fsS -H "Authorization: Bearer ${BACKOFFICE_JWT}" \
  "$reconcile_url" \
  | python3 - <<'PY'
import json, sys
data = json.load(sys.stdin)
balances = data.get("balances") or {}
positions = data.get("positions") or {}
bal_ok = balances.get("ok") is True
pos_ok = positions.get("ok") is True
if not (bal_ok and pos_ok):
    print("replay verify: FAIL", file=sys.stderr)
    sys.exit(1)
print("replay verify: PASS")
PY

if [[ "${PASS_FAIL}" == "1" ]]; then
  echo "PASS"
fi
