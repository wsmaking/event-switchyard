#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BACKOFFICE_URL:-http://localhost:8082}"
TOKEN="${BACKOFFICE_JWT:-}"
ACCOUNT_ID="${BACKOFFICE_ACCOUNT_ID:-}"
STALE_SEC="${BACKOFFICE_STALE_SEC:-120}"
STRICT="${BACKOFFICE_STRICT:-0}"
JWT_SECRET="${JWT_HS256_SECRET:-}"
PASS_FAIL="${BACKOFFICE_PASSFAIL:-1}"
PASS_COUNT=0
if [[ "${PASS_FAIL}" == "1" ]]; then
  trap 'echo "FAIL" >&2' ERR
fi

if [[ -z "$TOKEN" ]]; then
  if [[ -n "$JWT_SECRET" && -n "$ACCOUNT_ID" ]]; then
    TOKEN="$(
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
fi

echo "# health"
curl -fsS "${BASE_URL}/health"
echo
PASS_COUNT=$((PASS_COUNT + 1))

if [[ -z "$TOKEN" ]]; then
  echo "BACKOFFICE_JWT is required (Authorization: Bearer <JWT>)" >&2
  exit 2
fi

auth_header=("Authorization: Bearer ${TOKEN}")
account_qs=""
if [[ -n "$ACCOUNT_ID" ]]; then
  account_qs="accountId=${ACCOUNT_ID}"
fi

query_with() {
  local base_query="$1"
  if [[ -n "$base_query" ]]; then
    echo "?${base_query}"
  else
    echo ""
  fi
}

append_query() {
  local base_query="$1"
  local extra="$2"
  if [[ -n "$base_query" ]]; then
    echo "${base_query}&${extra}"
  else
    echo "${extra}"
  fi
}

echo "# stats"
stats_json="$(curl -fsS -H "${auth_header[@]}" "${BASE_URL}/stats$(query_with "${account_qs}")")"
echo "$stats_json"
printf "%s" "$stats_json" | STALE_SEC="$STALE_SEC" STRICT="$STRICT" python3 - <<'PY'
import datetime as dt
import json
import os
import sys

raw = sys.stdin.read().strip()
if not raw:
    print("stats: empty response", file=sys.stderr)
    sys.exit(1)

stats = json.loads(raw)
stale_sec = int(os.environ.get("STALE_SEC", "120"))
strict = os.environ.get("STRICT", "0").lower() in ("1", "true", "yes")
last_event_at = stats.get("lastEventAt")

if last_event_at is None:
    msg = "stats: lastEventAt missing (no events yet?)"
    print(msg, file=sys.stderr)
    sys.exit(1 if strict else 0)

if stale_sec <= 0:
    sys.exit(0)

try:
    if last_event_at.endswith("Z"):
        last_event_at = last_event_at.replace("Z", "+00:00")
    last_dt = dt.datetime.fromisoformat(last_event_at)
except Exception:
    print("stats: invalid lastEventAt", file=sys.stderr)
    sys.exit(1)

now = dt.datetime.now(tz=last_dt.tzinfo or dt.timezone.utc)
age = (now - last_dt).total_seconds()
print(f"stats: lastEventAt age={age:.0f}s")
if age > stale_sec:
    print(f"stats: stale (>{stale_sec}s)", file=sys.stderr)
    sys.exit(1)
PY
echo
PASS_COUNT=$((PASS_COUNT + 1))

echo "# positions"
curl -fsS -H "${auth_header[@]}" "${BASE_URL}/positions$(query_with "${account_qs}")"
echo
PASS_COUNT=$((PASS_COUNT + 1))

echo "# balances"
curl -fsS -H "${auth_header[@]}" "${BASE_URL}/balances$(query_with "${account_qs}")"
echo
PASS_COUNT=$((PASS_COUNT + 1))

echo "# ledger (last 10)"
ledger_query="$(append_query "${account_qs}" "limit=10")"
curl -fsS -H "${auth_header[@]}" "${BASE_URL}/ledger$(query_with "${ledger_query}")"
echo
PASS_COUNT=$((PASS_COUNT + 1))

echo "# reconcile"
reconcile_query="$(append_query "${account_qs}" "limit=1000")"
reconcile_json="$(curl -fsS -H "${auth_header[@]}" "${BASE_URL}/reconcile$(query_with "${reconcile_query}")")"
echo "$reconcile_json"
printf "%s" "$reconcile_json" | python3 - <<'PY'
import json
import sys

raw = sys.stdin.read().strip()
if not raw:
    print("reconcile: empty response", file=sys.stderr)
    sys.exit(1)

data = json.loads(raw)
balances = data.get("balances") or {}
positions = data.get("positions") or {}
bal_ok = balances.get("ok")
pos_ok = positions.get("ok")
if bal_ok is not True or pos_ok is not True:
    print("reconcile: mismatch detected", file=sys.stderr)
    sys.exit(1)
print("reconcile: ok")
PY
echo
PASS_COUNT=$((PASS_COUNT + 1))

if [[ "${PASS_FAIL}" == "1" ]]; then
  echo "PASS (${PASS_COUNT} checks)"
fi
