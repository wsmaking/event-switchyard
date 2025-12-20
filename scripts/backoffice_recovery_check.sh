#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BACKOFFICE_URL:-http://localhost:8082}"
TOKEN="${BACKOFFICE_JWT:-}"
ACCOUNT_ID="${BACKOFFICE_ACCOUNT_ID:-}"

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
curl -fsS -H "${auth_header[@]}" "${BASE_URL}/stats$(query_with "${account_qs}")"
echo

echo "# positions"
curl -fsS -H "${auth_header[@]}" "${BASE_URL}/positions$(query_with "${account_qs}")"
echo

echo "# balances"
curl -fsS -H "${auth_header[@]}" "${BASE_URL}/balances$(query_with "${account_qs}")"
echo

echo "# ledger (last 10)"
ledger_query="$(append_query "${account_qs}" "limit=10")"
curl -fsS -H "${auth_header[@]}" "${BASE_URL}/ledger$(query_with "${ledger_query}")"
echo
