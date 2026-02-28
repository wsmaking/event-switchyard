#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${ROOT_DIR}"

HOST="${GATEWAY_HOST:-localhost}"
PORT="${GATEWAY_PORT:-8081}"
JWT_SECRET="${JWT_HS256_SECRET:-dev-secret-change-me}"
DURATION="${LONGRUN_DURATION_SEC:-60}"
RPS="${LONGRUN_RPS:-200}"

mkdir -p var/results

python3 scripts/ops/bench_gateway.py sustained \
  --host "${HOST}" \
  --port "${PORT}" \
  --secret "${JWT_SECRET}" \
  --duration "${DURATION}" \
  --rps "${RPS}" \
  --output var/results
