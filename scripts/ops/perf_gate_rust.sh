#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
PORT="${PORT:-8081}"
HOST="${HOST:-localhost}"
JWT_SECRET="${JWT_HS256_SECRET:-secret123}"
REQUESTS="${REQUESTS:-1000}"
DURATION="${DURATION:-5}"
CONCURRENCY="${CONCURRENCY:-50}"

cd "${ROOT_DIR}"

echo "==> Building gateway-rust (release)"
cargo build -p gateway-rust --release

echo "==> Starting gateway-rust on ${HOST}:${PORT}"
GATEWAY_PORT="${PORT}" \
JWT_HS256_SECRET="${JWT_SECRET}" \
GATEWAY_AUDIT_PATH="var/gateway/audit_perf.log" \
KAFKA_ENABLE=0 \
./target/release/gateway-rust &
SERVER_PID=$!

cleanup() {
  if kill -0 "${SERVER_PID}" >/dev/null 2>&1; then
    kill "${SERVER_PID}" >/dev/null 2>&1 || true
    wait "${SERVER_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "==> Waiting for /health"
READY=0
for _ in $(seq 1 50); do
  if curl -s "http://${HOST}:${PORT}/health" >/dev/null 2>&1; then
    READY=1
    break
  fi
  sleep 0.2
done
if [[ "${READY}" != "1" ]]; then
  echo "ERROR: gateway-rust failed to start" >&2
  exit 1
fi

echo "==> Running perf gate"
python3 scripts/ops/perf_gate.py --run --ci \
  --host "${HOST}" \
  --port "${PORT}" \
  --requests "${REQUESTS}" \
  --duration "${DURATION}" \
  --concurrency "${CONCURRENCY}"
