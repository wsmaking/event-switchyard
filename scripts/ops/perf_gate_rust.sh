#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
PORT="${PORT:-8081}"
HOST="${HOST:-localhost}"
JWT_SECRET="${JWT_HS256_SECRET:-secret123}"
MODE="${MODE:-balanced}" # latency | throughput | balanced

REQUESTS_DEFAULT=2000
DURATION_DEFAULT=10
CONCURRENCY_DEFAULT=200

case "${MODE}" in
  latency)
    REQUESTS_DEFAULT=3000
    DURATION_DEFAULT=5
    CONCURRENCY_DEFAULT=50
    ;;
  throughput)
    REQUESTS_DEFAULT=1000
    DURATION_DEFAULT=20
    CONCURRENCY_DEFAULT=400
    ;;
  balanced)
    ;;
  *)
    echo "Unknown MODE: ${MODE} (use latency|throughput|balanced)" >&2
    exit 1
    ;;
esac

if [[ -z "${REQUESTS+x}" ]]; then
  REQUESTS="${REQUESTS_DEFAULT}"
fi
if [[ -z "${DURATION+x}" ]]; then
  DURATION="${DURATION_DEFAULT}"
fi
if [[ -z "${CONCURRENCY+x}" ]]; then
  CONCURRENCY="${CONCURRENCY_DEFAULT}"
fi

cd "${ROOT_DIR}"

echo "==> Building gateway-rust (release)"
(cd gateway-rust && cargo build --release)

echo "==> Starting gateway-rust on ${HOST}:${PORT}"
GATEWAY_PORT="${PORT}" \
JWT_HS256_SECRET="${JWT_SECRET}" \
GATEWAY_AUDIT_PATH="var/gateway/audit_perf.log" \
FASTPATH_DRAIN_ENABLE=1 \
FASTPATH_DRAIN_WORKERS=4 \
KAFKA_ENABLE=0 \
./gateway-rust/target/release/gateway-rust &
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
