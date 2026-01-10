#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
PORT="${PORT:-8081}"
HOST="${HOST:-localhost}"
JWT_SECRET="${JWT_HS256_SECRET:-secret123}"
MODE="${MODE:-balanced}" # latency | throughput | balanced
BASELINE_PATH="${BASELINE_PATH:-baseline/perf_gate_rust.json}"
BASELINE_REGRESSION="${BASELINE_REGRESSION:-0.05}"
UPDATE_BASELINE="${UPDATE_BASELINE:-0}"

REQUESTS_DEFAULT=2000
DURATION_DEFAULT=10
CONCURRENCY_DEFAULT=200
THREADS_DEFAULT=8

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
if [[ -z "${THREADS+x}" ]]; then
  THREADS="${THREADS_DEFAULT}"
fi

cd "${ROOT_DIR}"
mkdir -p var/results

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
WRK_OUTPUT="var/results/wrk_gate_${PORT}.txt"
HOST="${HOST}" \
PORT="${PORT}" \
DURATION="${DURATION}" \
CONNECTIONS="${CONCURRENCY}" \
THREADS="${THREADS}" \
LATENCY="${WRK_LATENCY:-0}" \
scripts/ops/wrk_gateway_rust.sh | tee "${WRK_OUTPUT}"

PERF_GATE_ARGS=(
  --run --ci
  --host "${HOST}"
  --port "${PORT}"
  --requests "${REQUESTS}"
  --duration "${DURATION}"
  --concurrency "${CONCURRENCY}"
  --wrk-input "${WRK_OUTPUT}"
)
if [[ -f "${BASELINE_PATH}" ]]; then
  PERF_GATE_ARGS+=( --baseline "${BASELINE_PATH}" --baseline-regression "${BASELINE_REGRESSION}" )
fi
if [[ "${UPDATE_BASELINE}" == "1" ]]; then
  PERF_GATE_ARGS+=( --update-baseline "${BASELINE_PATH}" )
fi

python3 scripts/ops/perf_gate.py "${PERF_GATE_ARGS[@]}"
