#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
RUN_DIR="${ROOT_DIR}/var/java-replay/run"
PID_FILE="${RUN_DIR}/gateway-rust.pid"
LOG_FILE="${RUN_DIR}/gateway-rust.log"

GATEWAY_PORT="${GATEWAY_PORT:-8081}"
GATEWAY_TCP_PORT="${GATEWAY_TCP_PORT:-0}"
JWT_HS256_SECRET="${JWT_HS256_SECRET:-secret123}"
FASTPATH_DRAIN_ENABLE="${FASTPATH_DRAIN_ENABLE:-1}"
FASTPATH_DRAIN_WORKERS="${FASTPATH_DRAIN_WORKERS:-4}"
KAFKA_ENABLE="${KAFKA_ENABLE:-0}"

mkdir -p "${RUN_DIR}"

export JWT_HS256_SECRET
export GATEWAY_BASE_URL="${GATEWAY_BASE_URL:-http://localhost:${GATEWAY_PORT}}"
export ACCOUNT_ID="${ACCOUNT_ID:-acct_demo}"

if [[ ! -x "${ROOT_DIR}/gateway-rust/target/release/gateway-rust" ]]; then
  echo "[build] gateway-rust release binary is missing"
  "${ROOT_DIR}/scripts/ops/build_gateway_rust_release.sh"
fi

if [[ -f "${PID_FILE}" ]]; then
  existing_pid="$(cat "${PID_FILE}")"
  if kill -0 "${existing_pid}" 2>/dev/null; then
    echo "[skip] gateway-rust already running (pid=${existing_pid})"
  else
    rm -f "${PID_FILE}"
  fi
fi

if [[ ! -f "${PID_FILE}" ]]; then
  (
    cd "${ROOT_DIR}"
    GATEWAY_PORT="${GATEWAY_PORT}" \
    GATEWAY_TCP_PORT="${GATEWAY_TCP_PORT}" \
    JWT_HS256_SECRET="${JWT_HS256_SECRET}" \
    KAFKA_ENABLE="${KAFKA_ENABLE}" \
    FASTPATH_DRAIN_ENABLE="${FASTPATH_DRAIN_ENABLE}" \
    FASTPATH_DRAIN_WORKERS="${FASTPATH_DRAIN_WORKERS}" \
    ./gateway-rust/target/release/gateway-rust >"${LOG_FILE}" 2>&1 &
    echo $! >"${PID_FILE}"
  )
  echo "[start] gateway-rust log=${LOG_FILE}"
fi

for ((i = 1; i <= 40; i++)); do
  if curl -fsS "http://localhost:${GATEWAY_PORT}/health" >/dev/null 2>&1; then
    echo "[ready] gateway-rust http://localhost:${GATEWAY_PORT}/health"
    break
  fi
  if [[ "${i}" -eq 40 ]]; then
    echo "[fail] gateway-rust health timeout" >&2
    exit 1
  fi
  sleep 1
done

"${ROOT_DIR}/scripts/ops/run_java_replay_stack.sh"

cat <<EOF
[ok] Business replay stack is up.
  gateway-rust   : http://localhost:${GATEWAY_PORT}
  app-java       : http://localhost:8080
  oms-java       : http://localhost:18081
  backoffice-java: http://localhost:18082
  logs dir       : ${RUN_DIR}

Frontend:
  cd ${ROOT_DIR}/frontend && npm run dev

Smoke:
  ${ROOT_DIR}/scripts/ops/smoke_business_replay_stack.sh
EOF
