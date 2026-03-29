#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
STATE_DIR="${ROOT_DIR}/var/business-debug"
RUN_DIR="${STATE_DIR}/run"
GATEWAY_STATE_DIR="${STATE_DIR}/gateway"
OMS_STATE_DIR="${STATE_DIR}/oms"
BACKOFFICE_STATE_DIR="${STATE_DIR}/backoffice"

GATEWAY_PORT="${GATEWAY_PORT:-8081}"
APP_PORT="${APP_PORT:-8080}"
OMS_PORT="${OMS_PORT:-18081}"
BACKOFFICE_PORT="${BACKOFFICE_PORT:-18082}"
EXCHANGE_TCP_PORT="${EXCHANGE_TCP_PORT:-9901}"
EXCHANGE_SIM_ADMIN_PORT="${EXCHANGE_SIM_ADMIN_PORT:-9902}"
ACCOUNT_ID="${ACCOUNT_ID:-acct_demo}"
JWT_HS256_SECRET="${JWT_HS256_SECRET:-secret123}"

APP_DEBUG_PORT="${APP_DEBUG_PORT:-5005}"
OMS_DEBUG_PORT="${OMS_DEBUG_PORT:-5006}"
BACKOFFICE_DEBUG_PORT="${BACKOFFICE_DEBUG_PORT:-5007}"
GATEWAY_SIM_DEBUG_PORT="${GATEWAY_SIM_DEBUG_PORT:-5008}"

mkdir -p "${RUN_DIR}" "${GATEWAY_STATE_DIR}" "${OMS_STATE_DIR}" "${BACKOFFICE_STATE_DIR}"

start_gradle_service() {
  local name="$1"
  local task="$2"
  shift 2
  local pid_file="${RUN_DIR}/${name}.pid"
  local log_file="${RUN_DIR}/${name}.log"

  if [[ -f "${pid_file}" ]]; then
    local existing_pid
    existing_pid="$(cat "${pid_file}" 2>/dev/null || true)"
    if [[ -n "${existing_pid}" ]] && kill -0 "${existing_pid}" 2>/dev/null; then
      echo "[skip] ${name} already running (pid=${existing_pid})"
      return
    fi
    rm -f "${pid_file}"
  fi

  (
    cd "${ROOT_DIR}"
    env "$@" nohup ./gradlew "${task}" >"${log_file}" 2>&1 </dev/null &
    echo $! >"${pid_file}"
  )
  echo "[start] ${name} task=${task} log=${log_file}"
}

start_binary_service() {
  local name="$1"
  local log_file="${RUN_DIR}/${name}.log"
  local pid_file="${RUN_DIR}/${name}.pid"
  local binary_path="${ROOT_DIR}/gateway-rust/target/debug/gateway-rust"
  shift

  if [[ -f "${pid_file}" ]]; then
    local existing_pid
    existing_pid="$(cat "${pid_file}" 2>/dev/null || true)"
    if [[ -n "${existing_pid}" ]] && kill -0 "${existing_pid}" 2>/dev/null; then
      echo "[skip] ${name} already running (pid=${existing_pid})"
      return
    fi
    rm -f "${pid_file}"
  fi

  : >"${log_file}"
  (
    cd "${ROOT_DIR}"
    if [[ ! -x "${binary_path}" || "${GATEWAY_RUST_REBUILD:-0}" == "1" ]]; then
      echo "[build] gateway-rust debug binary" >>"${log_file}"
      cargo build --manifest-path gateway-rust/Cargo.toml --bin gateway-rust >>"${log_file}" 2>&1
    else
      echo "[skip] reuse existing gateway-rust debug binary" >>"${log_file}"
    fi
    nohup env "$@" "${binary_path}" >>"${log_file}" 2>&1 </dev/null &
    echo $! >"${pid_file}"
  )
  echo "[start] ${name} binary=gateway-rust log=${log_file}"
}

wait_health() {
  local name="$1"
  local url="$2"
  local attempts="${3:-60}"

  for ((i = 1; i <= attempts; i++)); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      echo "[ready] ${name} ${url}"
      return 0
    fi
    sleep 1
  done

  echo "[fail] ${name} health timeout ${url}" >&2
  return 1
}

wait_tcp() {
  local name="$1"
  local host="$2"
  local port="$3"
  local attempts="${4:-40}"

  for ((i = 1; i <= attempts; i++)); do
    if nc -z "${host}" "${port}" >/dev/null 2>&1; then
      echo "[ready] ${name} tcp ${host}:${port}"
      return 0
    fi
    sleep 1
  done

  echo "[fail] ${name} tcp timeout ${host}:${port}" >&2
  return 1
}

if [[ ! -f "${ROOT_DIR}/frontend/dist/index.html" ]]; then
  echo "[build] frontend dist"
  (
    cd "${ROOT_DIR}/frontend"
    npm run build
  )
fi

start_gradle_service \
  "tcp-exchange-sim-debug" \
  ":gateway:runDebug" \
  EXCHANGE_TCP_PORT="${EXCHANGE_TCP_PORT}" \
  EXCHANGE_SIM_ADMIN_PORT="${EXCHANGE_SIM_ADMIN_PORT}" \
  EXCHANGE_SIM_PARTIAL_STEPS=3 \
  EXCHANGE_SIM_DELAY_MS=150 \
  GATEWAY_SIM_DEBUG_PORT="${GATEWAY_SIM_DEBUG_PORT}" \
  GATEWAY_SIM_DEBUG_SUSPEND=n \
  ORG_GRADLE_PROJECT_mainClass=gateway.exchange.TcpExchangeSimulatorMain

start_gradle_service \
  "oms-java-debug" \
  ":oms-java:runDebug" \
  ACCOUNT_ID="${ACCOUNT_ID}" \
  OMS_DEBUG_PORT="${OMS_DEBUG_PORT}" \
  OMS_DEBUG_SUSPEND=n \
  OMS_BUS_ENABLE=0 \
  OMS_BUS_KAFKA_ENABLE=0 \
  OMS_GATEWAY_AUDIT_ENABLE=true \
  OMS_GATEWAY_AUDIT_PATH="${GATEWAY_STATE_DIR}/audit.log" \
  OMS_GATEWAY_AUDIT_OFFSET_PATH="${OMS_STATE_DIR}/audit.offset" \
  OMS_GATEWAY_AUDIT_START_MODE=tail

start_gradle_service \
  "backoffice-java-debug" \
  ":backoffice-java:runDebug" \
  ACCOUNT_ID="${ACCOUNT_ID}" \
  BACKOFFICE_DEBUG_PORT="${BACKOFFICE_DEBUG_PORT}" \
  BACKOFFICE_DEBUG_SUSPEND=n \
  BACKOFFICE_BUS_ENABLE=0 \
  BACKOFFICE_BUS_KAFKA_ENABLE=0 \
  BACKOFFICE_GATEWAY_AUDIT_ENABLE=true \
  BACKOFFICE_GATEWAY_AUDIT_PATH="${GATEWAY_STATE_DIR}/audit.log" \
  BACKOFFICE_GATEWAY_AUDIT_OFFSET_PATH="${BACKOFFICE_STATE_DIR}/audit.offset" \
  BACKOFFICE_GATEWAY_AUDIT_START_MODE=tail

start_gradle_service \
  "app-java-debug" \
  ":app-java:runDebug" \
  ACCOUNT_ID="${ACCOUNT_ID}" \
  APP_HTTP_HOST=0.0.0.0 \
  APP_DEBUG_PORT="${APP_DEBUG_PORT}" \
  APP_DEBUG_SUSPEND=n \
  JWT_HS256_SECRET="${JWT_HS256_SECRET}" \
  GATEWAY_BASE_URL="http://localhost:${GATEWAY_PORT}" \
  EXCHANGE_SIM_ADMIN_URL="http://localhost:${EXCHANGE_SIM_ADMIN_PORT}" \
  OMS_JAVA_BASE_URL="http://localhost:${OMS_PORT}" \
  BACKOFFICE_JAVA_BASE_URL="http://localhost:${BACKOFFICE_PORT}"

wait_tcp "tcp-exchange-sim-debug" "127.0.0.1" "${EXCHANGE_TCP_PORT}"

start_binary_service \
  "gateway-rust-debug" \
  GATEWAY_PORT="${GATEWAY_PORT}" \
  GATEWAY_TCP_PORT=0 \
  JWT_HS256_SECRET="${JWT_HS256_SECRET}" \
  ACCOUNT_ID="${ACCOUNT_ID}" \
  EXCHANGE_TCP_HOST=127.0.0.1 \
  EXCHANGE_TCP_PORT="${EXCHANGE_TCP_PORT}" \
  FASTPATH_DRAIN_ENABLE=0 \
  KAFKA_ENABLE=0 \
  BUS_MODE=none \
  GATEWAY_WAL_PATH="${GATEWAY_STATE_DIR}/audit.log" \
  OUTBOX_OFFSET_PATH="${GATEWAY_STATE_DIR}/outbox.offset"

wait_health "oms-java-debug" "http://localhost:${OMS_PORT}/health"
wait_health "backoffice-java-debug" "http://localhost:${BACKOFFICE_PORT}/health"
wait_health "gateway-rust-debug" "http://localhost:${GATEWAY_PORT}/health"
wait_health "app-java-debug" "http://localhost:${APP_PORT}/health"

cat <<EOF
[ok] Business debug stack is up.
  app-java       : http://localhost:${APP_PORT}
  gateway-rust   : http://localhost:${GATEWAY_PORT}
  oms-java       : http://localhost:${OMS_PORT}
  backoffice-java: http://localhost:${BACKOFFICE_PORT}
  exchange-sim   : tcp://127.0.0.1:${EXCHANGE_TCP_PORT}
  sim-admin      : http://127.0.0.1:${EXCHANGE_SIM_ADMIN_PORT}
  state dir      : ${STATE_DIR}
  logs dir       : ${RUN_DIR}

Debug attach:
  app-java       : ${APP_DEBUG_PORT}
  oms-java       : ${OMS_DEBUG_PORT}
  backoffice-java: ${BACKOFFICE_DEBUG_PORT}
  exchange-sim   : ${GATEWAY_SIM_DEBUG_PORT}

VS Code:
  Task           : Business Debug Stack: Start
  Compound       : Attach business Java services
  Rust           : Attach gateway-rust

Smoke:
  ${ROOT_DIR}/scripts/ops/smoke_business_debug_stack.sh
EOF
