#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
RUN_DIR="${ROOT_DIR}/var/mainline/run"
mkdir -p "${RUN_DIR}"

GATEWAY_PORT="${GATEWAY_PORT:-8081}"
APP_PORT="${APP_PORT:-8080}"
OMS_PORT="${OMS_PORT:-18081}"
BACKOFFICE_PORT="${BACKOFFICE_PORT:-18082}"
EXCHANGE_TCP_PORT="${EXCHANGE_TCP_PORT:-9901}"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
KAFKA_TOPIC="${KAFKA_TOPIC:-events}"
JWT_HS256_SECRET="${JWT_HS256_SECRET:-secret123}"
ACCOUNT_ID="${ACCOUNT_ID:-acct_demo}"
MAINLINE_DB_URL="${MAINLINE_DB_URL:-jdbc:postgresql://localhost:5432/backoffice}"
MAINLINE_DB_USER="${MAINLINE_DB_USER:-backoffice}"
MAINLINE_DB_PASSWORD="${MAINLINE_DB_PASSWORD:-backoffice}"

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
  shift
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
    env "$@" nohup ./gateway-rust/target/release/gateway-rust >"${log_file}" 2>&1 </dev/null &
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

wait_for_topic_init() {
  for ((i = 1; i <= 40; i++)); do
    if docker compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic "${KAFKA_TOPIC}" --partitions 1 --replication-factor 1 >/dev/null 2>&1; then
      echo "[ready] kafka topic=${KAFKA_TOPIC}"
      return 0
    fi
    sleep 2
  done
  echo "[fail] kafka topic init timeout topic=${KAFKA_TOPIC}" >&2
  return 1
}

docker compose up -d zookeeper kafka postgres >/dev/null
wait_for_topic_init

echo "[build] gateway-rust release (features=kafka-bus)"
GATEWAY_RUST_FEATURES="kafka-bus" "${ROOT_DIR}/scripts/ops/build_gateway_rust_release.sh"

start_gradle_service \
  "tcp-exchange-sim" \
  ":gateway:run" \
  EXCHANGE_TCP_PORT="${EXCHANGE_TCP_PORT}" \
  EXCHANGE_SIM_PARTIAL_STEPS=3 \
  EXCHANGE_SIM_DELAY_MS=150 \
  ORG_GRADLE_PROJECT_mainClass=gateway.exchange.TcpExchangeSimulatorMain

start_gradle_service \
  "oms-java-mainline" \
  ":oms-java:run" \
  ACCOUNT_ID="${ACCOUNT_ID}" \
  OMS_STORE_MODE=postgres \
  OMS_DB_URL="${MAINLINE_DB_URL}" \
  OMS_DB_USER="${MAINLINE_DB_USER}" \
  OMS_DB_PASSWORD="${MAINLINE_DB_PASSWORD}" \
  OMS_BUS_ENABLE=1 \
  OMS_BUS_KAFKA_ENABLE=1 \
  OMS_BUS_KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS}" \
  OMS_BUS_KAFKA_TOPIC="${KAFKA_TOPIC}"

start_gradle_service \
  "backoffice-java-mainline" \
  ":backoffice-java:run" \
  ACCOUNT_ID="${ACCOUNT_ID}" \
  BACKOFFICE_STORE_MODE=postgres \
  BACKOFFICE_DB_URL="${MAINLINE_DB_URL}" \
  BACKOFFICE_DB_USER="${MAINLINE_DB_USER}" \
  BACKOFFICE_DB_PASSWORD="${MAINLINE_DB_PASSWORD}" \
  BACKOFFICE_BUS_ENABLE=1 \
  BACKOFFICE_BUS_KAFKA_ENABLE=1 \
  BACKOFFICE_BUS_KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS}" \
  BACKOFFICE_BUS_KAFKA_TOPIC="${KAFKA_TOPIC}"

start_binary_service \
  "gateway-rust-mainline" \
  GATEWAY_PORT="${GATEWAY_PORT}" \
  GATEWAY_TCP_PORT=0 \
  JWT_HS256_SECRET="${JWT_HS256_SECRET}" \
  ACCOUNT_ID="${ACCOUNT_ID}" \
  EXCHANGE_TCP_HOST=127.0.0.1 \
  EXCHANGE_TCP_PORT="${EXCHANGE_TCP_PORT}" \
  BUS_MODE=outbox \
  BUS_SCHEMA_VERSION=2 \
  KAFKA_ENABLE=1 \
  KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS}" \
  KAFKA_TOPIC="${KAFKA_TOPIC}" \
  FASTPATH_DRAIN_ENABLE=0

start_gradle_service \
  "app-java-mainline" \
  ":app-java:run" \
  ACCOUNT_ID="${ACCOUNT_ID}" \
  JWT_HS256_SECRET="${JWT_HS256_SECRET}" \
  GATEWAY_BASE_URL="http://localhost:${GATEWAY_PORT}" \
  OMS_JAVA_BASE_URL="http://localhost:${OMS_PORT}" \
  BACKOFFICE_JAVA_BASE_URL="http://localhost:${BACKOFFICE_PORT}"

wait_health "oms-java-mainline" "http://localhost:${OMS_PORT}/health"
wait_health "backoffice-java-mainline" "http://localhost:${BACKOFFICE_PORT}/health"
wait_health "gateway-rust-mainline" "http://localhost:${GATEWAY_PORT}/health"
wait_health "app-java-mainline" "http://localhost:${APP_PORT}/health"

cat <<EOF
[ok] Business mainline stack is up.
  app-java       : http://localhost:${APP_PORT}
  gateway-rust   : http://localhost:${GATEWAY_PORT}
  oms-java       : http://localhost:${OMS_PORT}
  backoffice-java: http://localhost:${BACKOFFICE_PORT}
  kafka          : ${KAFKA_BOOTSTRAP_SERVERS}
  postgres       : ${MAINLINE_DB_URL}
  run dir        : ${RUN_DIR}

Frontend:
  cd ${ROOT_DIR}/frontend && npm run dev

Smoke:
  ${ROOT_DIR}/scripts/ops/smoke_business_mainline_stack.sh
EOF
