#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
RUN_DIR="${ROOT_DIR}/var/java-replay/run"
OMS_BUS_ENABLE="${OMS_BUS_ENABLE:-1}"
OMS_BUS_KAFKA_ENABLE="${OMS_BUS_KAFKA_ENABLE:-0}"
BACKOFFICE_BUS_ENABLE="${BACKOFFICE_BUS_ENABLE:-1}"
BACKOFFICE_BUS_KAFKA_ENABLE="${BACKOFFICE_BUS_KAFKA_ENABLE:-0}"

export OMS_BUS_ENABLE
export OMS_BUS_KAFKA_ENABLE
export BACKOFFICE_BUS_ENABLE
export BACKOFFICE_BUS_KAFKA_ENABLE

mkdir -p "${RUN_DIR}"

start_service() {
  local name="$1"
  local task="$2"
  local pid_file="${RUN_DIR}/${name}.pid"
  local log_file="${RUN_DIR}/${name}.log"

  if [[ -f "${pid_file}" ]]; then
    local existing_pid
    existing_pid="$(cat "${pid_file}")"
    if kill -0 "${existing_pid}" 2>/dev/null; then
      echo "[skip] ${name} already running (pid=${existing_pid})"
      return
    fi
    rm -f "${pid_file}"
  fi

  (
    cd "${ROOT_DIR}"
    nohup ./gradlew "${task}" >"${log_file}" 2>&1 </dev/null &
    echo $! >"${pid_file}"
  )
  echo "[start] ${name} task=${task} log=${log_file}"
}

wait_health() {
  local name="$1"
  local url="$2"
  local attempts="${3:-40}"

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

start_service "oms-java" ":oms-java:run"
start_service "backoffice-java" ":backoffice-java:run"
start_service "app-java" ":app-java:run"

wait_health "oms-java" "http://localhost:18081/health"
wait_health "backoffice-java" "http://localhost:18082/health"
wait_health "app-java" "http://localhost:8080/health"

cat <<EOF
[ok] Java replay stack is up.
  app-java       : http://localhost:8080
  oms-java       : http://localhost:18081
  backoffice-java: http://localhost:18082
  state dir      : ${ROOT_DIR}/var/java-replay
  logs dir       : ${RUN_DIR}

Frontend:
  cd ${ROOT_DIR}/frontend && npm run dev
EOF
