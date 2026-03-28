#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
RUN_DIR="${ROOT_DIR}/var/mainline/run"

stop_service() {
  local name="$1"
  local pid_file="${RUN_DIR}/${name}.pid"

  if [[ ! -f "${pid_file}" ]]; then
    echo "[skip] ${name} pid file not found"
    return
  fi

  local pid
  pid="$(cat "${pid_file}" 2>/dev/null || true)"
  if [[ -z "${pid}" ]]; then
    echo "[stale] ${name} pid file unreadable"
    rm -f "${pid_file}"
    return
  fi
  if kill -0 "${pid}" 2>/dev/null; then
    kill -TERM "${pid}" 2>/dev/null || true
    for _ in $(seq 1 20); do
      if ! kill -0 "${pid}" 2>/dev/null; then
        break
      fi
      sleep 1
    done
    echo "[stop] ${name} pid=${pid}"
  else
    echo "[stale] ${name} pid=${pid}"
  fi
  rm -f "${pid_file}"
}

stop_service "app-java-mainline"
stop_service "gateway-rust-mainline"
stop_service "backoffice-java-mainline"
stop_service "oms-java-mainline"
stop_service "tcp-exchange-sim"

if [[ "${MAINLINE_INFRA_STOP:-1}" == "1" ]]; then
  docker compose stop postgres kafka zookeeper >/dev/null || true
  echo "[stop] docker compose services: postgres kafka zookeeper"
fi
