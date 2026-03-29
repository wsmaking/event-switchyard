#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
RUN_DIR="${ROOT_DIR}/var/business-debug/run"

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

stop_matching_processes() {
  local name="$1"
  local pattern="$2"
  local pids

  pids="$(ps -ax -o pid= -o command= | awk -v pat="${pattern}" '$0 ~ pat {print $1}')"
  if [[ -z "${pids}" ]]; then
    return
  fi

  while IFS= read -r pid; do
    [[ -z "${pid}" ]] && continue
    if kill -0 "${pid}" 2>/dev/null; then
      kill -TERM "${pid}" 2>/dev/null || true
      for _ in $(seq 1 20); do
        if ! kill -0 "${pid}" 2>/dev/null; then
          break
        fi
        sleep 1
      done
      echo "[stop] ${name} process pid=${pid}"
    fi
  done <<<"${pids}"
}

stop_service "gateway-rust-debug"
stop_service "app-java-debug"
stop_service "backoffice-java-debug"
stop_service "oms-java-debug"
stop_service "tcp-exchange-sim-debug"

stop_matching_processes "gateway-rust-debug" "gateway-rust/target/debug/gateway-rust"
stop_matching_processes "app-java-debug" "appjava.Main"
stop_matching_processes "backoffice-java-debug" "backofficejava.Main"
stop_matching_processes "oms-java-debug" "oms.Main"
stop_matching_processes "tcp-exchange-sim-debug" "gateway.exchange.TcpExchangeSimulatorMain"
