#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
RUN_DIR="${ROOT_DIR}/var/mainline/run"

stop_projection_service() {
  local name="$1"
  local pid_file="${RUN_DIR}/${name}.pid"
  if [[ ! -f "${pid_file}" ]]; then
    return
  fi
  local pid
  pid="$(cat "${pid_file}" 2>/dev/null || true)"
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    kill -TERM "${pid}" 2>/dev/null || true
    for _ in $(seq 1 20); do
      if ! kill -0 "${pid}" 2>/dev/null; then
        break
      fi
      sleep 1
    done
  fi
  rm -f "${pid_file}"
  echo "[stop] ${name}"
}

echo "[pre] health and ops"
"${ROOT_DIR}/scripts/ops/check_business_mainline_ops.sh"

stop_projection_service "app-java-mainline"
stop_projection_service "oms-java-mainline"
stop_projection_service "backoffice-java-mainline"

sleep 3

echo "[restart] projection services"
zsh "${ROOT_DIR}/scripts/ops/run_business_mainline_stack.sh"

echo "[post] health and ops"
"${ROOT_DIR}/scripts/ops/check_business_mainline_ops.sh"

echo "[ok] projection recovery drill completed"
