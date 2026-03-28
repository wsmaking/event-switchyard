#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
RUN_DIR="${ROOT_DIR}/var/java-replay/run"
PID_FILE="${RUN_DIR}/gateway-rust.pid"

"${ROOT_DIR}/scripts/ops/stop_java_replay_stack.sh"

if [[ ! -f "${PID_FILE}" ]]; then
  echo "[skip] gateway-rust pid file not found"
  exit 0
fi

pid="$(cat "${PID_FILE}" 2>/dev/null || true)"
if [[ -z "${pid}" ]]; then
  echo "[stale] gateway-rust pid file unreadable"
  rm -f "${PID_FILE}"
  exit 0
fi
if kill -0 "${pid}" 2>/dev/null; then
  kill -TERM "${pid}" 2>/dev/null || true
  for _ in $(seq 1 20); do
    if ! kill -0 "${pid}" 2>/dev/null; then
      break
    fi
    sleep 1
  done
  echo "[stop] gateway-rust pid=${pid}"
else
  echo "[stale] gateway-rust pid=${pid}"
fi

rm -f "${PID_FILE}"
