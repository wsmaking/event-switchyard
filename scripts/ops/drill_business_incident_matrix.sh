#!/usr/bin/env zsh
set -euo pipefail

APP_BASE_URL="${APP_BASE_URL:-http://localhost:8080}"
SIM_ADMIN_URL="${SIM_ADMIN_URL:-http://localhost:9902}"

reset_simulator() {
  curl -fsS -X POST "${SIM_ADMIN_URL}/admin/reset" >/dev/null
}

show_snapshot() {
  local label="$1"
  echo
  echo "--- ${label} ---"
  echo "[sim]"
  curl -fsS "${SIM_ADMIN_URL}/admin/status"
  echo
  echo "[go-no-go]"
  curl -fsS "${APP_BASE_URL}/api/ops/go-no-go"
  echo
  echo "[production-engineering]"
  curl -fsS "${APP_BASE_URL}/api/ops/production-engineering"
  echo
}

apply_control() {
  local payload="$1"
  curl -fsS -X POST "${SIM_ADMIN_URL}/admin/control" \
    -H 'Content-Type: application/json' \
    -d "${payload}" >/dev/null
}

trap reset_simulator EXIT

reset_simulator
show_snapshot "steady-state"

apply_control '{"mode":"THROTTLED","activeIncidents":["throttle pressure drill"]}'
show_snapshot "throttled"

apply_control '{"mode":"SESSION_FLAP","sessionState":"FLAPPING","activeIncidents":["session flap drill"]}'
show_snapshot "session-flap"

apply_control '{"mode":"NORMAL","sessionState":"RUNNING","auctionState":"HALTED","activeIncidents":["auction halt drill"]}'
show_snapshot "auction-halt"

apply_control '{"mode":"NORMAL","sessionState":"RUNNING","auctionState":"CONTINUOUS","dropCopyDivergence":true,"activeIncidents":["drop-copy divergence drill"]}'
show_snapshot "drop-copy-divergence"

reset_simulator
show_snapshot "reset"
