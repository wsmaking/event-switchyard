#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${ROOT_DIR}"

OUT="${OUT:-var/results/canary.json}"
EVENTS="${EVENTS:-500}"
SLO_PROFILE="${SLO_PROFILE:-light}"
SLO_P99_US="${SLO_P99_US:-}"
SLO_P999_US="${SLO_P999_US:-}"
SLO_TAIL_RATIO_MAX="${SLO_TAIL_RATIO_MAX:-}"
SLO_THROUGHPUT_MIN="${SLO_THROUGHPUT_MIN:-}"

APP_PID=""
cleanup() {
  if [[ -n "${APP_PID}" ]]; then
    kill "${APP_PID}" >/dev/null 2>&1 || true
    wait "${APP_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

mkdir -p "$(dirname "$OUT")"

echo "==> Starting app for check-lite"
FAST_PATH_ENABLE=1 FAST_PATH_METRICS=1 KAFKA_BRIDGE_ENABLE=0 \
  ./gradlew :app:run >/tmp/check_lite_app.log 2>&1 &
APP_PID=$!

READY=0
for _ in $(seq 1 60); do
  if curl -s "http://localhost:8080/health" >/dev/null 2>&1; then
    READY=1
    break
  fi
  sleep 0.5
done
if [[ "${READY}" != "1" ]]; then
  echo "ERROR: app failed to start; see /tmp/check_lite_app.log" >&2
  exit 1
fi

python3 scripts/quick_canary.py --url http://localhost:8080 --events "${EVENTS}" --out "${OUT}"

SLO_ARGS=(--in "${OUT}" --schema contracts/canary_result_v1.schema.json --profile "${SLO_PROFILE}")
if [[ -n "${SLO_P99_US}" ]]; then
  SLO_ARGS+=(--p99-us "${SLO_P99_US}")
fi
if [[ -n "${SLO_P999_US}" ]]; then
  SLO_ARGS+=(--p999-us "${SLO_P999_US}")
fi
if [[ -n "${SLO_TAIL_RATIO_MAX}" ]]; then
  SLO_ARGS+=(--tail-ratio-max "${SLO_TAIL_RATIO_MAX}")
fi
if [[ -n "${SLO_THROUGHPUT_MIN}" ]]; then
  SLO_ARGS+=(--throughput-min-events-per-sec "${SLO_THROUGHPUT_MIN}")
fi
python3 scripts/slo_gate.py "${SLO_ARGS[@]}"
