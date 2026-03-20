#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

STAMP="$(date +%Y%m%d_%H%M%S)"
PORT=18081
MANIFEST_PATH="contracts/fixtures/quant_eval_gate_scenarios.json"
OUTPUT_ROOT="var/results/quant_eval_gate_ci_${STAMP}"
WAIT_SECONDS=60

while [[ $# -gt 0 ]]; do
  case "$1" in
    --port)
      PORT="$2"
      shift 2
      ;;
    --manifest)
      MANIFEST_PATH="$2"
      shift 2
      ;;
    --output-dir)
      OUTPUT_ROOT="$2"
      shift 2
      ;;
    --wait-seconds)
      WAIT_SECONDS="$2"
      shift 2
      ;;
    *)
      echo "unknown arg: $1" >&2
      exit 2
      ;;
  esac
done

BASE_URL="http://127.0.0.1:${PORT}"
RUN_DIR="$OUTPUT_ROOT"
GATEWAY_LOG="${RUN_DIR}/gateway.log"
FEEDBACK_JSONL="${RUN_DIR}/quant_feedback.jsonl"
WAL_PATH="${RUN_DIR}/audit.log"
OUTBOX_OFFSET_PATH="${RUN_DIR}/outbox.offset"
MIRROR_OFFSET_PATH="${RUN_DIR}/audit.mirror.offset"

mkdir -p "$RUN_DIR"
: >"$FEEDBACK_JSONL"

GATEWAY_PID=""
cleanup() {
  if [[ -n "$GATEWAY_PID" ]] && kill -0 "$GATEWAY_PID" 2>/dev/null; then
    kill -TERM "$GATEWAY_PID" 2>/dev/null || true
    wait "$GATEWAY_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

env \
  GATEWAY_PORT="$PORT" \
  GATEWAY_TCP_PORT=0 \
  GATEWAY_WAL_PATH="$WAL_PATH" \
  AUDIT_MIRROR_ENABLE=false \
  AUDIT_MIRROR_OFFSET_PATH="$MIRROR_OFFSET_PATH" \
  OUTBOX_OFFSET_PATH="$OUTBOX_OFFSET_PATH" \
  BUS_MODE=disabled \
  KAFKA_ENABLE=false \
  QUANT_FEEDBACK_ENABLE=true \
  QUANT_FEEDBACK_PATH="$FEEDBACK_JSONL" \
  RUST_LOG=warn \
  cargo run --manifest-path gateway-rust/Cargo.toml --bin gateway-rust --quiet \
  >"$GATEWAY_LOG" 2>&1 &
GATEWAY_PID=$!

started=0
for _ in $(seq 1 "$WAIT_SECONDS"); do
  if curl -fsS "${BASE_URL}/metrics" >/dev/null 2>&1; then
    started=1
    break
  fi
  sleep 1
done

if [[ "$started" != "1" ]]; then
  echo "FAIL: gateway failed to start for quant eval gate" >&2
  echo "--- gateway log tail ---" >&2
  tail -n 200 "$GATEWAY_LOG" >&2 || true
  exit 1
fi

scripts/ops/check_quant_eval_gate.sh \
  --base-url "$BASE_URL" \
  --feedback-jsonl "$FEEDBACK_JSONL" \
  --output-dir "$RUN_DIR" \
  --manifest "$MANIFEST_PATH"

printf 'ARTIFACT_DIR=%s\n' "$RUN_DIR"
printf 'SUMMARY=%s\n' "${RUN_DIR}/quant_eval_gate.summary.txt"
