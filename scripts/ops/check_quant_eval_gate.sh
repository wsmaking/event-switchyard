#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

STAMP="$(date +%Y%m%d_%H%M%S)"
BASE_URL="http://127.0.0.1:8081"
FEEDBACK_JSONL="var/gateway/quant_feedback.jsonl"
OUTPUT_ROOT="/tmp/quant-eval-gate-${STAMP}"
MANIFEST_PATH="contracts/fixtures/quant_eval_gate_scenarios.json"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base-url)
      BASE_URL="$2"
      shift 2
      ;;
    --feedback-jsonl)
      FEEDBACK_JSONL="$2"
      shift 2
      ;;
    --output-dir)
      OUTPUT_ROOT="$2"
      shift 2
      ;;
    --manifest)
      MANIFEST_PATH="$2"
      shift 2
      ;;
    *)
      echo "unknown arg: $1" >&2
      exit 2
      ;;
  esac
done

CAPTURE_ROOT="${OUTPUT_ROOT}/capture_bundle"
REPORTS_DIR="${OUTPUT_ROOT}/reports"
JOINED_DIR="${OUTPUT_ROOT}/joined"
SUMMARY_OUT="${OUTPUT_ROOT}/quant_eval_gate.summary.txt"

mkdir -p "$OUTPUT_ROOT"

scripts/ops/run_quant_gateway_capture_batch.sh \
  --base-url "$BASE_URL" \
  --feedback-jsonl "$FEEDBACK_JSONL" \
  --output-dir "$CAPTURE_ROOT" \
  --manifest "$MANIFEST_PATH"

scripts/ops/run_mini_exchange_quant_batch.sh \
  --input-dir "${CAPTURE_ROOT}/intents" \
  --output-dir "$REPORTS_DIR"

scripts/ops/run_join_quant_feedback_with_mini_exchange_batch.sh \
  --reports-dir "$REPORTS_DIR" \
  --captures-dir "${CAPTURE_ROOT}/captures" \
  --output-dir "$JOINED_DIR"

python3 scripts/ops/check_quant_eval_gate.py \
  --summary "${JOINED_DIR}/batch_summary.csv" \
  --expectations "$MANIFEST_PATH" \
  --output "$SUMMARY_OUT"

printf 'WROTE %s\n' "$SUMMARY_OUT"
