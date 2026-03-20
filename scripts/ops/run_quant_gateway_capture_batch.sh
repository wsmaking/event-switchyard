#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

BASE_URL="http://127.0.0.1:8081"
FEEDBACK_JSONL="var/gateway/quant_feedback.jsonl"
OUTPUT_DIR="/tmp/quant-gateway-captures"
SHADOW_RUN_PREFIX="collector"
EXPORT_EXTRA=()
COLLECT_EXTRA=()

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
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --shadow-run-prefix)
      SHADOW_RUN_PREFIX="$2"
      shift 2
      ;;
    --)
      shift
      EXPORT_EXTRA+=("$@")
      break
      ;;
    *)
      COLLECT_EXTRA+=("$1")
      shift
      ;;
  esac
done

INTENTS_DIR="$OUTPUT_DIR/intents"
CAPTURES_DIR="$OUTPUT_DIR/captures"
mkdir -p "$INTENTS_DIR" "$CAPTURES_DIR"

scripts/ops/export_quant_strategy_intent_batch.sh --output-dir "$INTENTS_DIR" "${EXPORT_EXTRA[@]}"
scripts/ops/run_collect_quant_feedback_shadow_from_gateway.sh \
  --base-url "$BASE_URL" \
  --input-dir "$INTENTS_DIR" \
  --feedback-jsonl "$FEEDBACK_JSONL" \
  --output-dir "$CAPTURES_DIR" \
  --shadow-run-prefix "$SHADOW_RUN_PREFIX" \
  "${COLLECT_EXTRA[@]}"
