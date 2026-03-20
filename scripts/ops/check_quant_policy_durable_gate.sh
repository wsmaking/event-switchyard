#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

STAMP="$(date +%Y%m%d_%H%M%S)"
OUTPUT_DIR="var/results/quant_policy_durable_gate_${STAMP}"
SUMMARY_OUT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --summary-out)
      SUMMARY_OUT="$2"
      shift 2
      ;;
    *)
      echo "unknown arg: $1" >&2
      exit 2
      ;;
  esac
done

mkdir -p "$OUTPUT_DIR"
if [[ -z "$SUMMARY_OUT" ]]; then
  SUMMARY_OUT="${OUTPUT_DIR}/quant_policy_durable_gate.summary.txt"
fi

run_case() {
  local key="$1"
  local test_name="$2"
  local log_path="${OUTPUT_DIR}/${key}.log"

  if cargo test --manifest-path gateway-rust/Cargo.toml "$test_name" -- --exact \
    >"$log_path" 2>&1; then
    printf '%s=1\n' "$key" >>"$SUMMARY_OUT"
  else
    printf '%s=0\n' "$key" >>"$SUMMARY_OUT"
    printf 'gate_pass=0\n' >>"$SUMMARY_OUT"
    printf 'FAIL: %s\n' "$test_name" >&2
    printf '--- %s ---\n' "$log_path" >&2
    tail -n 200 "$log_path" >&2 || true
    exit 1
  fi
}

: >"$SUMMARY_OUT"
printf 'gate=quant_policy_durable\n' >>"$SUMMARY_OUT"
printf 'output_dir=%s\n' "$OUTPUT_DIR" >>"$SUMMARY_OUT"

run_case \
  "normal_durable_controller_soft_reject" \
  "strategy_intent_submit_normal_urgency_rejects_durable_controller_soft"
run_case \
  "high_bypasses_durable_controller_soft" \
  "strategy_intent_submit_high_urgency_bypasses_durable_controller_soft"
run_case \
  "high_does_not_bypass_durable_controller_hard" \
  "strategy_intent_submit_high_urgency_does_not_bypass_durable_controller_hard"
run_case \
  "normal_durable_backpressure_soft_reject" \
  "strategy_intent_submit_normal_urgency_rejects_durable_backpressure_soft"
run_case \
  "high_bypasses_durable_backpressure_soft" \
  "strategy_intent_submit_high_urgency_bypasses_durable_backpressure_soft"
run_case \
  "normal_durable_confirm_age_soft_reject" \
  "strategy_intent_submit_normal_urgency_rejects_durable_confirm_age_soft"
run_case \
  "high_bypasses_durable_confirm_age_soft" \
  "strategy_intent_submit_high_urgency_bypasses_durable_confirm_age_soft"

printf 'gate_pass=1\n' >>"$SUMMARY_OUT"
printf 'WROTE %s\n' "$SUMMARY_OUT"
