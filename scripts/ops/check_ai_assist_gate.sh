#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/var/results}"
REQUESTS="${REQUESTS:-300}"
TARGET_P95_MS="${TARGET_P95_MS:-800}"
TARGET_ERROR_RATE_MAX="${TARGET_ERROR_RATE_MAX:-0.01}"
TARGET_FALLBACK_RATE_MAX="${TARGET_FALLBACK_RATE_MAX:-0.05}"
ENFORCE_FALLBACK_GATE="${ENFORCE_FALLBACK_GATE:-0}"

STAMP="$(date +%Y%m%d_%H%M%S)"
TMP_OUT="/tmp/ai_assist_gate_${STAMP}.out"
SUMMARY_OUT="$OUT_DIR/ai_assist_gate_${STAMP}.summary.txt"
mkdir -p "$OUT_DIR"

cd "$ROOT_DIR"

REQUESTS="$REQUESTS" OUT_DIR="$OUT_DIR" scripts/ops/run_ai_assist_probe.sh | tee "$TMP_OUT"

probe_summary="$(awk -F= '/^\[artifacts\] summary=/{print $2}' "$TMP_OUT" | tail -n1)"
if [[ -z "$probe_summary" || ! -f "$probe_summary" ]]; then
  echo "FAIL: probe summary not found"
  exit 1
fi

read_value() {
  local key="$1"
  awk -F= -v k="$key" '$1==k {print $2}' "$probe_summary" | tail -n1
}

p95_ms="$(read_value latency_p95_ms)"
error_rate="$(read_value error_rate)"
fallback_rate="$(read_value fallback_rate)"
llm_enabled="$(read_value llm_enabled)"

pass_p95="$(awk -v v="$p95_ms" -v t="$TARGET_P95_MS" 'BEGIN{print (v+0<=t+0)?1:0}')"
pass_error="$(awk -v v="$error_rate" -v t="$TARGET_ERROR_RATE_MAX" 'BEGIN{print (v+0<=t+0)?1:0}')"
pass_fallback=1
if [[ "$ENFORCE_FALLBACK_GATE" == "1" ]]; then
  pass_fallback="$(awk -v v="$fallback_rate" -v t="$TARGET_FALLBACK_RATE_MAX" 'BEGIN{print (v+0<=t+0)?1:0}')"
fi
pass_all=$((pass_p95 & pass_error & pass_fallback))

cat >"$SUMMARY_OUT" <<EOF
ai_assist_gate
date=${STAMP}
target_p95_ms=${TARGET_P95_MS}
target_error_rate_max=${TARGET_ERROR_RATE_MAX}
target_fallback_rate_max=${TARGET_FALLBACK_RATE_MAX}
enforce_fallback_gate=${ENFORCE_FALLBACK_GATE}
llm_enabled=${llm_enabled}
observed_p95_ms=${p95_ms}
observed_error_rate=${error_rate}
observed_fallback_rate=${fallback_rate}
gate_pass_p95=${pass_p95}
gate_pass_error_rate=${pass_error}
gate_pass_fallback_rate=${pass_fallback}
gate_pass=${pass_all}
probe_summary=${probe_summary}
EOF

echo "[summary] p95_ms=${p95_ms} error_rate=${error_rate} fallback_rate=${fallback_rate} llm_enabled=${llm_enabled}"
echo "[artifacts] ${SUMMARY_OUT}"

if [[ "$pass_all" == "1" ]]; then
  echo "PASS: ai-assist gate satisfied"
  exit 0
fi

echo "FAIL: ai-assist gate not satisfied"
echo "  - p95_ms <= ${TARGET_P95_MS}: ${pass_p95}"
echo "  - error_rate <= ${TARGET_ERROR_RATE_MAX}: ${pass_error}"
if [[ "$ENFORCE_FALLBACK_GATE" == "1" ]]; then
  echo "  - fallback_rate <= ${TARGET_FALLBACK_RATE_MAX}: ${pass_fallback}"
fi
exit 1
