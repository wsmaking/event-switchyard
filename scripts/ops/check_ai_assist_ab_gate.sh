#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/var/results}"
REQUESTS="${REQUESTS:-300}"
REQUEST_TIMEOUT_SEC="${REQUEST_TIMEOUT_SEC:-2}"
BUILD_JAR="${BUILD_JAR:-1}"

LLM_PROVIDER_B="${LLM_PROVIDER_B:-openai}"
AI_ASSIST_OPENAI_API_KEY="${AI_ASSIST_OPENAI_API_KEY:-}"
AI_ASSIST_OPENAI_ENDPOINT="${AI_ASSIST_OPENAI_ENDPOINT:-https://api.openai.com/v1/chat/completions}"
AI_ASSIST_OPENAI_MODEL="${AI_ASSIST_OPENAI_MODEL:-gpt-4o-mini}"
AI_ASSIST_OPENAI_TEMPERATURE="${AI_ASSIST_OPENAI_TEMPERATURE:-0.1}"
AI_ASSIST_OPENAI_MAX_TOKENS="${AI_ASSIST_OPENAI_MAX_TOKENS:-120}"
AI_ASSIST_OPENAI_HTTP_TIMEOUT_MS="${AI_ASSIST_OPENAI_HTTP_TIMEOUT_MS:-1200}"

TARGET_A_ERROR_RATE_MAX="${TARGET_A_ERROR_RATE_MAX:-0.01}"
TARGET_B_ERROR_RATE_MAX="${TARGET_B_ERROR_RATE_MAX:-0.01}"
TARGET_B_P95_MS_MAX="${TARGET_B_P95_MS_MAX:-900}"
TARGET_P95_DELTA_MS_MAX="${TARGET_P95_DELTA_MS_MAX:-200}"
TARGET_B_FALLBACK_RATE_MAX="${TARGET_B_FALLBACK_RATE_MAX:-0.50}"
ENFORCE_FALLBACK_GATE="${ENFORCE_FALLBACK_GATE:-0}"
TARGET_LLM_USED_RATIO_MIN="${TARGET_LLM_USED_RATIO_MIN:-0.20}"
ENFORCE_LLM_USED_GATE="${ENFORCE_LLM_USED_GATE:-0}"

STAMP="$(date +%Y%m%d_%H%M%S)"
TMP_A="/tmp/ai_assist_ab_${STAMP}_a.out"
TMP_B="/tmp/ai_assist_ab_${STAMP}_b.out"
SUMMARY_OUT="$OUT_DIR/ai_assist_ab_gate_${STAMP}.summary.txt"
mkdir -p "$OUT_DIR"

cd "$ROOT_DIR"

echo "[A] rule-only baseline"
REQUESTS="$REQUESTS" \
REQUEST_TIMEOUT_SEC="$REQUEST_TIMEOUT_SEC" \
BUILD_JAR="$BUILD_JAR" \
OUT_DIR="$OUT_DIR" \
AI_ASSIST_LLM_ENABLED=false \
AI_ASSIST_LLM_PROVIDER=none \
scripts/ops/run_ai_assist_probe.sh | tee "$TMP_A"

a_summary="$(awk -F= '/^\[artifacts\] summary=/{print $2}' "$TMP_A" | tail -n1)"
if [[ -z "$a_summary" || ! -f "$a_summary" ]]; then
  echo "FAIL: A summary not found"
  exit 1
fi

echo "[B] llm-enabled candidate provider=${LLM_PROVIDER_B}"
REQUESTS="$REQUESTS" \
REQUEST_TIMEOUT_SEC="$REQUEST_TIMEOUT_SEC" \
BUILD_JAR=0 \
OUT_DIR="$OUT_DIR" \
AI_ASSIST_LLM_ENABLED=true \
AI_ASSIST_LLM_PROVIDER="$LLM_PROVIDER_B" \
AI_ASSIST_OPENAI_API_KEY="$AI_ASSIST_OPENAI_API_KEY" \
AI_ASSIST_OPENAI_ENDPOINT="$AI_ASSIST_OPENAI_ENDPOINT" \
AI_ASSIST_OPENAI_MODEL="$AI_ASSIST_OPENAI_MODEL" \
AI_ASSIST_OPENAI_TEMPERATURE="$AI_ASSIST_OPENAI_TEMPERATURE" \
AI_ASSIST_OPENAI_MAX_TOKENS="$AI_ASSIST_OPENAI_MAX_TOKENS" \
AI_ASSIST_OPENAI_HTTP_TIMEOUT_MS="$AI_ASSIST_OPENAI_HTTP_TIMEOUT_MS" \
scripts/ops/run_ai_assist_probe.sh | tee "$TMP_B"

b_summary="$(awk -F= '/^\[artifacts\] summary=/{print $2}' "$TMP_B" | tail -n1)"
if [[ -z "$b_summary" || ! -f "$b_summary" ]]; then
  echo "FAIL: B summary not found"
  exit 1
fi

summary_value() {
  local file="$1"
  local key="$2"
  awk -F= -v k="$key" '$1==k {print $2}' "$file" | tail -n1
}

a_p95_ms="$(summary_value "$a_summary" latency_p95_ms)"
a_error_rate="$(summary_value "$a_summary" error_rate)"
a_fallback_rate="$(summary_value "$a_summary" fallback_rate)"
a_success_total="$(summary_value "$a_summary" success_total)"
a_llm_used_total="$(summary_value "$a_summary" llm_used_total)"

b_p95_ms="$(summary_value "$b_summary" latency_p95_ms)"
b_error_rate="$(summary_value "$b_summary" error_rate)"
b_fallback_rate="$(summary_value "$b_summary" fallback_rate)"
b_success_total="$(summary_value "$b_summary" success_total)"
b_llm_used_total="$(summary_value "$b_summary" llm_used_total)"

b_p95_delta_ms="$(awk -v a="$a_p95_ms" -v b="$b_p95_ms" 'BEGIN{printf "%.3f", (b+0)-(a+0)}')"
b_llm_used_ratio="$(awk -v u="$b_llm_used_total" -v s="$b_success_total" 'BEGIN{if (s+0<=0) print "0.000000"; else printf "%.6f", (u+0)/(s+0)}')"

pass_a_error="$(awk -v v="$a_error_rate" -v t="$TARGET_A_ERROR_RATE_MAX" 'BEGIN{print (v+0<=t+0)?1:0}')"
pass_b_error="$(awk -v v="$b_error_rate" -v t="$TARGET_B_ERROR_RATE_MAX" 'BEGIN{print (v+0<=t+0)?1:0}')"
pass_b_p95="$(awk -v v="$b_p95_ms" -v t="$TARGET_B_P95_MS_MAX" 'BEGIN{print (v+0<=t+0)?1:0}')"
pass_b_p95_delta="$(awk -v v="$b_p95_delta_ms" -v t="$TARGET_P95_DELTA_MS_MAX" 'BEGIN{print (v+0<=t+0)?1:0}')"

pass_b_fallback=1
if [[ "$ENFORCE_FALLBACK_GATE" == "1" ]]; then
  pass_b_fallback="$(awk -v v="$b_fallback_rate" -v t="$TARGET_B_FALLBACK_RATE_MAX" 'BEGIN{print (v+0<=t+0)?1:0}')"
fi

pass_b_llm_used=1
if [[ "$ENFORCE_LLM_USED_GATE" == "1" ]]; then
  pass_b_llm_used="$(awk -v v="$b_llm_used_ratio" -v t="$TARGET_LLM_USED_RATIO_MIN" 'BEGIN{print (v+0>=t+0)?1:0}')"
fi

pass_all=$((pass_a_error & pass_b_error & pass_b_p95 & pass_b_p95_delta & pass_b_fallback & pass_b_llm_used))

cat >"$SUMMARY_OUT" <<EOF
ai_assist_ab_gate
date=${STAMP}
requests=${REQUESTS}
request_timeout_sec=${REQUEST_TIMEOUT_SEC}
llm_provider_b=${LLM_PROVIDER_B}
target_a_error_rate_max=${TARGET_A_ERROR_RATE_MAX}
target_b_error_rate_max=${TARGET_B_ERROR_RATE_MAX}
target_b_p95_ms_max=${TARGET_B_P95_MS_MAX}
target_p95_delta_ms_max=${TARGET_P95_DELTA_MS_MAX}
target_b_fallback_rate_max=${TARGET_B_FALLBACK_RATE_MAX}
enforce_fallback_gate=${ENFORCE_FALLBACK_GATE}
target_llm_used_ratio_min=${TARGET_LLM_USED_RATIO_MIN}
enforce_llm_used_gate=${ENFORCE_LLM_USED_GATE}
a_p95_ms=${a_p95_ms}
a_error_rate=${a_error_rate}
a_fallback_rate=${a_fallback_rate}
a_success_total=${a_success_total}
a_llm_used_total=${a_llm_used_total}
b_p95_ms=${b_p95_ms}
b_error_rate=${b_error_rate}
b_fallback_rate=${b_fallback_rate}
b_success_total=${b_success_total}
b_llm_used_total=${b_llm_used_total}
b_p95_delta_ms=${b_p95_delta_ms}
b_llm_used_ratio=${b_llm_used_ratio}
gate_pass_a_error=${pass_a_error}
gate_pass_b_error=${pass_b_error}
gate_pass_b_p95=${pass_b_p95}
gate_pass_b_p95_delta=${pass_b_p95_delta}
gate_pass_b_fallback=${pass_b_fallback}
gate_pass_b_llm_used=${pass_b_llm_used}
gate_pass=${pass_all}
a_summary=${a_summary}
b_summary=${b_summary}
EOF

echo "[summary] A p95=${a_p95_ms}ms err=${a_error_rate} fallback=${a_fallback_rate}"
echo "[summary] B p95=${b_p95_ms}ms err=${b_error_rate} fallback=${b_fallback_rate} llm_used_ratio=${b_llm_used_ratio}"
echo "[summary] delta p95=${b_p95_delta_ms}ms provider=${LLM_PROVIDER_B}"
echo "[artifacts] ${SUMMARY_OUT}"

if [[ "$pass_all" == "1" ]]; then
  echo "PASS: ai-assist A/B gate satisfied"
  exit 0
fi

echo "FAIL: ai-assist A/B gate not satisfied"
echo "  - A error_rate <= ${TARGET_A_ERROR_RATE_MAX}: ${pass_a_error}"
echo "  - B error_rate <= ${TARGET_B_ERROR_RATE_MAX}: ${pass_b_error}"
echo "  - B p95_ms <= ${TARGET_B_P95_MS_MAX}: ${pass_b_p95}"
echo "  - B p95_delta_ms <= ${TARGET_P95_DELTA_MS_MAX}: ${pass_b_p95_delta}"
if [[ "$ENFORCE_FALLBACK_GATE" == "1" ]]; then
  echo "  - B fallback_rate <= ${TARGET_B_FALLBACK_RATE_MAX}: ${pass_b_fallback}"
fi
if [[ "$ENFORCE_LLM_USED_GATE" == "1" ]]; then
  echo "  - B llm_used_ratio >= ${TARGET_LLM_USED_RATIO_MIN}: ${pass_b_llm_used}"
fi
exit 1

