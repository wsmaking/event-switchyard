#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/var/results}"
RUNS="${RUNS:-1}"
DURATION="${DURATION:-1800}"

TARGET_ACK_ACCEPTED_P999_US="${TARGET_ACK_ACCEPTED_P999_US:-80}"
TARGET_DURABLE_CONFIRM_P999_US="${TARGET_DURABLE_CONFIRM_P999_US:-250000000}"

mkdir -p "$OUT_DIR"
cd "$ROOT_DIR"

STAMP="$(date +%Y%m%d_%H%M%S)"
RUN_LOG="$OUT_DIR/v3_long_soak_gate_${STAMP}.log"
SUMMARY="$OUT_DIR/v3_long_soak_gate_${STAMP}.summary.txt"
RAW_TSV="$OUT_DIR/v3_long_soak_gate_${STAMP}.tsv"

echo -e "run\trc\tack_accepted_p99_us\tack_accepted_p999_us\tdurable_confirm_p99_us\tdurable_confirm_p999_us\tsummary_path" >"$RAW_TSV"

set +e
RUNS="$RUNS" DURATION="$DURATION" scripts/ops/check_v3_local_strict_gate.sh >"$RUN_LOG" 2>&1
strict_rc=$?
set -e

loop_summary="$(rg -o '/Users/[^ ]+v3_local_strict_gate_[0-9_]+\.summary\.txt' "$RUN_LOG" | tail -n1 || true)"
raw_tsv=""
if [[ -n "$loop_summary" && -f "$loop_summary" ]]; then
  raw_tsv="$(awk -F= '/^raw_tsv=/{print $2}' "$loop_summary" | tail -n1)"
fi

if [[ -z "$raw_tsv" || ! -f "$raw_tsv" ]]; then
  cat <<EOF >"$SUMMARY"
v3_long_soak_gate
strict_rc=${strict_rc}
error=missing_raw_tsv
run_log=${RUN_LOG}
EOF
  echo "FAIL: missing raw_tsv in strict gate output (log=${RUN_LOG})"
  exit 1
fi

pass_ack_p999=1
pass_durable_p999=1
total_runs=0

while IFS=$'\t' read -r run rc _ _ ack_p99 durable_p99 summary_path; do
  [[ -z "${run:-}" ]] && continue
  total_runs=$((total_runs + 1))
  ack_p999=0
  durable_p999=0
  if [[ -n "$summary_path" && -f "$summary_path" ]]; then
    ack_p999="$(awk -F= '/^server_live_ack_accepted_p999_us=/{print $2}' "$summary_path" | tail -n1)"
    durable_p999="$(awk -F= '/^server_durable_confirm_p999_us=/{print $2}' "$summary_path" | tail -n1)"
    ack_p999="${ack_p999:-0}"
    durable_p999="${durable_p999:-0}"
  fi

  ack_ok="$(awk -v v="$ack_p999" -v t="$TARGET_ACK_ACCEPTED_P999_US" 'BEGIN{print (v+0<=t+0)?1:0}')"
  durable_ok="$(awk -v v="$durable_p999" -v t="$TARGET_DURABLE_CONFIRM_P999_US" 'BEGIN{print (v+0<=t+0)?1:0}')"
  if [[ "$ack_ok" != "1" ]]; then pass_ack_p999=0; fi
  if [[ "$durable_ok" != "1" ]]; then pass_durable_p999=0; fi

  echo -e "${run}\t${rc}\t${ack_p99}\t${ack_p999}\t${durable_p99}\t${durable_p999}\t${summary_path}" >>"$RAW_TSV"
done < <(tail -n +2 "$raw_tsv")

if [[ "$total_runs" -eq 0 ]]; then
  cat <<EOF >"$SUMMARY"
v3_long_soak_gate
strict_rc=${strict_rc}
error=no_runs_parsed
raw_tsv=${raw_tsv}
run_log=${RUN_LOG}
EOF
  echo "FAIL: no runs parsed from ${raw_tsv}"
  exit 1
fi

gate_pass=$(( (strict_rc == 0 ? 1 : 0) & pass_ack_p999 & pass_durable_p999 ))

cat <<EOF >"$SUMMARY"
v3_long_soak_gate
strict_rc=${strict_rc}
runs=${total_runs}
target_ack_accepted_p999_us=${TARGET_ACK_ACCEPTED_P999_US}
target_durable_confirm_p999_us=${TARGET_DURABLE_CONFIRM_P999_US}
pass_ack_accepted_p999=${pass_ack_p999}
pass_durable_confirm_p999=${pass_durable_p999}
gate_pass=${gate_pass}
raw_tsv=${RAW_TSV}
strict_raw_tsv=${raw_tsv}
strict_log=${RUN_LOG}
EOF

echo "[artifacts] summary=${SUMMARY}"
echo "[artifacts] raw=${RAW_TSV}"
echo "[artifacts] strict_log=${RUN_LOG}"

if [[ "$gate_pass" == "1" ]]; then
  echo "PASS: long soak gate satisfied"
  exit 0
fi

echo "FAIL: long soak gate failed"
exit 1
