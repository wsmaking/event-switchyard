#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/var/results}"
STAMP="$(date +%Y%m%d_%H%M%S)"
WAIT_LOG="${WAIT_LOG:-$OUT_DIR/v3_quiet_wait_${STAMP}.log}"
PRE_NOISE="${PRE_NOISE:-$OUT_DIR/v3_quiet_wait_${STAMP}.noise.txt}"

WAIT_MAX_SEC="${WAIT_MAX_SEC:-300}"
POLL_SEC="${POLL_SEC:-2}"
STABLE_POLLS="${STABLE_POLLS:-5}"
MDS_CPU_MAX="${MDS_CPU_MAX:-20}"
CHROME_CPU_MAX="${CHROME_CPU_MAX:-5}"
RUN_ON_TIMEOUT="${RUN_ON_TIMEOUT:-0}"

if ! [[ "$WAIT_MAX_SEC" =~ ^[0-9]+$ ]] || [[ "$WAIT_MAX_SEC" -le 0 ]]; then
  echo "FAIL: WAIT_MAX_SEC must be a positive integer (current: $WAIT_MAX_SEC)"
  exit 1
fi
if ! [[ "$POLL_SEC" =~ ^[0-9]+$ ]] || [[ "$POLL_SEC" -le 0 ]]; then
  echo "FAIL: POLL_SEC must be a positive integer (current: $POLL_SEC)"
  exit 1
fi
if ! [[ "$STABLE_POLLS" =~ ^[0-9]+$ ]] || [[ "$STABLE_POLLS" -le 0 ]]; then
  echo "FAIL: STABLE_POLLS must be a positive integer (current: $STABLE_POLLS)"
  exit 1
fi

mkdir -p "$OUT_DIR"
cd "$ROOT_DIR"

scripts/ops/perf_noise_guard.sh "$PRE_NOISE" >/dev/null

max_polls=$(( (WAIT_MAX_SEC + POLL_SEC - 1) / POLL_SEC ))
stable=0
ready=0

{
  echo "v3_quiet_wait"
  echo "started_at=$(date +%Y-%m-%dT%H:%M:%S%z)"
  echo "wait_max_sec=$WAIT_MAX_SEC"
  echo "poll_sec=$POLL_SEC"
  echo "stable_polls=$STABLE_POLLS"
  echo "mds_cpu_max=$MDS_CPU_MAX"
  echo "chrome_cpu_max=$CHROME_CPU_MAX"
  echo "run_on_timeout=$RUN_ON_TIMEOUT"
  echo "pre_noise_snapshot=$PRE_NOISE"
  echo ""
} >"$WAIT_LOG"

for i in $(seq 1 "$max_polls"); do
  read -r mds_cpu chrome_cpu < <(
    ps -axo pid,%cpu,command | awk '
      /Metadata.framework\/Versions\/A\/Support\/mds( |$)/ {mds+=$2}
      /Metadata.framework\/Versions\/A\/Support\/mds_stores( |$)/ {mds+=$2}
      /Metadata.framework\/Versions\/A\/Support\/mdworker( |$)/ {mds+=$2}
      /Metadata.framework\/Versions\/A\/Support\/mdworker_shared( |$)/ {mds+=$2}
      /Google Chrome/ {chrome+=$2}
      /Chrome Helper/ {chrome+=$2}
      END {printf "%.1f %.1f\n", mds+0, chrome+0}
    '
  )

  ok="$(awk -v m="$mds_cpu" -v c="$chrome_cpu" -v mm="$MDS_CPU_MAX" -v cm="$CHROME_CPU_MAX" \
    'BEGIN {print ((m+0)<=mm && (c+0)<=cm) ? 1 : 0}')"
  if [[ "$ok" == "1" ]]; then
    stable=$((stable + 1))
  else
    stable=0
  fi

  line="$(printf "%s poll=%03d/%03d mds_cpu=%s chrome_cpu=%s stable=%d" \
    "$(date +%Y-%m-%dT%H:%M:%S%z)" "$i" "$max_polls" "$mds_cpu" "$chrome_cpu" "$stable")"
  echo "$line" | tee -a "$WAIT_LOG"

  if [[ "$stable" -ge "$STABLE_POLLS" ]]; then
    ready=1
    break
  fi
  sleep "$POLL_SEC"
done

if [[ "$ready" == "1" ]]; then
  echo "result=READY" | tee -a "$WAIT_LOG"
else
  echo "result=NOT_READY" | tee -a "$WAIT_LOG"
  if [[ "$RUN_ON_TIMEOUT" != "1" ]]; then
    echo "FAIL: quiet condition was not reached. log=$WAIT_LOG"
    exit 3
  fi
fi

echo "run=run_v3_phase2_compare.sh" | tee -a "$WAIT_LOG"
echo "INFO: launching benchmark (log: $WAIT_LOG)"
scripts/ops/run_v3_phase2_compare.sh
