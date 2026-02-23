#!/usr/bin/env bash
set -euo pipefail

OUT_PATH="${1:-}"
STRICT="${STRICT:-0}"
CPU_THRESHOLD="${CPU_THRESHOLD:-35}"

if [[ -n "$OUT_PATH" ]]; then
  mkdir -p "$(dirname "$OUT_PATH")"
fi

OS="$(uname -s)"
TS="$(date +%Y-%m-%dT%H:%M:%S%z)"
HOST="$(hostname 2>/dev/null || echo unknown)"

snapshot() {
  {
    echo "perf_noise_guard"
    echo "timestamp=${TS}"
    echo "host=${HOST}"
    echo "os=${OS}"
    echo "strict=${STRICT}"
    echo "cpu_threshold=${CPU_THRESHOLD}"
    echo ""
    echo "[top_cpu_processes]"
    ps -axo pid,%cpu,command | sort -k2 -nr | awk 'NR<=25'
    echo ""
    echo "[load_average]"
    if command -v uptime >/dev/null 2>&1; then
      uptime
    else
      echo "uptime_unavailable"
    fi
  } >"${OUT_PATH:-/dev/stdout}"
}

if [[ -n "$OUT_PATH" ]]; then
  snapshot
else
  snapshot >/dev/stdout
fi

# strict時のみ、他プロセスCPU過大をfailにする。
if [[ "$STRICT" == "1" ]]; then
  offenders="$(
    ps -axo %cpu,command | awk -v th="$CPU_THRESHOLD" '
      NR==1 { next }
      {
        cpu=$1+0
        cmd=""
        for (i=2; i<=NF; i++) {
          cmd = cmd $i " "
        }
        if (cpu >= th &&
            cmd !~ /gateway-rust/ &&
            cmd !~ /wrk/ &&
            cmd !~ /curl/ &&
            cmd !~ /ps -axo/ &&
            cmd !~ /sort -k2/ &&
            cmd !~ /head -n/ &&
            cmd !~ /perf_noise_guard/ ) {
          print cpu "\t" cmd
        }
      }
    '
  )"
  if [[ -n "$offenders" ]]; then
    echo "FAIL: noisy background processes detected (STRICT=1, CPU_THRESHOLD=${CPU_THRESHOLD})" >&2
    echo "$offenders" >&2
    exit 1
  fi
fi
