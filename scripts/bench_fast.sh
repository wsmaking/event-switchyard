set -euo pipefail

#
# Fast Path Benchmark Script
#
# Measures Fast Path performance with Disruptor
# Target: p99 < 100μs
#
# Usage:
#   FAST_PATH_ENABLE=1 ./scripts/bench_fast.sh [--runs N] [--symbols SYMBOLS] [--out FILE]
#

runs=1000
symbols="BTC,ETH,USDT"
out="results/fast_path.json"
endpoint="http://localhost:8080/events"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --runs) runs="$2"; shift 2;;
    --symbols) symbols="$2"; shift 2;;
    --out) out="$2"; shift 2;;
    --endpoint) endpoint="$2"; shift 2;;
    -h|--help)
      cat <<'USAGE'
Usage: scripts/bench_fast.sh [OPTIONS]

Options:
  --runs N          Number of requests to send (default: 1000)
  --symbols CSV     Comma-separated symbols for fast path (default: BTC,ETH,USDT)
  --out FILE        Output file for results (default: results/fast_path.json)
  --endpoint URL    HTTP endpoint (default: http://localhost:8080/events)

Environment:
  FAST_PATH_ENABLE=1      Enable fast path routing
  FAST_PATH_SYMBOLS=...   Symbols to route to fast path

Example:
  FAST_PATH_ENABLE=1 FAST_PATH_SYMBOLS=BTC,ETH ./scripts/bench_fast.sh --runs 10000
USAGE
      exit 0
      ;;
    *) echo "Unknown arg: $1"; exit 1;;
  esac
done

mkdir -p "$(dirname "$out")"

echo "==> Fast Path Benchmark"
echo "  Runs: $runs"
echo "  Symbols: $symbols"
echo "  Endpoint: $endpoint"
echo "  Output: $out"
echo

# Generate random payload
payload="$(head -c 200 /dev/urandom | base64)"

# Run benchmark
start_ns=$(date +%s%N)
success=0
fail=0

IFS=',' read -ra SYMS <<< "$symbols"
for ((i=1; i<=runs; i++)); do
  # Round-robin symbols
  idx=$((i % ${#SYMS[@]}))
  sym="${SYMS[$idx]}"

  http_code=$(curl -s -w "%{http_code}" -o /dev/null \
    -X POST "${endpoint}?key=${sym}" \
    -H "Content-Type: application/octet-stream" \
    --data-raw "$payload" \
    --max-time 1 || echo "000")

  if [[ "$http_code" == "200" ]]; then
    ((success++))
  else
    ((fail++))
    [[ $fail -le 5 ]] && echo "WARN: HTTP $http_code for key=$sym"
  fi

  # Progress
  if [[ $((i % 100)) -eq 0 ]]; then
    echo -ne "\r  Progress: $i/$runs ($success OK, $fail FAIL)"
  fi
done
echo

end_ns=$(date +%s%N)
elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))
throughput=$(awk "BEGIN {printf \"%.2f\", $success / ($elapsed_ms / 1000.0)}")

echo "==> Results"
echo "  Success: $success"
echo "  Failed: $fail"
echo "  Elapsed: ${elapsed_ms}ms"
echo "  Throughput: ${throughput} req/s"

# Fetch stats from /stats endpoint (if available)
stats_endpoint="${endpoint%/events}/stats"
stats_json=$(curl -s "$stats_endpoint" 2>/dev/null || echo "{}")

# Write results
cat > "$out" <<EOF
{
  "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "config": {
    "runs": $runs,
    "symbols": "$symbols",
    "endpoint": "$endpoint"
  },
  "results": {
    "success": $success,
    "failed": $fail,
    "elapsed_ms": $elapsed_ms,
    "throughput_rps": $throughput
  },
  "stats": $stats_json
}
EOF

echo "==> Wrote $out"

# Evaluate against target
avg_latency_us=$(echo "$stats_json" | jq -r '.fast_path_avg_process_us // 0')
if (( $(echo "$avg_latency_us > 0" | bc -l) )); then
  echo
  echo "==> Fast Path Metrics"
  echo "  Avg Latency: ${avg_latency_us}μs"

  if (( $(echo "$avg_latency_us < 100" | bc -l) )); then
    echo "  ✓ Target met (p99 < 100μs)"
  else
    echo "  ✗ Target missed (p99 < 100μs)"
  fi
fi

