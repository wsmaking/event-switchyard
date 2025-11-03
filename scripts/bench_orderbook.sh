#!/usr/bin/env bash
set -euo pipefail

#
# Order Book Benchmark Script
#
# Measures Fast Path performance with Order Matching logic
# Target: p99 < 100μs
#
# Payload Format (8 bytes):
#   [0]: side (0=BUY, 1=SELL)
#   [1-3]: price (24-bit int)
#   [4-7]: quantity (32-bit int)
#
# Usage:
#   FAST_PATH_ENABLE=1 FAST_PATH_METRICS=1 ./scripts/bench_orderbook.sh [OPTIONS]
#

runs=10000
symbols="BTC"
out="results/orderbook_bench.json"
endpoint="http://localhost:8080/events"
price_min=50000
price_max=51000
quantity_min=1
quantity_max=100

while [[ $# -gt 0 ]]; do
  case "$1" in
    --runs) runs="$2"; shift 2;;
    --symbols) symbols="$2"; shift 2;;
    --out) out="$2"; shift 2;;
    --endpoint) endpoint="$2"; shift 2;;
    -h|--help)
      cat <<'USAGE'
Usage: scripts/bench_orderbook.sh [OPTIONS]

Options:
  --runs N          Number of orders to send (default: 10000)
  --symbols CSV     Comma-separated symbols (default: BTC)
  --out FILE        Output file for results (default: results/orderbook_bench.json)
  --endpoint URL    HTTP endpoint (default: http://localhost:8080/events)

Environment:
  FAST_PATH_ENABLE=1      Enable fast path routing
  FAST_PATH_METRICS=1     Enable metrics collection

Example:
  FAST_PATH_ENABLE=1 FAST_PATH_METRICS=1 ./scripts/bench_orderbook.sh --runs 50000
USAGE
      exit 0
      ;;
    *) echo "Unknown arg: $1"; exit 1;;
  esac
done

mkdir -p "$(dirname "$out")"

echo "==> Order Book Benchmark"
echo "  Runs: $runs"
echo "  Symbols: $symbols"
echo "  Endpoint: $endpoint"
echo "  Price range: $price_min - $price_max"
echo "  Output: $out"
echo

# Python script to generate 8-byte order payload
gen_order_payload() {
  python3 <<EOF
import sys
import random
import struct

side = random.randint(0, 1)  # 0=BUY, 1=SELL
price = random.randint($price_min, $price_max)
quantity = random.randint($quantity_min, $quantity_max)

# Pack as 8 bytes: side(1) + price(3) + quantity(4)
payload = struct.pack('B', side)  # side
payload += struct.pack('>I', price)[1:]  # price (24-bit, big-endian, skip first byte)
payload += struct.pack('>I', quantity)  # quantity (32-bit, big-endian)

sys.stdout.buffer.write(payload)
EOF
}

# Run benchmark
start_ns=$(date +%s%N)
success=0
fail=0

IFS=',' read -ra SYMS <<< "$symbols"
tmpfile=$(mktemp)
trap "rm -f $tmpfile" EXIT

for ((i=1; i<=runs; i++)); do
  # Round-robin symbols
  idx=$((i % ${#SYMS[@]}))
  sym="${SYMS[$idx]}"

  # Generate random order payload (8 bytes) to temp file
  gen_order_payload > "$tmpfile"

  http_code=$(curl -s -w "%{http_code}" -o /dev/null \
    -X POST "${endpoint}?key=${sym}" \
    -H "Content-Type: application/octet-stream" \
    --data-binary "@$tmpfile" \
    --max-time 1 || echo "000")

  if [[ "$http_code" == "200" ]]; then
    ((success++))
  else
    ((fail++))
    [[ $fail -le 5 ]] && echo "WARN: HTTP $http_code for key=$sym"
  fi

  # Progress
  if [[ $((i % 1000)) -eq 0 ]]; then
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

# Fetch stats from /stats endpoint
stats_endpoint="${endpoint%/events}/stats"
stats_json=$(curl -s "$stats_endpoint" 2>/dev/null || echo "{}")

# Extract key metrics
fast_path_count=$(echo "$stats_json" | jq -r '.fast_path_count // 0')
slow_path_count=$(echo "$stats_json" | jq -r '.slow_path_count // 0')
fallback_count=$(echo "$stats_json" | jq -r '.fallback_count // 0')
avg_publish_us=$(echo "$stats_json" | jq -r '.fast_path_avg_publish_us // 0')
avg_process_us=$(echo "$stats_json" | jq -r '.fast_path_avg_process_us // 0')
p50_us=$(echo "$stats_json" | jq -r '.fast_path_process_p50_us // 0')
p99_us=$(echo "$stats_json" | jq -r '.fast_path_process_p99_us // 0')
p999_us=$(echo "$stats_json" | jq -r '.fast_path_process_p999_us // 0')
drop_count=$(echo "$stats_json" | jq -r '.fast_path_drop_count // 0')

echo
echo "==> Fast Path Metrics"
echo "  Fast Path Count: $fast_path_count"
echo "  Slow Path Count: $slow_path_count"
echo "  Fallback Count: $fallback_count"
echo "  Drop Count: $drop_count"
echo "  Avg Publish Latency: ${avg_publish_us}μs"
echo "  Avg Process Latency: ${avg_process_us}μs"
echo "  P50 Latency: ${p50_us}μs"
echo "  P99 Latency: ${p99_us}μs"
echo "  P999 Latency: ${p999_us}μs"

# Write results
cat > "$out" <<EOF
{
  "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "config": {
    "runs": $runs,
    "symbols": "$symbols",
    "endpoint": "$endpoint",
    "price_range": [$price_min, $price_max]
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

echo
echo "==> Wrote $out"

# Evaluate against target (p99 < 100μs)
echo
echo "==> Performance Evaluation"
if (( $(echo "$p99_us > 0" | bc -l) )); then
  if (( $(echo "$p99_us < 100" | bc -l) )); then
    echo "  ✓ Target met: p99 = ${p99_us}μs < 100μs"
  else
    echo "  ✗ Target missed: p99 = ${p99_us}μs >= 100μs"
  fi
else
  echo "  ! No metrics available (ensure FAST_PATH_METRICS=1)"
fi

if (( drop_count > 0 )); then
  echo "  ⚠ Warning: $drop_count events dropped (buffer full)"
fi

if (( fallback_count > 0 )); then
  echo "  ⚠ Warning: $fallback_count events fell back to Slow Path"
fi
