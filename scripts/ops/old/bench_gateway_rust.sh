#!/bin/bash
# gateway-rust ベンチマーク
# Full Rust 実装のレイテンシ測定

set -e
cd "$(dirname "$0")/../.."

PORT=8091
WARMUP=1000
REQUESTS=10000

echo "=== Gateway Rust Benchmark ==="
echo "Warmup: $WARMUP, Requests: $REQUESTS"
echo ""

# サーバー起動
echo "Starting gateway-rust on port $PORT..."
GATEWAY_PORT=$PORT ./gateway-rust/target/release/gateway-rust &
SERVER_PID=$!
sleep 2

# 起動確認
if ! curl -s http://localhost:$PORT/health > /dev/null; then
    echo "ERROR: Server failed to start"
    kill $SERVER_PID 2>/dev/null
    exit 1
fi
echo "Server started (PID: $SERVER_PID)"

# ウォームアップ
echo ""
echo "Warming up ($WARMUP requests)..."
for i in $(seq 1 $WARMUP); do
    curl -s -X POST http://localhost:$PORT/api/orders \
        -H "Content-Type: application/json" \
        -d "{\"accountId\":$i,\"symbol\":\"AAPL\",\"side\":\"BUY\",\"qty\":100,\"price\":15000}" > /dev/null
done

# レイテンシリセット
echo "Resetting latency histogram..."
# gateway-rust にはリセットAPIがないため、ヒストグラムはウォームアップ込みで計測

# ベンチマーク
echo ""
echo "Running benchmark ($REQUESTS requests)..."
LATENCIES=()
for i in $(seq 1 $REQUESTS); do
    RESPONSE=$(curl -s -X POST http://localhost:$PORT/api/orders \
        -H "Content-Type: application/json" \
        -d "{\"accountId\":$((i+WARMUP)),\"symbol\":\"AAPL\",\"side\":\"BUY\",\"qty\":100,\"price\":15000}")
    LATENCY=$(echo "$RESPONSE" | grep -o '"latencyNs":[0-9]*' | cut -d: -f2)
    if [ -n "$LATENCY" ]; then
        LATENCIES+=($LATENCY)
    fi

    # 進捗表示
    if [ $((i % 1000)) -eq 0 ]; then
        echo "  Progress: $i / $REQUESTS"
    fi
done

# 統計計算
echo ""
echo "Calculating statistics..."
SORTED=($(printf '%s\n' "${LATENCIES[@]}" | sort -n))
COUNT=${#SORTED[@]}

if [ $COUNT -gt 0 ]; then
    P50_IDX=$((COUNT * 50 / 100))
    P99_IDX=$((COUNT * 99 / 100))
    MIN=${SORTED[0]}
    MAX=${SORTED[$((COUNT-1))]}
    P50=${SORTED[$P50_IDX]}
    P99=${SORTED[$P99_IDX]}

    # 平均計算
    SUM=0
    for L in "${LATENCIES[@]}"; do
        SUM=$((SUM + L))
    done
    AVG=$((SUM / COUNT))

    echo ""
    echo "=== Results (E2E HTTP latency) ==="
    echo "  Count: $COUNT"
    echo "  Min:   $MIN ns"
    echo "  Avg:   $AVG ns"
    echo "  P50:   $P50 ns"
    echo "  P99:   $P99 ns"
    echo "  Max:   $MAX ns"

    # サーバー側統計
    echo ""
    echo "=== Server-side metrics ==="
    curl -s http://localhost:$PORT/health | python3 -c "
import sys, json
data = json.load(sys.stdin)
print(f\"  Queue len:    {data['queueLen']}\")
print(f\"  Latency P50:  {data['latencyP50Ns']} ns\")
print(f\"  Latency P99:  {data['latencyP99Ns']} ns\")
"
else
    echo "ERROR: No latency data collected"
fi

# クリーンアップ
echo ""
echo "Stopping server..."
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null || true
echo "Done."
