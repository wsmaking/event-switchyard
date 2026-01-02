#!/usr/bin/env bash
# 長時間安定性テストスクリプト
#
# 目的: HFT Fast Pathの長時間運用時の安定性を検証
# - メモリリーク検出
# - レイテンシ劣化検出
# - エラー率監視
# - スループット維持確認

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# テスト設定
TEST_DURATION_MINUTES=${TEST_DURATION_MINUTES:-60}  # デフォルト: 60分
REQUEST_RATE_PER_SEC=${REQUEST_RATE_PER_SEC:-100}   # デフォルト: 100 req/s
STATS_INTERVAL_SEC=${STATS_INTERVAL_SEC:-10}        # デフォルト: 10秒ごと
LOG_DIR="${PROJECT_ROOT}/var/stability_test"

# ログディレクトリ作成
mkdir -p "$LOG_DIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
METRICS_LOG="${LOG_DIR}/metrics_${TIMESTAMP}.jsonl"
SUMMARY_LOG="${LOG_DIR}/summary_${TIMESTAMP}.txt"

echo "===== HFT Fast Path 長時間安定性テスト ====="
echo "テスト時間: ${TEST_DURATION_MINUTES}分"
echo "リクエスト頻度: ${REQUEST_RATE_PER_SEC} req/s"
echo "メトリクス収集間隔: ${STATS_INTERVAL_SEC}秒"
echo "メトリクスログ: ${METRICS_LOG}"
echo "サマリーログ: ${SUMMARY_LOG}"
echo ""

# アプリケーションが起動していることを確認
if ! curl -s http://localhost:8080/stats > /dev/null 2>&1; then
    echo "ERROR: アプリケーションが起動していません"
    echo "次のコマンドで起動してください:"
    echo "  FAST_PATH_ENABLE=1 FAST_PATH_METRICS=1 KAFKA_BRIDGE_ENABLE=1 ./gradlew run"
    exit 1
fi

echo "アプリケーション起動確認 OK"
echo ""

# 初期メトリクス取得
INITIAL_STATS=$(curl -s http://localhost:8080/stats)
echo "初期メトリクス:"
echo "$INITIAL_STATS" | python3 -m json.tool
echo ""

# テスト開始
START_TIME=$(date +%s)
END_TIME=$((START_TIME + TEST_DURATION_MINUTES * 60))

echo "テスト開始: $(date)"
echo ""

REQUEST_COUNT=0
STATS_COUNT=0

# メトリクス収集関数
collect_metrics() {
    local timestamp=$(date +%s)
    local elapsed=$((timestamp - START_TIME))
    local stats=$(curl -s http://localhost:8080/stats)

    # メトリクスをJSONL形式で保存
    echo "{\"timestamp\": $timestamp, \"elapsed_sec\": $elapsed, \"stats\": $stats}" >> "$METRICS_LOG"

    # 主要メトリクスを表示
    local p99=$(echo "$stats" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('fast_path_process_p99_us', 0))")
    local error_count=$(echo "$stats" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('persistence_queue_error_count', 0))")
    local drop_count=$(echo "$stats" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('fast_path_drop_count', 0))")

    printf "[%5ds] p99=%.2fμs errors=%d drops=%d requests=%d\n" \
        "$elapsed" "$p99" "$error_count" "$drop_count" "$REQUEST_COUNT"
}

# バックグラウンドでリクエスト送信
send_requests() {
    local interval=$(awk "BEGIN {print 1.0 / $REQUEST_RATE_PER_SEC}")

    while [ $(date +%s) -lt $END_TIME ]; do
        curl -s -X POST "http://localhost:8080/events?key=BTC" \
            -H "Content-Type: application/json" \
            -d '{"price": 50000.00, "volume": 1.5}' > /dev/null 2>&1 || true

        REQUEST_COUNT=$((REQUEST_COUNT + 1))
        sleep "$interval"
    done
}

# リクエスト送信を開始 (バックグラウンド)
send_requests &
REQUEST_PID=$!

# メトリクス収集ループ
while [ $(date +%s) -lt $END_TIME ]; do
    collect_metrics
    STATS_COUNT=$((STATS_COUNT + 1))
    sleep $STATS_INTERVAL_SEC
done

# リクエスト送信を停止
kill $REQUEST_PID 2>/dev/null || true
wait $REQUEST_PID 2>/dev/null || true

echo ""
echo "テスト終了: $(date)"
echo ""

# 最終メトリクス取得
sleep 2  # バッファフラッシュ待ち
FINAL_STATS=$(curl -s http://localhost:8080/stats)

echo "最終メトリクス:"
echo "$FINAL_STATS" | python3 -m json.tool
echo ""

# サマリー生成
cat > "$SUMMARY_LOG" <<EOF
===== HFT Fast Path 長時間安定性テスト サマリー =====

テスト時刻: $(date)
テスト時間: ${TEST_DURATION_MINUTES}分
リクエスト頻度: ${REQUEST_RATE_PER_SEC} req/s
総リクエスト数: ${REQUEST_COUNT}

初期メトリクス:
$(echo "$INITIAL_STATS" | python3 -m json.tool)

最終メトリクス:
$(echo "$FINAL_STATS" | python3 -m json.tool)

詳細メトリクス: ${METRICS_LOG}
EOF

echo "サマリー保存: ${SUMMARY_LOG}"
echo ""

# 分析結果表示
echo "===== 分析結果 ====="

INITIAL_P99=$(echo "$INITIAL_STATS" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('fast_path_process_p99_us', 0))" 2>/dev/null || echo "0")
FINAL_P99=$(echo "$FINAL_STATS" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('fast_path_process_p99_us', 0))" 2>/dev/null || echo "0")
FINAL_ERRORS=$(echo "$FINAL_STATS" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('persistence_queue_error_count', 0))" 2>/dev/null || echo "0")
FINAL_DROPS=$(echo "$FINAL_STATS" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('fast_path_drop_count', 0))" 2>/dev/null || echo "0")

echo "p99レイテンシ: ${INITIAL_P99}μs → ${FINAL_P99}μs"
echo "エラー数: ${FINAL_ERRORS}"
echo "ドロップ数: ${FINAL_DROPS}"
echo ""

# 判定
P99_DEGRADATION=$(awk "BEGIN {print ($FINAL_P99 - $INITIAL_P99) / $INITIAL_P99 * 100}")
echo "p99劣化率: ${P99_DEGRADATION}%"

if (( $(echo "$FINAL_P99 > 100" | bc -l) )); then
    echo "⚠️  警告: p99が100μs目標を超過"
fi

if [ "$FINAL_ERRORS" -gt 0 ]; then
    echo "⚠️  警告: エラーが${FINAL_ERRORS}件発生"
fi

if [ "$FINAL_DROPS" -gt 0 ]; then
    echo "⚠️  警告: ${FINAL_DROPS}件のイベントがドロップ"
fi

if (( $(echo "$P99_DEGRADATION > 10" | bc -l) )); then
    echo "⚠️  警告: p99が10%以上劣化"
fi

if [ "$FINAL_ERRORS" -eq 0 ] && [ "$FINAL_DROPS" -eq 0 ] && (( $(echo "$P99_DEGRADATION < 10" | bc -l) )) && (( $(echo "$FINAL_P99 < 100" | bc -l) )); then
    echo ""
    echo "✅ テスト合格: 安定性良好"
else
    echo ""
    echo "❌ テスト不合格: 問題あり"
fi

echo ""
echo "詳細: ${SUMMARY_LOG}"
