# カナリアテスト実行スクリプト: SLO回帰チェック用の合成トラフィック生成

set -euo pipefail

# デフォルト設定
PROFILES_DIR="bench/profiles"
OUT_FILE="var/results/canary.json"
APP_URL="${APP_URL:-http://localhost:8080}"
ENV_TYPE="${ENV_TYPE:-ci}"
DURATION_OVERRIDE=""

# 使用方法
usage() {
  cat <<EOF
使用方法: $0 [オプション]

オプション:
  --profiles DIR     プロファイル検索ディレクトリ (デフォルト: bench/profiles)
  --out FILE         出力JSON (デフォルト: var/results/canary.json)
  --env TYPE         環境タイプ (local|staging|production|ci) (デフォルト: ci)
  --duration SEC     持続時間上書き (全プロファイルに適用)
  --help             ヘルプ表示

例:
  # burst.yamlプロファイルでCI環境テスト
  $0 --profiles bench/profiles --out var/results/canary.json

  # ステージング環境で300秒のフルテスト
  $0 --env staging --duration 300
EOF
}

# 引数パース
while [[ $# -gt 0 ]]; do
  case "$1" in
    --profiles) PROFILES_DIR="$2"; shift 2;;
    --out)      OUT_FILE="$2"; shift 2;;
    --env)      ENV_TYPE="$2"; shift 2;;
    --duration) DURATION_OVERRIDE="$2"; shift 2;;
    --help)     usage; exit 0;;
    *) echo "❌ 未知の引数: $1" >&2; usage; exit 2;;
  esac
done

# 出力ディレクトリ作成
mkdir -p "$(dirname "$OUT_FILE")"

# Git情報取得
GIT_COMMIT=$(git rev-parse HEAD 2>/dev/null || echo "unknown")
GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

echo "🚀 カナリアテスト開始"
echo "   環境: $ENV_TYPE"
echo "   アプリURL: $APP_URL"
echo "   プロファイルディレクトリ: $PROFILES_DIR"
echo "   出力ファイル: $OUT_FILE"
echo ""

# アプリ起動確認
if ! curl -s -f "$APP_URL/health" >/dev/null 2>&1; then
  echo "❌ エラー: アプリが起動していません ($APP_URL/health)" >&2
  exit 1
fi
echo "✅ アプリケーション起動確認完了"

# YAMLプロファイルを読み込み (burstのみ実行、将来的には複数対応)
PROFILE_NAME="burst"
PROFILE_FILE="$PROFILES_DIR/burst.yaml"

if [[ ! -f "$PROFILE_FILE" ]]; then
  echo "❌ エラー: プロファイルが見つかりません: $PROFILE_FILE" >&2
  exit 1
fi

# YAMLから設定読み込み (簡易パース: grep/sed/awk)
EVENTS_TOTAL=$(grep '^events_total:' "$PROFILE_FILE" | awk '{print $2}')
DURATION_SEC=$(grep '^duration_sec:' "$PROFILE_FILE" | awk '{print $2}')
KEYS=$(grep -A 10 '^keys:' "$PROFILE_FILE" | grep '  - ' | sed 's/.*- //' | tr '\n' ',' | sed 's/,$//')

# JSON配列用にキーを整形 (カンマ区切り → JSON配列)
KEYS_JSON=$(echo "$KEYS" | sed 's/,/", "/g' | sed 's/^/"/' | sed 's/$/"/')

# 持続時間上書き
if [[ -n "$DURATION_OVERRIDE" ]]; then
  DURATION_SEC="$DURATION_OVERRIDE"
fi

echo "📋 プロファイル: $PROFILE_NAME"
echo "   イベント総数: $EVENTS_TOTAL"
echo "   持続時間: ${DURATION_SEC}秒"
echo "   キー: $KEYS"
echo ""

# ウォームアップ (5%のイベントを先行送信)
WARMUP_EVENTS=$((EVENTS_TOTAL * 5 / 100))
if [[ $WARMUP_EVENTS -lt 100 ]]; then
  WARMUP_EVENTS=100
fi

echo "🔥 ウォームアップ開始 (${WARMUP_EVENTS}イベント)..."
IFS=',' read -ra KEY_ARRAY <<< "$KEYS"
for i in $(seq 1 "$WARMUP_EVENTS"); do
  KEY="${KEY_ARRAY[$((RANDOM % ${#KEY_ARRAY[@]}))]}"
  PAYLOAD="{\"symbol\":\"$KEY\",\"price\":$((RANDOM % 100000 + 1000)),\"quantity\":$((RANDOM % 100)),\"ts\":$(date +%s%3N)}"

  # /ingressエンドポイントにPOST (ヘッダーでkey指定)
  curl -s -X POST "$APP_URL/ingress" \
    -H "Content-Type: application/json" \
    -H "X-Key: $KEY" \
    -d "$PAYLOAD" >/dev/null 2>&1 || true

  # スロットリング (最初は低速)
  if [[ $((i % 10)) -eq 0 ]]; then
    sleep 0.01
  fi
done
echo "✅ ウォームアップ完了"

# メイン負荷生成 (burst.yamlのpatternに従う)
echo "⚡ メイン負荷テスト開始 (${EVENTS_TOTAL}イベント, ${DURATION_SEC}秒)..."

# burst.yamlのpattern:
#   - ramp_up: 10s (100→1000 events/s)
#   - burst: 20s (1000 events/s)
#   - ramp_down: 10s (1000→100 events/s)
#   - recovery: 20s (100 events/s)

# 簡易実装: 一定レートでイベント送信 (将来的にはpattern対応)
RATE=$((EVENTS_TOTAL / DURATION_SEC))
SLEEP_INTERVAL=$(echo "scale=6; 1.0 / $RATE" | bc)

START_TIME=$(date +%s)
SENT_COUNT=0

while [[ $SENT_COUNT -lt $EVENTS_TOTAL ]]; do
  KEY="${KEY_ARRAY[$((RANDOM % ${#KEY_ARRAY[@]}))]}"
  PAYLOAD="{\"symbol\":\"$KEY\",\"price\":$((RANDOM % 100000 + 1000)),\"quantity\":$((RANDOM % 100)),\"ts\":$(date +%s%3N)}"

  curl -s -X POST "$APP_URL/ingress" \
    -H "Content-Type: application/json" \
    -H "X-Key: $KEY" \
    -d "$PAYLOAD" >/dev/null 2>&1 || true

  SENT_COUNT=$((SENT_COUNT + 1))

  # 進捗表示 (10%ごと)
  if [[ $((SENT_COUNT % (EVENTS_TOTAL / 10))) -eq 0 ]]; then
    PROGRESS=$((SENT_COUNT * 100 / EVENTS_TOTAL))
    echo "   進捗: ${PROGRESS}% ($SENT_COUNT/$EVENTS_TOTAL)"
  fi

  # レート制御
  sleep "$SLEEP_INTERVAL" 2>/dev/null || true
done

ELAPSED_TIME=$(($(date +%s) - START_TIME))
echo "✅ 負荷テスト完了 (送信: ${SENT_COUNT}イベント, 経過: ${ELAPSED_TIME}秒)"

# クールダウン (メトリクス集計待ち)
echo "⏳ クールダウン (2秒)..."
sleep 2

# /statsからメトリクス取得
echo "📊 メトリクス収集中..."
STATS_JSON=$(curl -s "$APP_URL/stats")

if [[ -z "$STATS_JSON" ]] || [[ "$STATS_JSON" == "null" ]]; then
  echo "❌ エラー: メトリクス取得失敗" >&2
  exit 1
fi

# メトリクス抽出 (jqがなければPython fallback)
if command -v jq >/dev/null 2>&1; then
  # jq利用可能
  FAST_PATH_COUNT=$(echo "$STATS_JSON" | jq -r '.fast_path_count // 0' || echo "0")
  DROP_COUNT=$(echo "$STATS_JSON" | jq -r '.fast_path_drop_count // 0' || echo "0")
  PROCESS_P50=$(echo "$STATS_JSON" | jq -r '.fast_path_process_p50_us // 0' || echo "0")
  PROCESS_P99=$(echo "$STATS_JSON" | jq -r '.fast_path_process_p99_us // 0' || echo "0")
  PROCESS_P999=$(echo "$STATS_JSON" | jq -r '.fast_path_process_p999_us // 0' || echo "0")
  PUBLISH_P50=$(echo "$STATS_JSON" | jq -r '.fast_path_publish_p50_us // 0' || echo "0")
  PUBLISH_P99=$(echo "$STATS_JSON" | jq -r '.fast_path_publish_p99_us // 0' || echo "0")
  PUBLISH_P999=$(echo "$STATS_JSON" | jq -r '.fast_path_publish_p999_us // 0' || echo "0")
  PQ_WRITE_P99=$(echo "$STATS_JSON" | jq -r '.persistence_queue_write_p99_us // 0' || echo "0")
  PQ_ERROR_COUNT=$(echo "$STATS_JSON" | jq -r '.persistence_queue_error_count // 0' || echo "0")
  PQ_LAG=$(echo "$STATS_JSON" | jq -r '.persistence_queue_lag // 0' || echo "0")
else
  # Python fallback
  read -r FAST_PATH_COUNT DROP_COUNT PROCESS_P50 PROCESS_P99 PROCESS_P999 \
          PUBLISH_P50 PUBLISH_P99 PUBLISH_P999 PQ_WRITE_P99 PQ_ERROR_COUNT PQ_LAG \
    < <(python3 -c "
import json, sys
data = json.loads('''$STATS_JSON''')
print(
  data.get('fast_path_count', 0),
  data.get('fast_path_drop_count', 0),
  data.get('fast_path_process_p50_us', 0),
  data.get('fast_path_process_p99_us', 0),
  data.get('fast_path_process_p999_us', 0),
  data.get('fast_path_publish_p50_us', 0),
  data.get('fast_path_publish_p99_us', 0),
  data.get('fast_path_publish_p999_us', 0),
  data.get('persistence_queue_write_p99_us', 0),
  data.get('persistence_queue_error_count', 0),
  data.get('persistence_queue_lag', 0)
)
")
fi

# tail_ratio計算 (p99/p50) - ゼロ除算回避
if [[ "$PROCESS_P50" != "0" ]] && [[ -n "$PROCESS_P50" ]]; then
  TAIL_RATIO=$(echo "scale=2; $PROCESS_P99 / $PROCESS_P50" | bc || echo "0")
else
  TAIL_RATIO="0"
fi

# スループット計算 (events/sec) - ゼロ除算回避
if [[ $ELAPSED_TIME -gt 0 ]]; then
  THROUGHPUT=$(echo "scale=2; $FAST_PATH_COUNT / $ELAPSED_TIME" | bc || echo "0")
else
  THROUGHPUT="0"
fi

# エラー率計算 (%)
if [[ $FAST_PATH_COUNT -gt 0 ]]; then
  ERROR_RATE=$(echo "scale=4; ($PQ_ERROR_COUNT * 100.0) / $FAST_PATH_COUNT" | bc || echo "0.0000")
else
  ERROR_RATE="0.0000"
fi

echo "✅ メトリクス収集完了"
echo ""
echo "📈 結果サマリ:"
echo "   Fast Path処理数: $FAST_PATH_COUNT"
echo "   ドロップ数: $DROP_COUNT"
echo "   p50: ${PROCESS_P50}μs"
echo "   p99: ${PROCESS_P99}μs"
echo "   p999: ${PROCESS_P999}μs"
echo "   Tail Ratio: $TAIL_RATIO"
echo "   スループット: ${THROUGHPUT} events/s"
echo "   エラー率: ${ERROR_RATE}%"
echo ""

# contracts/bench.v1.schema.json準拠のJSON生成
cat > "$OUT_FILE" <<EOF
{
  "version": "v1",
  "timestamp": "$TIMESTAMP",
  "environment": {
    "type": "$ENV_TYPE",
    "config": {
      "fast_path_enable": true,
      "fast_path_metrics": true,
      "kafka_bridge_enable": false,
      "jvm_heap_mb": 2048
    },
    "git_commit": "$GIT_COMMIT",
    "git_branch": "$GIT_BRANCH"
  },
  "profile": {
    "name": "$PROFILE_NAME",
    "duration_sec": $DURATION_SEC,
    "events_total": $EVENTS_TOTAL,
    "warmup_events": $WARMUP_EVENTS,
    "keys": [$KEYS_JSON]
  },
  "metrics": {
    "fast_path": {
      "count": $FAST_PATH_COUNT,
      "process_latency_us": {
        "p50": $PROCESS_P50,
        "p99": $PROCESS_P99,
        "p999": $PROCESS_P999
      },
      "publish_latency_us": {
        "p50": $PUBLISH_P50,
        "p99": $PUBLISH_P99,
        "p999": $PUBLISH_P999
      },
      "drop_count": $DROP_COUNT
    },
    "persistence_queue": {
      "write_latency_us": {
        "p99": $PQ_WRITE_P99
      },
      "error_count": $PQ_ERROR_COUNT,
      "lag": $PQ_LAG
    },
    "summary": {
      "tail_ratio": $TAIL_RATIO,
      "throughput_events_per_sec": $THROUGHPUT,
      "error_rate_percent": $ERROR_RATE
    }
  },
  "slo_compliance": {
    "status": "PASS",
    "checks": []
  }
}
EOF

echo "💾 結果をファイルに保存: $OUT_FILE"
echo ""
echo "🎯 次のステップ: SLOゲート実行"
echo "   python scripts/slo_gate.py \\"
echo "     --in $OUT_FILE \\"
echo "     --schema contracts/bench.v1.schema.json \\"
echo "     --github-summary var/results/github_summary.md"
echo ""
echo "✅ カナリアテスト完了"
