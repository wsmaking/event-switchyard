#!/usr/bin/env bash
set -euo pipefail

# ========= デフォルト設定 =========
NUM_MSG="${NUM_MSG:-10000}"
ACKS=("all" "1")
LINGER=("0" "5" "50")
BATCH=("16384" "65536")
REPEAT=1

ARCHIVE=1                 # 実行前に既存結果をアーカイブ退避
KEEP_ARCHIVES=5           # results/archive の保持世代

RESULTS_DIR="$PWD/results"
ARCHIVE_DIR="$RESULTS_DIR/archive"

# ★ ランごと固有トピックにするためのプレフィックス
TOPIC_PREFIX="${TOPIC_PREFIX:-events}"
PARTITIONS="${PARTITIONS:-1}"
REPLICATION="${REPLICATION:-1}"
BOOTSTRAP="${BOOTSTRAP_SERVERS:-}"   # 例: "kafka:9092,kafka2:9093,kafka3:9094"

usage() {
  cat <<'USAGE'
Usage: ./bench.sh [--acks all|1] [--linger N] [--batch N] [--repeat N]
                  [--no-archive] [--keep-archives N]
                  [--topic-prefix NAME] [--partitions N] [--replication N] [--bootstrap HOSTS]
                  [--num N]

例:
  NUM_MSG=500000 ./bench.sh
  ./bench.sh --topic-prefix events --partitions 1 --replication 1 --acks all --linger 5 --batch 65536 --num 500000
USAGE
  exit 1
}

# ========= 引数パース =========
while [[ $# -gt 0 ]]; do
  case "$1" in
    --acks)           ACKS=("$2"); shift 2 ;;
    --linger)         LINGER=("$2"); shift 2 ;;
    --batch)          BATCH=("$2"); shift 2 ;;
    --repeat)         REPEAT="$2"; shift 2 ;;
    --no-archive)     ARCHIVE=0; shift ;;
    --keep-archives)  KEEP_ARCHIVES="$2"; shift 2 ;;
    --topic-prefix)   TOPIC_PREFIX="$2"; shift 2 ;;
    --partitions)     PARTITIONS="$2"; shift 2 ;;
    --replication)    REPLICATION="$2"; shift 2 ;;
    --bootstrap)      BOOTSTRAP="$2"; shift 2 ;;
    --num)            NUM_MSG="$2"; shift 2 ;;
    -h|--help)        usage ;;
    *) echo "unknown arg: $1"; usage ;;
  esac
done

# ========= 事前チェック =========
command -v jq      >/dev/null 2>&1 || { echo "ERROR: jq が必要です。brew install jq 等で導入してください"; exit 1; }
command -v docker  >/dev/null 2>&1 || { echo "ERROR: Docker が必要です"; exit 1; }
mkdir -p "$RESULTS_DIR"

echo ">> build images if needed"
docker compose build topic-init producer consumer >/dev/null

echo ">> ensure zookeeper + kafka up"
docker compose up -d zookeeper kafka >/dev/null

# ========= 既存結果のアーカイブ退避 =========
archive_results() {
  shopt -s nullglob
  local files=( "$RESULTS_DIR"/*.jsonl "$RESULTS_DIR"/summary*.csv )
  if (( ${#files[@]} > 0 )); then
    local ts; ts="$(date +%Y%m%d-%H%M%S)"
    local dst="$ARCHIVE_DIR/$ts"
    mkdir -p "$dst"
    echo ">> archive old results -> $dst"
    mv "${files[@]}" "$dst"/
  fi
  shopt -u nullglob

  # 新しい順に KEEP_ARCHIVES を残す
  if [ -d "$ARCHIVE_DIR" ]; then
    local i=0
    for d in $(ls -1dt "$ARCHIVE_DIR"/* 2>/dev/null); do
      i=$((i+1))
      if (( i > KEEP_ARCHIVES )); then
        rm -rf -- "$d"
      fi
    done
  fi
}

if (( ARCHIVE == 1 )); then
  archive_results
else
  echo ">> skip archiving old results"
fi

# ========= 実行回数の表示 =========
total=$(( ${#ACKS[@]} * ${#LINGER[@]} * ${#BATCH[@]} * REPEAT ))
echo ">> total runs: $total (ACKS=${ACKS[*]}  LINGER=${LINGER[*]}  BATCH=${BATCH[*]}  REPEAT=${REPEAT}  NUM_MSG=${NUM_MSG})"

# ========= 1コンビネーション実行関数 =========
run_one() {
  local a="$1" l="$2" b="$3" r="$4"
  local run_id; run_id="$(date +%Y%m%d-%H%M%S)-a${a}-l${l}-b${b}-r${r}"
  local topic="${TOPIC_PREFIX}-${run_id}"
  echo "=== RUN $run_id (topic=${topic}) ==="

  # ラン専用トピックを明示作成（冪等）
  docker compose run --rm -T \
    -e TOPIC_NAME="${topic}" \
    -e PARTITIONS="${PARTITIONS}" \
    -e REPLICATION="${REPLICATION}" \
    ${BOOTSTRAP:+-e BOOTSTRAP_SERVERS="$BOOTSTRAP"} \
    topic-init >/dev/null

  # Producer（デタッチ）
  local pn="esy-prod-$run_id"
  docker compose run -d --name "$pn" -T \
    -e RUN_ID="$run_id" \
    -e TOPIC_NAME="$topic" \
    -e ACKS="$a" -e LINGER_MS="$l" -e BATCH_SIZE="$b" \
    -e NUM_MSG="$NUM_MSG" \
    ${BOOTSTRAP:+-e BOOTSTRAP_SERVERS="$BOOTSTRAP"} \
    producer >/dev/null

  # Consumer（前面 / 結果ディレクトリをマウント）
  docker compose run --rm -T \
    -v "$RESULTS_DIR:/var/log/results" \
    -e RESULTS_DIR=/var/log/results \
    -e RUN_ID="$run_id" -e GROUP_ID="esy-$run_id" \
    -e TOPIC_NAME="$topic" \
    -e ACKS="$a" -e LINGER_MS="$l" -e BATCH_SIZE="$b" \
    -e EXPECTED_MSG="$NUM_MSG" \
    -e IDLE_MS="1500" \
    ${BOOTSTRAP:+-e BOOTSTRAP_SERVERS="$BOOTSTRAP"} \
    consumer

  # 後片付け
  docker wait "$pn" >/dev/null 2>&1 || true
  docker rm -f "$pn"   >/dev/null 2>&1 || true
  sleep 1
}

# ========= 全組み合わせ実行 =========
for a in "${ACKS[@]}"; do
  for l in "${LINGER[@]}"; do
    for b in "${BATCH[@]}"; do
      for ((r=1; r<=REPEAT; r++)); do
        run_one "$a" "$l" "$b" "$r"
      done
    done
  done
done

# ========= 集計（summary.csv を作成） =========
echo ">> build summary.csv"
shopt -s nullglob
jsons=( "$RESULTS_DIR"/*.jsonl )
if (( ${#jsons[@]} == 0 )); then
  echo "WARN: 新しい結果ファイルがありません（*.jsonl）"
  exit 0
fi

{
  echo "run_id,acks,linger_ms,batch_size,p50_us,p95_us,p99_us,received"
  jq -r '[.run_id,.acks,.linger_ms,.batch_size,.p50_us,.p95_us,.p99_us,.received] | @csv' "${jsons[@]}" | sort
} > "$RESULTS_DIR/summary.csv"

# p99 降順 TOP10（ヘッダを除いて sort）
awk -F, 'NR>1' "$RESULTS_DIR/summary.csv" | sort -t, -k7,7nr | head -n 10 > "$RESULTS_DIR/summary_top_p99.txt"

echo ">> done."
echo "   - $RESULTS_DIR/summary.csv"
echo "   - $RESULTS_DIR/summary_top_p99.txt"
