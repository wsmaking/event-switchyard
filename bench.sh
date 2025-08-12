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
PARTITIONS="${PARTITIONS:-3}"
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

docker compose down --remove-orphans >/dev/null 2>&1 || true

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

# ========= REPLICATION の安全ガード（ここに追加） =========
# ブローカ数を推定（BOOTSTRAPが "host1,host2,host3" なら 3）
if [[ -n "$BOOTSTRAP" ]]; then
  IFS=',' read -r -a __bs <<< "$BOOTSTRAP"
  BROKERS="${BROKERS:-${#__bs[@]}}"
else
  BROKERS="${BROKERS:-1}"   # composeでkafkaが1台なら1
fi

if (( REPLICATION > BROKERS )); then
  echo "WARN: replication=$REPLICATION > brokers=$BROKERS -> ${BROKERS} に下げます"
  REPLICATION="$BROKERS"
fi

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

  # REPLICATION ガード（関数内）
  local repl="$REPLICATION"
  local brokers="$BROKERS"
  if [[ -n "$BOOTSTRAP" ]]; then
    IFS=',' read -r -a __bs <<< "$BOOTSTRAP"
    brokers="${BROKERS:-${#__bs[@]}}"
  fi
  if (( repl > brokers )); then repl="$brokers"; fi

  # ラン専用トピックを明示作成（冪等）
  docker compose run --rm -T \
    -e TOPIC_NAME="${topic}" \
    -e PARTITIONS="${PARTITIONS}" \
    -e REPLICATION="${repl}" \
    ${BOOTSTRAP:+-e BOOTSTRAP_SERVERS="$BOOTSTRAP"} \
    topic-init >/dev/null

  # Producer（デタッチ）
  local pn="esy-prod-$run_id"
  docker compose run -d --name "$pn" -T \
    -e PAYLOAD_BYTES="${PAYLOAD_BYTES:-200}" \
    -e WARMUP_MSGS="${WARMUP_MSGS:-0}" \
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
    -e COMMIT_INTERVAL_MS="${COMMIT_INTERVAL_MS:-1000}" \
    -e LAG_LOG_INTERVAL_MS="${LAG_LOG_INTERVAL_MS:-1000}" \
    -e SKIP_BACKLOG="${SKIP_BACKLOG:-false}" \
    ${BOOTSTRAP:+-e BOOTSTRAP_SERVERS="$BOOTSTRAP"} \
    consumer

  # 後片付け
  docker wait "$pn" >/dev/null 2>&1 || true
  docker rm -f "$pn"   >/dev/null 2>&1 || true
  sleep 1

  docker compose exec -T kafka kafka-topics --bootstrap-server "${BOOTSTRAP:-kafka:9092}" \
  --delete --topic "$topic" >/dev/null 2>&1 || true
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
  echo "run_id,acks,linger_ms,batch_size,commit_interval_ms,lag_sample_every_msgs,received,measured,throughput_mps,p50_us,p95_us,p99_us,lag_avg,lag_max,lag_samples,lag_final"
  jq -r '[.run_id,.acks,.linger_ms,.batch_size,.commit_interval_ms,.lag_sample_every_msgs,.received,.measured,.throughput_mps,.p50_us,.p95_us,.p99_us,.lag_avg,.lag_max,.lag_samples,.lag_final] | @csv' "${jsons[@]}" | sort
} > "$RESULTS_DIR/summary.csv"

## p99 降順 TOP10（ヘッダを除いて sort）
awk -F, 'NR>1' "$RESULTS_DIR/summary.csv" | sort -t, -k12,12nr | head -n 10 > "$RESULTS_DIR/summary_top_p99.txt"

# ========= 可視化（dashboard.html を生成） =========
python3 - "$RESULTS_DIR/summary.csv" "$RESULTS_DIR/dashboard.html" <<'PY'
import csv, json, sys, html
from pathlib import Path

inp = Path(sys.argv[1]); out = Path(sys.argv[2])
rows = list(csv.DictReader(inp.open()))
# 数値キャスト
for r in rows:
    for k in ("linger_ms","batch_size","commit_interval_ms","lag_sample_every_msgs","received","measured",
              "p50_us","p95_us","p99_us","lag_max","lag_samples","lag_final"):
        r[k] = int(float(r[k]))
    r["lag_avg"] = float(r.get("lag_avg", 0) or 0)
    r["throughput_mps"] = float(r["throughput_mps"])

    r["p50_ms"] = r["p50_us"]/1000.0
    r["p95_ms"] = r["p95_us"]/1000.0
    r["p99_ms"] = r["p99_us"]/1000.0
    r["label"] = f"acks={r['acks']}, linger={r['linger_ms']}, batch={r['batch_size']}, commit={r['commit_interval_ms']}ms, sample={r['lag_sample_every_msgs']}"




data_json = json.dumps(rows)
src = html.escape(str(inp))

html_doc = """<!doctype html><meta charset="utf-8">
<title>Event Switchyard — W1 Summary</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
 body{font:14px system-ui,-apple-system,Segoe UI,Roboto,sans-serif;margin:20px}
 .grid{display:grid;grid-template-columns:1fr;gap:24px}
 @media(min-width:1200px){.grid{grid-template-columns:1fr 1fr}}
 .card{padding:16px;border:1px solid #eee;border-radius:12px;box-shadow:0 2px 16px rgba(0,0,0,.06)}
 h1{margin:0 0 8px} .tag{font-size:12px;color:#666}
 table{border-collapse:collapse;width:100%}
 th,td{border-bottom:1px solid #eee;padding:8px 6px;text-align:left}
 tr:hover td{background:#fafafa}
</style>
<h1>W1 Summary</h1>
<div class="tag">Source: __SRC__</div>
<div class="grid">
  <div class="card"><div id="scatter" style="height:420px"></div></div>
  <div class="card"><div id="bar_mps" style="height:420px"></div></div>
  <div class="card"><div id="bar_p99" style="height:420px"></div></div>
  <div class="card"><div id="bar_lag" style="height:420px"></div></div>
  <div class="card"><div id="bar_lag_final" style="height:420px"></div></div>
  <div class="card"><div id="table"></div></div>
</div>
<script>
const rows = __DATA__;

Plotly.newPlot('scatter', [{
  x: rows.map(r=>r.p99_ms),
  y: rows.map(r=>r.throughput_mps),
  text: rows.map(r=>r.label),
  mode:'markers+text',
  textposition:'top center'
}], {title:'Throughput (MPS) vs p99 latency (ms)', xaxis:{title:'p99 (ms)'}, yaxis:{title:'MPS'}});

const byMps=[...rows].sort((a,b)=>b.throughput_mps-a.throughput_mps);
Plotly.newPlot('bar_mps', [{
  x: byMps.map(r=>r.label),
  y: byMps.map(r=>r.throughput_mps),
  type:'bar'
}], {title:'Throughput by configuration', xaxis:{tickangle:-40}});

const byP99=[...rows].sort((a,b)=>a.p99_ms-b.p99_ms);
Plotly.newPlot('bar_p99', [{
  x: byP99.map(r=>r.label),
  y: byP99.map(r=>r.p99_ms),
  type:'bar'
}], {title:'p99 latency by configuration (ms)', xaxis:{tickangle:-40}});

const byLag=[...rows].sort((a,b)=>b.lag_avg-a.lag_avg);
Plotly.newPlot('bar_lag', [{
  x: byLag.map(r=>r.label),
  y: byLag.map(r=>r.lag_avg),
  type:'bar'
}], {title:'Average consumer lag (messages)', xaxis:{tickangle:-40}, yaxis:{title:'avg lag'}});

const byLagFinal=[...rows].sort((a,b)=>b.lag_final-a.lag_final);
Plotly.newPlot('bar_lag_final', [{
  x: byLagFinal.map(r=>r.label),
  y: byLagFinal.map(r=>r.lag_final),
  type:'bar'
}], {title:'Final consumer lag at end (messages)', xaxis:{tickangle:-40}, yaxis:{title:'final lag'}});

const tbl = document.getElementById('table');
tbl.innerHTML = `<table><thead><tr>
<th>run_id</th><th>acks</th><th>linger</th><th>batch</th>
<th>p50(ms)</th><th>p95(ms)</th><th>p99(ms)</th><th>MPS</th></tr></thead><tbody>` +
rows.map(r=>`<tr><td>${r.run_id}</td><td>${r.acks}</td><td>${r.linger_ms}</td><td>${r.batch_size}</td>
<td>${r.p50_ms.toFixed(1)}</td><td>${r.p95_ms.toFixed(1)}</td><td>${r.p99_ms.toFixed(1)}</td><td>${r.throughput_mps.toFixed(0)}</td></tr>`).join('') + `</tbody></table>`;
</script>"""

# 置換で埋め込む（f-string禁止）
html_doc = html_doc.replace("__DATA__", data_json).replace("__SRC__", src)
out.write_text(html_doc, encoding="utf-8")
print("Wrote", out)
PY

echo "   - $RESULTS_DIR/dashboard.html  # ブラウザで開いてください"

echo ">> done."
echo "   - $RESULTS_DIR/summary.csv"
echo "   - $RESULTS_DIR/summary_top_p99.txt"
