#!/usr/bin/env bash
set -euo pipefail

# ========= デフォルト設定 =========
NUM_MSG="${NUM_MSG:-10000}"
ACKS=("all" "1")
LINGER=("0" "5" "50")
BATCH=("16384" "65536")
REPEAT="${REPEAT:-1}"
ARCHIVE=1                 # 実行前に既存結果をアーカイブ退避
KEEP_ARCHIVES=5           # results/archive の保持世代
KEY_STRATEGY=("none" "symbol" "order_id")

RESULTS_DIR="$PWD/results"
ARCHIVE_DIR="$RESULTS_DIR/archive"

# ★ ランごと固有トピックにするためのプレフィックス
TOPIC_PREFIX="${TOPIC_PREFIX:-events}"
PARTITIONS="${PARTITIONS:-3}"
REPLICATION="${REPLICATION:-1}"
BOOTSTRAP="${BOOTSTRAP_SERVERS:-}"   # 例: "kafka:9092,kafka2:9093,kafka3:9094"
KEEP_TOPIC="${KEEP_TOPIC:-0}"

PROFILE=${PROFILE:-rt}  # rt | drain

if [[ "$PROFILE" == "rt" ]]; then
  export SKIP_BACKLOG=true
  export EXPECTED_MSG=0        # 件数で待たず、idle で終了
  export WARMUP_MSGS=${WARMUP_MSGS:-0}  # 使うならここで調整
else # drain
  export SKIP_BACKLOG=false
  export EXPECTED_MSG="$NUM_MSG"  # 全件読み切る
fi

echo ">> running $(/usr/bin/env realpath "$0" 2>/dev/null || echo "$0")"

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

# ★ 外部クラスタ利用時はローカルKafkaを起動しない
if [[ -z "$BOOTSTRAP" ]]; then
  echo ">> build images if needed"
  docker compose build topic-init producer consumer >/dev/null

  echo ">> ensure zookeeper + kafka up"
  docker compose up -d zookeeper kafka >/dev/null
else
  echo ">> external cluster mode (skip local kafka up)"
  # 外部クラスタの場合、topic-init は run 時に自動ビルドされるので省略可
  docker compose build producer consumer >/dev/null
fi

archive_results() {
  # 収集したいパターンを列挙
  local patterns=(
    "$RESULTS_DIR/bench-*.jsonl"
    "$RESULTS_DIR/summary*.csv"
    "$RESULTS_DIR/ack-*.hgrm"
    "$RESULTS_DIR/lag-*.csv"
    "$RESULTS_DIR/group-describe-*.txt"
    "$RESULTS_DIR/topic-describe-*.txt"
    "$RESULTS_DIR/ack-summary.jsonl"
    "$RESULTS_DIR/dashboard.html"
  )

  # 実在ファイルだけ配列に積む
  shopt -s nullglob
  local to_move=()
  for pat in "${patterns[@]}"; do
    for f in $pat; do
      [[ -e "$f" ]] && to_move+=( "$f" )
    done
  done
  shopt -u nullglob

  if (( ${#to_move[@]} > 0 )); then
    local ts dst
    ts="$(date +%Y%m%d-%H%M%S)"
    dst="$ARCHIVE_DIR/$ts"
    mkdir -p "$dst"
    echo ">> archive old results -> $dst"
    # mv 失敗で止まらないように保険も入れておく
    mv "${to_move[@]}" "$dst"/ || true
    # 古いアーカイブの世代管理
    local i=0
    for d in $(ls -1dt "$ARCHIVE_DIR"/* 2>/dev/null); do
      i=$((i+1))
      (( i > KEEP_ARCHIVES )) && rm -rf -- "$d"
    done
  else
    echo ">> no previous result files to archive"
  fi
}


if (( ARCHIVE == 1 )); then
  archive_results
else
  echo ">> skip archiving old results"
fi

# ========= 実行回数の表示 =========
total=$(( ${#ACKS[@]} * ${#LINGER[@]} * ${#BATCH[@]} * ${#KEY_STRATEGY[@]} * REPEAT ))
echo ">> total runs: $total (ACKS=${ACKS[*]}  LINGER=${LINGER[*]}  BATCH=${BATCH[*]}  KEY=${KEY_STRATEGY[*]}  REPEAT=${REPEAT}  NUM_MSG=${NUM_MSG})"

# ========= 1コンビネーション実行関数 =========
run_one() {
  local a="$1" l="$2" b="$3" r="$4" ks="$5"
  local run_id; run_id="$(date +%Y%m%d-%H%M%S)-k${ks}-a${a}-l${l}-b${b}-r${r}"
  local topic="${TOPIC_PREFIX}-${run_id}"
  echo "=== RUN $run_id (topic=${topic}, key=${ks}) ==="

  # REPLICATION ガード（関数内）
  local repl="$REPLICATION"

  local brokers="$BROKERS"
  if [[ -n "$BOOTSTRAP" ]]; then
    IFS=',' read -r -a __bs <<< "$BOOTSTRAP"
    brokers="${BROKERS:-${#__bs[@]}}"
  fi
  if (( repl > brokers )); then repl="$brokers"; fi

  local num_for_prod="$NUM_MSG"
  if [[ "$PROFILE" == "rt" ]]; then
    num_for_prod=0
  fi


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
    -v "$RESULTS_DIR:/var/log/results" \
    -e COMPRESSION_TYPE="${COMPRESSION_TYPE:-none}" \
    -e RUN_SECS="${RUN_SECS:-30}" \
    -e HOT_KEY_EVERY="${HOT_KEY_EVERY:-0}" \
    -e RESULTS_DIR=/var/log/results \
    -e PROFILE="$PROFILE" \
    -e ACK_RECORDER="${ACK_RECORDER:-1}" \
    -e ACK_HIST_PATH="/var/log/results/ack-$run_id.hgrm" \
    -e PAYLOAD_BYTES="${PAYLOAD_BYTES:-200}" \
    -e WARMUP_MSGS="${WARMUP_MSGS:-0}" \
    -e RUN_ID="$run_id" \
    -e TOPIC_NAME="$topic" \
    -e ACKS="$a" -e LINGER_MS="$l" -e BATCH_SIZE="$b" \
    -e KEY_STRATEGY="$ks" \
    -e NUM_MSG="$num_for_prod" \
    ${BOOTSTRAP:+-e BOOTSTRAP_SERVERS="$BOOTSTRAP"} \
    producer >/dev/null

  # Consumer（前面 / 結果ディレクトリをマウント）
  docker compose run --rm -T \
    -v "$RESULTS_DIR:/var/log/results" \
    -e RESULTS_DIR=/var/log/results \
    -e PROFILE="$PROFILE" \
    -e RUN_ID="$run_id" -e GROUP_ID="esy-$run_id" \
    -e TOPIC_NAME="$topic" \
    -e ACKS="$a" -e LINGER_MS="$l" -e BATCH_SIZE="$b" \
    -e KEY_STRATEGY="$ks" \
    -e EXPECTED_MSG="${EXPECTED_MSG:-0}" \
    -e IDLE_MS="1500" \
    -e COMMIT_INTERVAL_MS="${COMMIT_INTERVAL_MS:-1000}" \
    -e LAG_LOG_INTERVAL_MS="${LAG_LOG_INTERVAL_MS:-1000}" \
    -e SKIP_BACKLOG="${SKIP_BACKLOG:-true}" \
    ${BOOTSTRAP:+-e BOOTSTRAP_SERVERS="$BOOTSTRAP"} \
    consumer

  # 後片付け
  docker wait "$pn"    >/dev/null 2>&1 || true
  docker rm -f "$pn"   >/dev/null 2>&1 || true
  sleep 1

  # lag.csv を1秒粒度で生成（ConsumerのJSONLから抽出）
  if command -v jq >/dev/null 2>&1; then
    jq -r --arg RUN "$run_id" '
      select(.run_id==$RUN and .type=="lag" and .ts_sec!=null and .lag!=null)
      | [.ts_sec,.lag] | @csv' "$RESULTS_DIR"/bench-*.jsonl \
      > "$RESULTS_DIR/lag-$run_id.csv" || true
  fi

  # --- グループ/トピックの describe を残す（トピック削除前に！）
  if [[ -z "$BOOTSTRAP" ]]; then
    echo ">> capture consumer group/topic describe before topic delete"
    local bs="kafka:9092"

    # coordinator 反映待ち（Empty -> Stable 遷移の猶予）
    sleep 1
    for i in {1..6}; do
      if docker compose exec -T kafka \
        kafka-consumer-groups --bootstrap-server "$bs" \
        --describe --group "esy-$run_id" \
        > "$RESULTS_DIR/group-describe-$run_id.txt" 2>/dev/null; then
        break
      fi
      sleep 0.5
    done

    docker compose exec -T kafka \
      kafka-topics --bootstrap-server "$bs" \
      --describe --topic "$topic" \
      > "$RESULTS_DIR/topic-describe-$run_id.txt" 2>/dev/null || true
  fi

  # 削除ロジック
  if [[ -z "$BOOTSTRAP" ]]; then
    local bs="kafka:9092"
    if (( KEEP_TOPIC == 1 )); then
      echo ">> KEEP_TOPIC=1 -> skip topic delete ($topic)"
    else
      docker compose exec -T kafka \
        kafka-topics --bootstrap-server "$bs" \
        --delete --topic "$topic" >/dev/null 2>&1 || true
    fi
  fi
}

# ========= 全組み合わせ実行 =========
for a in "${ACKS[@]}"; do
  for l in "${LINGER[@]}"; do
    for b in "${BATCH[@]}"; do
      for ks in "${KEY_STRATEGY[@]}"; do
        for ((r=1; r<=REPEAT; r++)); do
          run_one "$a" "$l" "$b" "$r" "$ks"
        done
      done
    done
  done
done

# ========= 集計（summary.csv を作成） =========
echo ">> build summary.csv"
shopt -s nullglob
jsons=( "$RESULTS_DIR"/bench-*.jsonl )
if (( ${#jsons[@]} == 0 )); then
  echo "WARN: 新しい結果ファイルがありません（*.jsonl）"
  exit 0
fi

{
  echo "run_id,acks,linger_ms,batch_size,key_strategy,profile,commit_interval_ms,lag_sample_every_msgs,received,measured,throughput_mps,p50_us,p95_us,p99_us,p999_us,tail_ratio,tail_step_999,hdr_count,lag_avg,lag_max,lag_samples,lag_final,compression"
  jq -r --arg PROFILE "$PROFILE" --arg COMP "${COMPRESSION_TYPE:-none}" '
    select(.throughput_mps != null) |
    [
      .run_id,.acks,.linger_ms,.batch_size,.key_strategy, (.profile // $PROFILE),
      .commit_interval_ms,.lag_sample_every_msgs,
      .received,.measured,.throughput_mps,
      .p50_us,.p95_us,.p99_us,
      (.p999_us // 0), (.tail_ratio // 0), (.tail_step_999 // 0), (.hdr_count // 0),
      .lag_avg,.lag_max,.lag_samples,.lag_final,
      $COMP
    ] | @csv' \
    "${jsons[@]}" | sort
} > "$RESULTS_DIR/summary.csv"

## p99 降順 TOP10（ヘッダを除いて sort）
awk -F, 'NR>1' "$RESULTS_DIR/summary.csv" | sort -t, -k13,13nr | head -n 10 > "$RESULTS_DIR/summary_top_p99.txt"

echo ">> build summary_median.csv"
python3 - <<'PY'
import csv,statistics
inp="results/summary.csv"; out="results/summary_median.csv"
grp={}
with open(inp) as f:
  r=csv.DictReader(f)
  for x in r:
    key=(x["acks"],x["linger_ms"],x["batch_size"],x.get("key_strategy","-"),x.get("profile","-"),x.get("compression","-"))
    grp.setdefault(key,[]).append((float(x["p99_us"]), float(x["throughput_mps"])))
with open(out,"w",newline="") as w:
  w.write("acks,linger_ms,batch_size,key_strategy,profile,compression,p99_us_median,mps_median,count\n")
  for (a,l,b,k,p,c),vals in grp.items():
    p99s=[v[0] for v in vals]; mps=[v[1] for v in vals]
    w.write(f"{a},{l},{b},{k},{p},{c},{statistics.median(p99s):.0f},{statistics.median(mps):.0f},{len(vals)}\n")
print("wrote", out)
PY

echo ">> sanity checks"
# lag_samples ≈ measured / lag_sample_every_msgs のズレ検知（±2まで許容）
jq -r '[.run_id,.measured,.lag_sample_every_msgs,.lag_samples] | @tsv' "$RESULTS_DIR"/bench-*.jsonl \
| awk '{exp=int($2/$3); diff=$4-exp; if (diff<-2 || diff>2) print "WARN lag_samples", $1, "diff=",diff}' || true

# p99の信頼度：hdr_count < 1000 を警告（CSVの18列目が hdr_count）
awk -F, 'NR>1 && $18 < 1000 {print "WARN: low hdr_count ->", $0}' "$RESULTS_DIR"/summary.csv || true


# ========= 可視化（dashboard.html を生成） =========
python3 - "$RESULTS_DIR/summary.csv" "$RESULTS_DIR/dashboard.html" <<'PY'
import csv, json, sys, html
from pathlib import Path

inp = Path(sys.argv[1]); out = Path(sys.argv[2])
rows = list(csv.DictReader(inp.open()))
# 数値キャスト
for r in rows:
    for k in ("linger_ms","batch_size","commit_interval_ms","lag_sample_every_msgs",
              "received","measured","p50_us","p95_us","p99_us","p999_us",
              "lag_max","lag_samples","lag_final","hdr_count"):
        r[k] = int(float(r.get(k, 0) or 0))
    for k in ("lag_avg","throughput_mps","tail_ratio","tail_step_999"):
        r[k] = float(r.get(k, 0) or 0)

    r["p50_ms"]  = r["p50_us"]/1000.0
    r["p95_ms"]  = r["p95_us"]/1000.0
    r["p99_ms"]  = r["p99_us"]/1000.0
    r["p999_ms"] = r["p999_us"]/1000.0

    # ラベルに key を含める
    r["label"] = f"comp={r.get('compression','-')}, key={r.get('key_strategy','-')}, acks={r['acks']}, linger={r['linger_ms']}, batch={r['batch_size']}, commit={r['commit_interval_ms']}ms, sample={r['lag_sample_every_msgs']}"



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
  <div class="card"><div id="bar_tail" style="height:420px"></div></div>  <!-- 追加 -->
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

const byTail=[...rows].sort((a,b)=>b.tail_ratio-a.tail_ratio);
Plotly.newPlot('bar_tail', [{
  x: byTail.map(r=>r.label),
  y: byTail.map(r=>r.tail_ratio),
  type:'bar'
}], {title:'Tail ratio (p99/p50)', xaxis:{tickangle:-40}, yaxis:{title:'p99 / p50'}});

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
