# Event Switchyard（Rust/v3 Low-Latency）

このリポジトリの正本 README は本ファイルのみ。

## 現在の主軸
- 実運用・性能検証の主軸: Rust 実装（`gateway-rust/`）
- hot path: `POST /v3/orders`（latency-first, volatile accept）
- durability-first の別系統は `v2`/`/orders` で分離運用

## 正本ドキュメント
- 契約: `docs/ops/contract_draft.md`
- 現行可視化: `docs/ops/current_design_visualization.md`
- 最新実測集約: `docs/ops/current_design_visualization.md` の `2.2.11 最新実測集約（単一正本）`
- durable設計: `docs/ops/durable_path_design.md`
- durable可視化: `docs/ops/durable_path_visualization.md`
- 進捗: `docs/ops/phase_progress.md`
- 最速学習: `docs/ops/catchup_fasttrack.md`
- docs索引: `docs/ops/INDEX.md`

補足:
- 旧資料は `docs/old/` と `docs/ops/old/` に退避済み。

## リポジトリ見取り図
- Rust gateway: `gateway-rust/`
- 共通コア: `gateway-core/`
- 運用スクリプト: `scripts/ops/`
- 契約/可視化: `docs/ops/`
- 学習/補助設計: `docs/arch/`
- 旧 Kotlin/Java 比較資産: `app/`, `gateway/`, `backoffice/`

## 現役スクリプト（最小）
- `scripts/ops/build_gateway_rust_release.sh`
- `scripts/ops/check_v3_local_strict_gate.sh`
- `scripts/ops/check_v3_stable_gate.sh`
- `scripts/ops/check_v3_absolute_gate.sh`
- `scripts/ops/check_v3_capability_gate.sh`
- `scripts/ops/check_v3_durable_tail_gate.sh`
- `scripts/ops/run_v3_absolute_gate_loop.sh`
- `scripts/ops/run_v3_open_loop_probe.sh`
- `scripts/ops/run_v3_phase2_compare.sh`
- `scripts/ops/run_v3_phase2_compare_when_quiet.sh`
- `scripts/ops/run_v3_capacity_sweep.sh`
- `scripts/ops/open_loop_v3_load.py`
- `scripts/ops/open_loop_v3_tcp_load.py`
- `scripts/ops/perf_noise_guard.sh`
- `scripts/ops/wrk_gateway_rust.sh`
- `scripts/ops/wrk_orders.lua`

補足:
- 上記以外の旧スクリプトは `scripts/old/`, `scripts/ops/old/`, `scripts/tools/old/`, `tools/old/` に退避。

## クイックスタート（Rust）
### 1. ビルド
```bash
cd gateway-rust
cargo build --release
cd ..
```

補足:
- 既定ビルドは Kafka 依存を含まない（`kafka-bus` feature OFF）。
- Kafka 連携を有効化してビルドする場合のみ:
```bash
cd gateway-rust
cargo build --release --features kafka-bus
cd ..
```

### 2. 起動（ローカル検証）
```bash
GATEWAY_PORT=29001 \
GATEWAY_TCP_PORT=0 \
JWT_HS256_SECRET=secret123 \
KAFKA_ENABLE=0 \
FASTPATH_DRAIN_ENABLE=1 \
FASTPATH_DRAIN_WORKERS=4 \
./gateway-rust/target/release/gateway-rust
```

### 3. ヘルス/メトリクス確認
```bash
curl -sS http://127.0.0.1:29001/health
curl -sS http://127.0.0.1:29001/metrics
```

## API（現行）
### v3（hot path）
- `POST /v3/orders`
- `GET /v3/orders/{sessionId}/{sessionSeq}`
- `V3_TCP_ENABLE=1` + `V3_TCP_PORT=<port>` で v3専用TCP入口を有効化
- v3 TCP frame は JWT token をpayload内に含める（Authorizationヘッダ不要）

### Durable系
- `POST /orders`
- `GET /orders/{order_id}`
- `GET /orders/client/{client_order_id}`
- `POST /orders/{order_id}/cancel`

## v3 の制御要点
- 同期境界: `parse -> risk -> try_enqueue -> response`
- 返却: `202 VOLATILE_ACCEPT`
- 水位制御: SOFT/HARD/KILL（`429/503/kill`）
- 異常時: `LOSS_SUSPECT` を残し、`session -> shard -> global` 昇格
- 入口分離:
  - `V3_HTTP_INGRESS_ENABLE=false` で HTTP受注を無効化
  - `V3_HTTP_CONFIRM_ENABLE=true` で confirm照会は HTTP 維持可能
  - `V3_TCP_ENABLE=true` + `V3_TCP_PORT=<port>` で v3 TCP受注を有効化

## よく使う検証コマンド
### local strict gate（Linux未確保時）
```bash
scripts/ops/check_v3_local_strict_gate.sh
```

### stable gate（10k/300s）
```bash
scripts/ops/check_v3_stable_gate.sh
```

### absolute gate（Linux専用）
```bash
scripts/ops/check_v3_absolute_gate.sh
```

### capability gate（能力側）
```bash
scripts/ops/check_v3_capability_gate.sh
```

### phase2比較
```bash
RUNS=5 WARMUP_RUNS=1 scripts/ops/run_v3_phase2_compare.sh
```

### open-loop probe
```bash
TARGET_RPS=10000 DURATION=300 scripts/ops/run_v3_open_loop_probe.sh
```

## 主要メトリクス（v3）
- `gateway_live_ack_p99_us`
- `gateway_v3_accepted_rate`
- `gateway_v3_rejected_soft_total`
- `gateway_v3_rejected_hard_total`
- `gateway_v3_rejected_killed_total`
- `gateway_v3_loss_suspect_total`
- `gateway_v3_stage_parse_p99_us`
- `gateway_v3_stage_risk_p99_us`
- `gateway_v3_stage_enqueue_p99_us`
- `gateway_v3_stage_serialize_p99_us`

## モニタリング（Grafana + Prometheus）

### 環境共通の検証手順（macOS / Linux）

前提:
- Docker + `docker compose` が使えること（Linux は Docker 20.10+ 推奨）
- Python 3.10+（`scripts/ops/open_loop_v3_load.py` 実行用）
- Windows は WSL2 上で実行する想定

0. 検証対象の Gateway endpoint を決める（既定: `127.0.0.1:8081`）
```bash
export GATEWAY_HOST=127.0.0.1
export GATEWAY_PORT=8081
```
- Prometheus の既定 scrape 先は `host.docker.internal:8081`。
- `GATEWAY_PORT` を変える場合は `monitoring/prometheus.yml` も同じポートに合わせる。

1. Gateway が `/health` と `/metrics` を返す状態にする
```bash
curl -sS "http://${GATEWAY_HOST}:${GATEWAY_PORT}/health"
curl -sS "http://${GATEWAY_HOST}:${GATEWAY_PORT}/metrics" | head
```

2. Prometheus + Grafana を起動する（composeファイルを明示）
```bash
docker compose -f docker-compose.yml --profile monitoring up -d prometheus grafana
```

3. Monitoring 側の起動確認
```bash
curl -sS http://127.0.0.1:9090/-/ready
curl -sS http://127.0.0.1:3000/api/health
```

4. Prometheus ターゲット確認
```bash
curl -sS 'http://127.0.0.1:9090/api/v1/targets?state=active'
curl -sS -G 'http://127.0.0.1:9090/api/v1/query' --data-urlencode 'query=up{job="event-switchyard-gateway"}'
```
- `event-switchyard-gateway (host.docker.internal:8081)` が `health: up` ならOK。
- `hft-fast-path (host.docker.internal:8080)` は Kotlin 側未起動なら `down` で正常。

5. 負荷前メトリクス確認
```bash
curl -sS "http://${GATEWAY_HOST}:${GATEWAY_PORT}/metrics" | grep -E '^(gateway_v3_accepted_total|gateway_v3_rejected_soft_total|gateway_v3_rejected_hard_total|gateway_v3_rejected_killed_total|gateway_v3_accepted_rate) '
```

6. 再計測を実行
- 推奨（最善条件の再計測 + gate判定 + summary出力）:
```bash
export PORT="${GATEWAY_PORT}" DURATION=60 TARGET_RPS=10000 TARGET_COMPLETED_RPS=10000 \
  TARGET_ACK_ACCEPTED_P99_US=40 TARGET_ACCEPTED_RATE=0.99 \
  TARGET_DURABLE_CONFIRM_P99_US=120000000 WARN_DURABLE_CONFIRM_P99_US=120000000 \
  ENFORCE_GATE=1 V3_DURABLE_ADMISSION_FSYNC_ONLY_SOFT_SUSTAIN_TICKS=0 \
  V3_DURABLE_ADMISSION_FSYNC_ONLY_HARD_SUSTAIN_TICKS=0 BUILD_RELEASE=0
scripts/ops/run_v3_open_loop_probe.sh
```
- 手元 Gateway に対して軽く負荷だけ流したい場合:
```bash
scripts/ops/open_loop_v3_load.py --host "${GATEWAY_HOST}" --port "${GATEWAY_PORT}" --duration-sec 20 --target-rps 1000 --workers 32 --accounts 16
```

7. 負荷後メトリクス確認（増分確認）
```bash
curl -sS "http://${GATEWAY_HOST}:${GATEWAY_PORT}/metrics" | grep -E '^(gateway_v3_accepted_total|gateway_v3_rejected_soft_total|gateway_v3_rejected_hard_total|gateway_v3_rejected_killed_total|gateway_v3_accepted_rate) '
curl -sS -G 'http://127.0.0.1:9090/api/v1/query' --data-urlencode 'query=rate(gateway_v3_rejected_hard_total[1m])'
curl -sS -G 'http://127.0.0.1:9090/api/v1/query' --data-urlencode 'query=rate(gateway_v3_accepted_total[1m])'
curl -sS -G 'http://127.0.0.1:9090/api/v1/query' --data-urlencode 'query=increase(gateway_v3_accepted_total[15m])'
```

注記:
- `gateway_v3_accepted_rate` は累積カウンタ比率（process start以降）なので、過去runの影響を受ける。
- 「直近の受理状況」を見るときは `rate(gateway_v3_accepted_total[1m])` と reject系 `rate(...)` を併用する。
- `run_v3_open_loop_probe.sh` は run 終了時に Gateway を停止するため、`up` は `0` に戻る。計測有無は `increase(...[15m])` で確認する。

### Grafana で確認
1. `http://localhost:3000` を開く
2. ログイン: `admin` / `admin`
3. 時間範囲を `Last 15 minutes` に設定
4. ダッシュボード:
   - `http://localhost:3000/d/hft-fast-path/hft-fast-path-real-time-performance`
   - `http://localhost:3000/d/gateway-ops/gateway-operations`
5. クエリを直接確認する場合:
   - 左メニュー `Explore` を開く
   - クエリ行 `A` の右隣 datasource が `Prometheus` になっていることを確認
   - `Prometheus disabled` と表示される場合は datasource プルダウンから `Prometheus` を選択
   - クエリ例:
     - `rate(gateway_v3_accepted_total[1m])`
     - `rate(gateway_v3_rejected_hard_total[1m])`
     - `increase(gateway_v3_accepted_total[15m])`
     - `gateway_live_ack_accepted_p99_us`

### トラブルシュート

#### 1) Grafanaに何も表示されない
- Prometheusで直接クエリ:
```bash
curl -sS -G 'http://127.0.0.1:9090/api/v1/query' --data-urlencode 'query=up{job="event-switchyard-gateway"}'
curl -sS -G 'http://127.0.0.1:9090/api/v1/query' --data-urlencode 'query=rate(gateway_v3_accepted_total[1m])'
curl -sS -G 'http://127.0.0.1:9090/api/v1/query' --data-urlencode 'query=increase(gateway_v3_accepted_total[15m])'
curl -sS -G 'http://127.0.0.1:9090/api/v1/query' --data-urlencode 'query=gateway_live_ack_accepted_p99_us'
```
- ここで値が返るなら、Grafana側の時間範囲/ダッシュボード選択の問題。

#### 2) `event-switchyard-gateway` が `down`
- まず Gateway が `http://${GATEWAY_HOST}:${GATEWAY_PORT}/metrics` で応答するか確認。
- ポートが `8081` 以外なら `monitoring/prometheus.yml` の target を修正し、Prometheus再起動:
```bash
docker compose -f docker-compose.yml --profile monitoring restart prometheus
```

#### 3) 古いダッシュボード定義が残る
- monitoring volume を初期化して再作成:
```bash
docker compose -f docker-compose.yml --profile monitoring down -v
docker compose -f docker-compose.yml --profile monitoring up -d prometheus grafana
```

#### 4) `/v3/orders` が `500 {"error":"SecretNotConfigured"}` を返す
- Gateway起動時に `JWT_HS256_SECRET` が未設定。
- 例（accepted確認を優先する簡易起動）:
```bash
cd gateway-rust
JWT_HS256_SECRET=secret123 V3_DURABLE_ADMISSION_CONTROLLER_ENABLED=0 ./target/release/gateway-rust
```

### Grafanaログインできない場合
```bash
docker exec hft-grafana grafana cli admin reset-admin-password admin
```

### 停止
```bash
docker compose -f docker-compose.yml --profile monitoring down
```

## AI Triage（Gate失敗時の自動原因分析）

Gate失敗時に自動実行され、Grafanaにannotationとして投稿される。

```bash
# 手動実行
python3 scripts/ops/ai_incident_agent.py --run-name <RUN_NAME> --results-dir var/results

# Grafanaに手動投稿
python3 scripts/ops/ai_triage_notify.py --triage-json var/results/<RUN_NAME>.triage.json
```

環境変数:
- `AI_TRIAGE_PROVIDER`: `mock`(default) / `openai`
- `AI_TRIAGE_GRAFANA`: `1`(default) / `0`で無効化
- `GRAFANA_URL`, `GRAFANA_USER`, `GRAFANA_PASSWORD`: Grafana接続先

### CPUカウンタ付き実行（Mac/Linux共通）

`run_ai_perf_probe.py` は「負荷実行 + CPU/電力カウンタ収集 + triage」を1回で実行する。

```bash
# 1) RAG index更新（docs + summary + metrics + perf）
python3 scripts/ops/ai_rag_index.py --docs-dir docs/ops --results-dir var/results --db-path var/ai_index/docs.sqlite

# 2) 環境非依存でまず確実に動かす（counterなし）
python3 scripts/ops/run_ai_perf_probe.py \
  --counter-mode none \
  --provider mock \
  -- scripts/ops/run_v3_open_loop_probe.sh
```

OS別カウンタ収集:

```bash
# Linux: perf stat
python3 scripts/ops/run_ai_perf_probe.py \
  --counter-mode linux-perf \
  --provider mock \
  -- scripts/ops/run_v3_open_loop_probe.sh

# macOS: powermetrics（権限により collection_ok=false になる場合あり）
python3 scripts/ops/run_ai_perf_probe.py \
  --counter-mode mac-powermetrics \
  --provider mock \
  -- scripts/ops/run_v3_open_loop_probe.sh
```

OpenAIで解析する場合:

```bash
export OPENAI_API_KEY=...your_key...
python3 scripts/ops/run_ai_perf_probe.py \
  --counter-mode auto \
  --provider openai \
  --model gpt-5-nano \
  -- scripts/ops/run_v3_open_loop_probe.sh
```

出力物:
- `var/results/<RUN_NAME>.perf.json`
- `var/results/<RUN_NAME>.triage.json`（`--skip-ai` なし時）
- `var/results/<RUN_NAME>.perf_stat.csv`（Linux `perf` 使用時）
- `var/results/<RUN_NAME>.powermetrics.txt` / `.powermetrics.err.txt`（macOS `powermetrics` 使用時）

ベースライン比較（差分%）:

```bash
python3 scripts/ops/run_ai_perf_probe.py \
  --counter-mode auto \
  --baseline-run-name <BASELINE_RUN_NAME> \
  --provider mock \
  -- scripts/ops/run_v3_open_loop_probe.sh
```

設計: `docs/ops/ai_rag_agent_triage_design.md`

## 運用上の注意
- `ack_p99` 単独で品質判定しない。
- `accepted_rate` と `ack_accepted_p99` を必ず併記する。
- ベンチ結果は `var/results/` と正本ドキュメントに紐付けて残す。

## READMEポリシー
- 本リポジトリの管理対象 README は `README.md`（本ファイル）を正本とする。
- サブディレクトリ README は廃止し、必要情報は本ファイルか `docs/ops/` 正本へ集約する。
- 退避した旧 README:
  - `docs/old/adr_decisions_readme_legacy.md`
  - `docs/old/frontend_readme_legacy.md`
  - `docs/old/gateway_rust_readme_legacy.md`
