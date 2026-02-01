# HFT Fast Path Event Switchyard

実装途中
---

## クイックスタート（3分）

### 1. アプリケーション起動

```bash
# ヘルプ表示
make

# アプリケーション起動
make run

# 別ターミナルで以下を実行:
make dev-bench       # ベンチマーク実行
make stats           # 統計表示
make metrics         # Prometheusメトリクス表示
make dashboard       # リアルタイムダッシュボード (Ctrl+C で終了)
```

### 2. グラフ可視化（Grafana + Prometheus）

```bash
# Grafana起動 → http://localhost:3000 (admin/admin)
make grafana

# ダッシュボード: "HFT Fast Path - Real-time Performance"
# Grafana停止
make grafana-down
```

**アクセス先**:
- **UI (開発)**: http://localhost:5173 （Vite dev server）
- **UI (本番寄せ/Nginx)**: http://localhost/ （Nginxが静的配信 + /api を中継）
- **API (外部)**: http://localhost/api （Nginx経由）
- Grafana監視: http://localhost:3000 (admin/admin)
- Prometheus: http://localhost:9090

ポートの考え方（実務寄せ）
- 外部公開は **Nginx(80/443)** のみ（UIと/apiはNginx経由）。
- Appは **8080固定**（コード直指定）で内部専用。変更する場合はコード修正が必要。
- Gatewayは `GATEWAY_PORT`（デフォルト8081）、BackOfficeは `BACKOFFICE_PORT`（デフォルト8082）。

### 3. ワンコマンドで全実行

```bash
# ビルド→起動→ベンチ→メトリクス
make all
```

### 4. 停止

```bash
# アプリケーション停止
make stop

# 監視スタック停止
make grafana-down
```

---

## TCP取引所シミュレータ（体感差比較）

HTTP/Simulator と TCP で「受理→約定レイテンシ」の体感差を比べる最小セット。

### 1) TCP取引所シミュレータを起動

```bash
./gradlew :gateway:run -PmainClass=gateway.exchange.TcpExchangeSimulatorMainKt
```

### 2) Gateway を TCP で起動

```bash
EXCHANGE_TRANSPORT=tcp ./gradlew :gateway:run
```

### 3) 比較スモーク（metricsの差を確認）

```bash
scripts/ops/exchange_transport_smoke.sh tcp
scripts/ops/exchange_transport_smoke.sh sim
```

**補足**:
- 既定のTCPポートは `EXCHANGE_TCP_PORT=9901`
- `EXCHANGE_TRANSPORT=sim` は従来の ExchangeSimulator を使用

---

## ユースケース別コマンド

### 開発

| ユースケース | コマンド | 説明 |
|------------|---------|------|
| アプリ起動 | `make run` | Fast Path有効・メトリクス公開 |
| ビルド | `make build` | Gradleビルド実行 |
| テスト | `make test` | ユニットテスト実行 |
| クリーンビルド | `make clean` | `./gradlew clean build` |
| 停止 | `make stop` | 実行中プロセスをkill |

### 監視・メトリクス

| ユースケース | コマンド | 説明 |
|------------|---------|------|
| Grafana起動 | `make grafana` | Prometheus + Grafana起動 |
| Grafana停止 | `make grafana-down` | 監視スタック停止 |
| メトリクス表示 | `make metrics` | Prometheusメトリクス（ターミナル） |
| 統計情報 | `make stats` | JSON形式統計情報 |
| ヘルスチェック | `make health` | `/health` エンドポイント確認 |
| リアルタイムダッシュボード | `make dashboard` | ターミナル上のライブ更新 |

### ベンチマーク・検証

| ユースケース | コマンド | 説明 |
|------------|---------|------|
| 開発ベンチ | `make dev-bench` | 10,000リクエスト負荷テスト |
| CI/CDベンチ | `make bench` | 性能回帰テスト用 |
| 性能ゲート | `make gate` | SLO検証（p99 < 100μs） |
| ゲート+ベンチ | `make check` | `bench` + `gate` |
| ベースライン更新 | `make bless` | 良い結果をベースラインに昇格 |
| Kafka疎通（Gate2） | `scripts/ops/kafka_smoke_check.sh` | Kafka publishの疎通・キュー収束確認 |
| Rust PRゲート | `make perf-gate-rust-ci` | 実測 + 回帰 + strict |
| Rust Nightly | `make perf-gate-rust-nightly` | 実測 + full report |
| Rust baseline更新 | `make perf-gate-rust-bless` | 明示的なベースライン更新 |

---

## パフォーマンス

**実測値** (10,000イベント送信):

| 指標 | 値 | 目標 | 状態 |
|------|-----|------|------|
| **Fast Path p99** | **29.79μs** | < 100μs | ✅ 70.2%余裕 |
| Fast Path p50 | 15.32μs | < 50μs | ✅ |
| スループット | 12,000+ req/s | > 10,000 req/s | ✅ |
| ドロップ数 | 0 | 0 | ✅ |
| エラー数 | 0 | 0 | ✅ |
| Persistence Queue p99 | 188.67μs | - | - |
| Tail Ratio (p99/p50) | 1.94 | < 12.0 | ✅ |

**性能特性**:
- Zero GC: ホットパスでのGCなし
- データロスゼロ: Chronicle Queue経由で完全永続化

**高負荷実測（最新）**:
- Fast Path (60s, target 1000 RPS): actual_rps=816.4, avg_p99_us=770.66, max_p99_us=3668.71
- SSE (200 conns, 60s): events=200, errors=0
- Kafka有効 (60s, target 1000 RPS): actual_rps=815.93, avg_p99_us=700.39, max_p99_us=4879.58
- 実測日時: 2026-02-01 00:17-00:19 (local)
- コマンド: `python3 scripts/ops/bench_gateway.py sustained --duration 60 --rps 1000 --port 18084 --secret secret123`,
  `python3 scripts/ops/sse_load_test.py --url http://localhost:18084/stream --connections 200 --duration 60 --jwt-secret secret123 --account-id acct_demo`,
  `KAFKA_ENABLE=1 JWT_HS256_SECRET=secret123 GATEWAY_PORT=18085 ./gradlew :gateway:run`
- 詳細: `results/bench_20260201_001705.json`, `results/load_fastpath_20260201_001705.txt`, `results/load_sse_20260201_001705.txt`,
  `results/bench_20260201_001958.json`, `results/load_kafka_20260201_001958.txt`, `results/metrics_fastpath_20260201_001705.txt`, `results/metrics_kafka_20260201_001958.txt`

---

## アーキテクチャ

### 全体構成

```
┌─────────────────────────────────────────────────────────────┐
│                      HFT Event Switchyard                   │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐       ┌──────────────┐                  │
│  │   HTTP API   │       │  Kafka Input │                  │
│  │   :8080      │       │   Consumer   │                  │
│  └───────┬──────┘       └───────┬──────┘                  │
│          │                      │                          │
│          └──────────┬───────────┘                          │
│                     │                                      │
│            ┌────────▼────────┐                            │
│            │   Fast Path     │ ← LMAX Disruptor           │
│            │ (Ring Buffer)   │   Zero-copy processing     │
│            └────────┬────────┘   HDR Histogram            │
│                     │            p99 < 100μs              │
│          ┌──────────┼──────────┐                          │
│          │          │          │                          │
│    ┌─────▼─────┐ ┌──▼────┐ ┌──▼──────────┐              │
│    │ In-Memory │ │Metrics│ │ Persistence │              │
│    │ Orderbook │ │Export │ │   Queue     │              │
│    │           │ │       │ │ (Chronicle) │              │
│    └───────────┘ └───────┘ └─────────────┘              │
│                                                             │
└─────────────────────────────────────────────────────────────┘

         │                     │                   │
         │                     │                   │
         ▼                     ▼                   ▼
   ┌──────────┐         ┌──────────┐        ┌──────────┐
   │Kafka Out │         │Prometheus│        │Chronicle │
   │(events)  │         │:9090     │        │Queue     │
   └──────────┘         └─────┬────┘        │(Disk)    │
                              │             └──────────┘
                              ▼
                        ┌──────────┐
                        │ Grafana  │
                        │  :3000   │
                        └──────────┘
```

### データフロー

```
HTTP Request → Router → Fast Path Disruptor (Ring Buffer #1, Thread.MAX_PRIORITY)
                             ↓ p99: 29.79μs
                        TradeEventHandler
                             ├─ ビジネスロジック
                             └─ Persistence Queue公開 (非同期)
                                  ↓
                        Persistence Queue Disruptor (Ring Buffer #2, Thread.NORM_PRIORITY)
                             ↓ p99: 188μs (Fast Pathと並列)
                        PersistenceEventHandler
                             └─ Chronicle Queue書き込み
                                  ↓
                        Chronicle Queue (永続化)
                             ↓
                        Kafka Bridge (非同期転送)
                             ↓
                        Kafka
```

### 主要コンポーネント

#### 1. Fast Path (LMAX Disruptor #1)
**役割**: 超低レイテンシイベント処理パイプライン

**実装**: [app/src/main/kotlin/app/fast/FastPathEngine.kt](app/src/main/kotlin/app/fast/FastPathEngine.kt)

**設定**:
- Ring Bufferサイズ: 65536（2の累乗）
- Wait Strategy: YieldingWaitStrategy
- スレッド優先度: MAX_PRIORITY

**データフロー**:
```
HTTP Request → Ring Buffer → EventHandler → Orderbook Update → Response
              (0コピー)     (インライン処理)   (メモリ内)
```

**性能特性**:
- p99レイテンシ: 29.79μs（目標: <100μs）✅
- スループット: 12,000+ req/s
- ゼロGC: Immutable data class使用

#### 2. Persistence Queue (LMAX Disruptor #2)
**役割**: 耐障害性のための非同期永続化

**実装**: [app/src/main/kotlin/app/fast/PersistenceQueue.kt](app/src/main/kotlin/app/fast/PersistenceQueue.kt)

**設定**:
- Ring Bufferサイズ: 32768
- Wait Strategy: BlockingWaitStrategy
- スレッド優先度: NORM_PRIORITY

**データフロー**:
```
Fast Path → Disruptor (Async) → Chronicle Queue → Disk
          (非同期キュー)         (mmap)
```

#### 3. Chronicle Queue
**役割**: Memory-mapped永続化

**実装**: [app/src/main/kotlin/app/kafka/ChronicleQueueWriter.kt](app/src/main/kotlin/app/kafka/ChronicleQueueWriter.kt)

**特性**:
- メモリマップドファイル（off-heap）
- Fast Pathとは別スレッドで動作（クリティカルパスに影響しない）
- クラッシュ時もファイルから復旧可能

#### 4. Kafka Bridge
**役割**: Chronicle Queue → Kafka非同期転送

**実装**: [app/src/main/kotlin/app/kafka/KafkaBridge.kt](app/src/main/kotlin/app/kafka/KafkaBridge.kt)

**設定**:
- 100イベントバッチ
- 50msタイムアウト

#### 5. HDR Histogram
**役割**: 高精度パーセンタイル計測

**実装**: [app/src/main/kotlin/app/metrics/Metrics.kt](app/src/main/kotlin/app/metrics/Metrics.kt)

**特性**:
- p50/p99/p999を正確に計測
- メモリ効率的（圧縮アルゴリズム）

#### 6. HTTP API (Java HttpServer)
**役割**: 外部からのイベント受信とメトリクス公開

**実装**: [app/src/main/kotlin/app/http/HttpIngress.kt](app/src/main/kotlin/app/http/HttpIngress.kt)

**エンドポイント**:
```
POST /events?key={key}    - イベント受信
GET  /stats               - JSON統計情報
GET  /metrics             - Prometheus形式メトリクス
GET  /health              - ヘルスチェック
GET  /api/admin           - システム設定API (Operations Dashboard用)
```

#### 7. 監視スタック (Prometheus + Grafana)
**役割**: リアルタイム性能監視と可視化

**自動プロビジョニング**:
- データソース: [monitoring/grafana/provisioning/datasources/prometheus.yml](monitoring/grafana/provisioning/datasources/prometheus.yml)
- ダッシュボード: [monitoring/grafana/dashboards/fast-path.json](monitoring/grafana/dashboards/fast-path.json)

---

## API エンドポイント

### POST /events

イベント送信

```bash
curl "http://localhost:8080/events?key=BTC" \
  -X POST -H "Content-Type: application/json" \
  -d '{"price": 50000.00, "volume": 1.5}'
```

**レスポンス**:
- `200 OK`: 処理成功
- `409 NOT_OWNER`: 処理拒否 (バッファ満杯等)
- `400 BAD_REQUEST`: keyパラメータ不足
- `500 INTERNAL_SERVER_ERROR`: サーバーエラー

### GET /stats

統計情報取得 (JSON)

```bash
curl http://localhost/stats
```

**レスポンス例**:
```json
{
  "fast_path_count": 1628,
  "fast_path_avg_publish_us": 2.559,
  "fast_path_avg_process_us": 14.844,
  "fast_path_publish_p50_us": 1.333,
  "fast_path_publish_p99_us": 7.503,
  "fast_path_process_p50_us": 11.919,
  "fast_path_process_p99_us": 41.183,
  "fast_path_drop_count": 0,
  "persistence_queue_error_count": 0,
  "persistence_queue_write_p99_us": 188.671
}
```

### GET /metrics

Prometheusメトリクス

```bash
curl http://localhost/metrics
```

**主要メトリクス**:
- `fast_path_process_latency_p99_microseconds`: Fast Path処理p99レイテンシ
- `fast_path_process_latency_p50_microseconds`: Fast Path処理p50レイテンシ
- `fast_path_events_total`: Fast Path処理イベント総数
- `fast_path_drops_total`: ドロップ数
- `fast_path_lag`: Fast Path遅延
- `persistence_queue_write_latency_p99_microseconds`: Persistence Queue書き込みp99レイテンシ
- `persistence_queue_lag`: Persistence Queue遅延
- `persistence_queue_errors_total`: Persistence Queueエラー数
- `jvm_heap_used_bytes`: JVMヒープ使用量
- `jvm_heap_max_bytes`: JVMヒープ最大量
- `jvm_thread_count`: JVMスレッド数

### GET /health

ヘルスチェック

```bash
curl http://localhost/health
```

**レスポンス例**:
```json
{
  "status": "healthy",
  "timestamp": 1698765432000,
  "uptime_ms": 123456,
  "uptime_human": "2m 3s",
  "jvm": {
    "memory": {
      "heap_used_mb": 256,
      "heap_max_mb": 1024,
      "heap_usage_percent": 25.0
    },
    "threads": {
      "count": 15,
      "peak": 20,
      "daemon": 10
    }
  },
  "fast_path": {
    "enabled": true,
    "p99_us": 41.183,
    "drop_count": 0,
    "error_count": 0
  }
}
```

**ステータス**:
- `healthy`: すべて正常 (HTTP 200)
- `degraded`: 一部劣化 (drop_count > 0 or lag > 10000) (HTTP 200)
- `unhealthy`: 異常 (error_count > 100) (HTTP 503)

**判定基準**:
- `unhealthy`: p99 > 200μs、ドロップ > 100、エラー > 10、メモリ > 95%
- `degraded`: p99 > 100μs、ドロップ > 0、エラー > 0、メモリ > 85%
- `healthy`: 上記以外

---

## Grafanaダッシュボード

### 使い方

1. アプリケーション起動
```bash
make run
```

2. Grafana起動
```bash
make grafana
```

3. ブラウザでアクセス
   - URL: http://localhost:3000
   - ユーザ: admin
   - パスワード: admin

4. ダッシュボード確認
   - "HFT Fast Path - Real-time Performance" を開く

5. ベンチマーク実行 (別ターミナル)
```bash
make dev-bench
```

グラフがリアルタイムで更新されます (1秒間隔)。

### 11パネル構成

| 行 | パネル | メトリクス | 閾値 |
|----|--------|-----------|------|
| 1 | Fast Path Latency (p99/p50) | `fast_path_process_latency_p99_microseconds` | p99 < 100μs |
| 1 | Current p99 Gauge | 同上 | 100μs（赤） |
| 1 | Throughput | `rate(fast_path_events_total[5s])` | - |
| 2 | Total Events | `fast_path_events_total` | - |
| 2 | Drops | `fast_path_drops_total` | 0（理想） |
| 3 | Fast Path Lag | `fast_path_lag` | - |
| 3 | JVM Heap | `jvm_heap_used_bytes / jvm_heap_max_bytes` | <80% |
| 4 | Thread Count | `jvm_thread_count` | - |
| 4 | Persistence p99 | `persistence_queue_write_latency_p99_microseconds` | - |
| 5 | Persistence Lag | `persistence_queue_lag` | - |
| 5 | Persistence Errors | `persistence_queue_errors_total` | 0（理想） |

---

## 環境変数

### Fast Path設定

| 変数名 | デフォルト | 説明 |
|--------|-----------|------|
| `FAST_PATH_ENABLE` | `1` (有効) | `0`で無効化 |
| `FAST_PATH_METRICS` | `0` | `1`でメトリクス収集を有効化 |
| `FAST_PATH_SYMBOLS` | (全て) | Fast Path対象シンボル (カンマ区切り、例: `BTC,ETH`) |
| `FAST_PATH_FALLBACK` | `1` | `1`でFast Path失敗時にSlow Pathへフォールバック |

### Chronicle Queue & Kafka設定

| 変数名 | デフォルト | 説明 |
|--------|-----------|------|
| `KAFKA_BRIDGE_ENABLE` | `1` (有効) | `0`で無効化 |
| `CHRONICLE_QUEUE_PATH` | `./data/queue` | Chronicle Queueデータディレクトリ |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafkaブートストラップサーバー |
| `KAFKA_TOPIC` | `events` | Kafka送信先トピック |

### その他

| 変数名 | デフォルト | 説明 |
|--------|-----------|------|
| `ENGINE_ID` | e1 | エンジンインスタンスID |
| `ETCD_ENDPOINTS` | http://127.0.0.1:2379 | etcdエンドポイント (カンマ区切り) |
| `OWNED_KEYS` | - | 担当キー (カンマ区切り、例: BTC,ETH) |
| `ENGINE_POLL_MS` | 300 | etcd polling間隔 (ms) |
| `RAG_ENABLE` | 0 | RAGベース初動トリアージ有効化 (1: 有効) |
| `ORDER_POOL_ENABLE` | `0` | オブジェクトプール有効化 |
| `AUDIT_LOG_ENABLE` | `0` | 監査ログ有効化 |
| `ZGC_ENABLE` | `0` | ZGC有効化 |

### 戦略設定（App / Strategy）

| 変数名 | デフォルト | 説明 |
|--------|-----------|------|
| `APP_DB_URL` | `jdbc:postgresql://localhost:5432/backoffice` | Strategy設定のDB接続URL |
| `APP_DB_USER` | `backoffice` | DBユーザー |
| `APP_DB_PASSWORD` | `backoffice` | DBパスワード |
| `APP_DB_POOL` | `4` | DBプールサイズ |
| `STRATEGY_ADMIN_TOKEN` | (空) | `PUT /api/strategy` のBearerトークン。空なら認可なし |
| `STRATEGY_CONFIG_REFRESH_MS` | `5000` | Strategy設定の再読込間隔(ms) |
| `STRATEGY_AUTO_ENABLE` | `1` | 1でStrategyAutoTraderを起動 |
| `STRATEGY_SYMBOLS` | `7203,6758,9984` | 戦略の初期銘柄(CSV) |
| `STRATEGY_TICK_MS` | `1000` | 戦略の初期tick間隔(ms) |
| `STRATEGY_MAX_ORDERS_PER_MIN` | `0` | 初期の発注上限(0=無制限) |
| `STRATEGY_COOLDOWN_MS` | `0` | 初期のクールダウン(ms) |

**フロントの環境変数**:
- `VITE_STRATEGY_ADMIN_TOKEN`: Strategy設定更新用のBearerトークン。`STRATEGY_ADMIN_TOKEN` と同じ値にする。

**マイグレーション**:
- 起動時にFlywayが `app/src/main/resources/db/migration` のSQLを自動適用する。
- DB未接続の場合は `fallback` になり、`PUT /api/strategy` は 503 を返す（自動戦略は無効化）。

### 起動パターン

```bash
# Fast Path + メトリクス（推奨）
FAST_PATH_ENABLE=1 FAST_PATH_METRICS=1 ./gradlew run

# Kafka連携付き
FAST_PATH_ENABLE=1 FAST_PATH_METRICS=1 KAFKA_BRIDGE_ENABLE=1 ./gradlew run

# ZGC（低レイテンシGC）
ZGC_ENABLE=1 FAST_PATH_ENABLE=1 FAST_PATH_METRICS=1 ./gradlew run
```

---

## ローカル環境（従来の方法）

```bash
# 1. ビルド
./gradlew build

# 2. Fast Path有効化で起動
FAST_PATH_ENABLE=1 FAST_PATH_METRICS=1 ./gradlew run

# 3. オーダーブックベンチマーク実行 (Python推奨)
python3 scripts/bench_orderbook.py --runs 10000

# 4. 統計確認
curl http://localhost/stats
curl http://localhost/metrics  # Prometheus形式
curl http://localhost/health
```

---

## SLOカナリーテスト（開発者向け）

```bash
# アプリケーション起動 (別ターミナル)
FAST_PATH_ENABLE=1 FAST_PATH_METRICS=1 ./gradlew run

# クイックカナリー実行
python3 scripts/quick_canary.py \
  --url http://localhost \
  --events 1000 \
  --out var/results/canary.json

# SLOゲート検証
python3 scripts/slo_gate.py \
  --in var/results/canary.json

# バックプレッシャーチューナー
python3 scripts/backpressure_tuner.py \
  --in var/results/canary.json

# コストガード
python3 scripts/cost_guard.py \
  --in var/results/canary.json \
  --budget-yen-per-1k 12.0

# 変更リスク分析
python3 scripts/change_risk.py \
  --diff-base main \
  --out var/results/change_risk.json
```

---

## CI Gate Policy（開発/CI 共通）

Gate0（PR）: 数分で終わる軽量ゲート。単体テスト/契約/簡易faultまで。  
Gate1（main）: 回帰検知の中量ゲート。bench + 性能ゲート。  
Gate2（nightly）: 長時間/重い検証を含む重量ゲート。
Gate2 の性能ゲートは専用ランナーで固定し、`RUNNER_LABEL` に紐づくベースラインを比較する。
個人開発では専用ランナーが用意できるまで nightly のスケジュール実行は停止し、手動実行に留める。

```bash
# Gate0: PR
make check-lite

# Gate1: main
make check

# Gate2: nightly/weekly
make check-full
```

補足:
- Gate2 の性能ゲートは `RUNNER_LABEL` ごとのベースラインを使用する（例: `baseline/perf_gate_rust_perf-gate-rust.json`）。
- ベースライン更新は同一ランナーで `make perf-gate-rust-bless` を実行する。

---

## Docker Compose

### 監視スタック込みフルスタック起動

```bash
# Prometheus + Grafana + Kafkaを含むフルスタック起動
docker-compose -f docker-compose.monitoring.yml up -d

# アクセス
# - API: http://localhost/api
# - Grafana: http://localhost:3000 (admin/admin)
# - Prometheus: http://localhost:9090
```

---

## フロント配信（本番寄せ: Nginx + MinIO）

本番寄せの構成は「Nginxが入口 / 静的ファイルはS3(=MinIO) / APIはAppへ同一オリジンで中継」。

```bash
# フロントビルド→MinIOへアップロード
scripts/ops/frontend_upload_minio.sh

# Nginx + App + Gateway + BackOffice をまとめて起動
docker compose --profile frontend up -d nginx
```

アクセス:
- UI: http://localhost/

補足:
- 直接MinIOを公開しない構成（本番寄せ）。必要なら `docker compose --profile frontend up -d minio` で確認可能。
- `/api/*` と `/ws/*` は Nginx が App へ転送するため、フロントは同一オリジンで動作。
- デモデータを自動生成する場合は `STRATEGY_AUTO_ENABLE=1 docker compose --profile frontend up -d nginx`

---

## Kubernetes

```bash
# 1. 事前準備
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/priorityclass.yaml

# 2. 設定
vim k8s/configmap.yaml  # etcd/Kafkaエンドポイント編集
kubectl apply -f k8s/configmap.yaml

# 3. デプロイ
kubectl apply -f k8s/pvc.yaml
kubectl apply -f k8s/service.yaml
kubectl apply -f k8s/deployment.yaml

# 4. 確認
kubectl get pods -n hft-system
kubectl logs -n hft-system -l app=hft-fast-path
```

---

## パッケージ構成

### app/src/main/kotlin/app/

```
app/
├── Main.kt                        # エントリーポイント、環境変数読み込み
├── control/
│   ├── Assigner.kt               # リーダー選出・キー割り当て
│   └── LeaseRegistrar.kt         # etcd lease管理
├── engine/
│   ├── Engine.kt                 # イベントエンジン (旧実装、Fast Path無効時に使用)
│   └── Router.kt                 # HTTPリクエストルーティング
├── fast/
│   ├── FastPathEngine.kt         # Fast Path実装 (LMAX Disruptor #1)
│   └── PersistenceQueue.kt       # Persistence Queue (Disruptor #2)
├── kafka/
│   ├── ChronicleQueueWriter.kt   # Chronicle Queue書き込み
│   └── KafkaBridge.kt            # Kafka非同期転送
├── http/
│   ├── HttpIngress.kt            # HTTPサーバー、POST /events受付
│   ├── StatsController.kt        # GET /stats (JSON統計)
│   ├── MetricsController.kt      # GET /metrics (Prometheus)
│   └── HealthController.kt       # GET /health
├── metrics/
│   └── Metrics.kt                # HDR Histogram、Prometheusメトリクス管理
├── ownership/
│   ├── Ownership.kt              # キーオーナーシップ管理
│   └── AssignmentPoller.kt       # etcd polling
├── cdc/
│   └── CdcQueue.kt               # Change Data Capture (CDC用、未使用)
├── log/
│   ├── LogQueue.kt               # ログキュー (未使用)
│   └── LogWriter.kt              # ログ書き込み (未使用)
└── util/
    └── JsonLine.kt               # JSON行フォーマット
```

### scripts/

```
scripts/
├── quick_canary.py               # クイックカナリーテスト (合成トラフィック生成)
├── slo_gate.py                   # SLO検証ゲート (CI統合用)
├── backpressure_tuner.py         # バックプレッシャー自動チューナー
├── replay_minset.py              # 15分最小リプレイ抽出
├── generate_runbook_suggest.py   # RAGベース初動トリアージ
├── cost_guard.py                 # コストガード (予算監視)
├── dlp_guard.py                  # PIIガード (個人情報検出)
├── change_risk.py                # 変更リスクスコアリング
├── agg_to_bench.py               # ベンチマーク集計 (汎用)
└── measure_cmd.py                # コマンド実行時間測定
```

### docs/

```
docs/
├── specs/
│   └── slo.md                    # SLO仕様 (目標値・閾値・アラート)
├── adr/
│   ├── 001-slo-regression-gate.md
│   ├── 002-backpressure-auto-tuner.md
│   ├── 003-minimal-replay-strategy.md
│   ├── 004-rag-based-triage.md
│   ├── 005-cost-pii-guards.md
│   └── 006-change-risk-scoring.md
├── runbook.md                    # インシデント対応手順
├── change_risk.md                # 変更リスク評価ガイド
└── GRAFANA_IMPLEMENTATION_LEARNINGS.md  # Grafana実装の学習ログ
```

### contracts/

```
contracts/
└── bench.v1.schema.json          # ベンチマーク結果JSON Schema Draft 7
```

### bench/profiles/

```
bench/profiles/
├── burst.yaml                    # バーストトラフィック (1000 events/s)
├── skew.yaml                     # スキュートラフィック (BTC 70%)
└── steady.yaml                   # 定常トラフィック (100 events/s)
```

---

## 各ソースがやっていること

### コアロジック

### Rust Gateway（gateway-rust）

#### [gateway-rust/src/main.rs](gateway-rust/src/main.rs)
- **役割**: Rust版Gatewayのエントリーポイント
- **処理**:
  1. 設定読み込み（port/queue/TTL）
  2. AuditLog/SSE/Bus/Outboxを初期化
  3. Exchange worker または drain worker を起動
  4. HTTP/TCPサーバーを並行起動

#### [gateway-rust/src/server/http/mod.rs](gateway-rust/src/server/http/mod.rs)
- **役割**: HTTP入口のルーティングと状態管理
- **処理**:
  1. JWT認証
  2. IdempotencyKey判定（ShardedOrderStore）
  3. Risk→Queue投入（FastPath）
  4. 監査ログ・SSE・Busへの通知
  5. Backpressure判定（inflight/WAL/ディスク）

#### [gateway-rust/src/server/http/orders.rs](gateway-rust/src/server/http/orders.rs)
- **役割**: 注文受理・取得・キャンセルのHTTPハンドラ
- **処理**:
  - POST /orders → FastPathEngine → 202（`acceptSeq` / `requestId` を返す）
  - GET /orders/{id} → 注文状態
  - GET /orders/client/{client_order_id} → PENDING/DURABLE/REJECTED/UNKNOWN
  - POST /orders/{id}/cancel → キャンセル要求
  - Idempotency-Key もしくは client_order_id を必須にして再送可能にする

#### [gateway-rust/src/server/http/audit.rs](gateway-rust/src/server/http/audit.rs)
- **役割**: 監査イベント取得 / 監査ログ検証 / アンカー取得

#### [gateway-rust/src/server/http/sse.rs](gateway-rust/src/server/http/sse.rs)
- **役割**: SSEストリーム配信（リプレイ + ブロードキャスト）

#### [gateway-rust/src/server/http/metrics.rs](gateway-rust/src/server/http/metrics.rs)
- **役割**: health/metrics の運用エンドポイント

#### [gateway-rust/src/engine/fast_path.rs](gateway-rust/src/engine/fast_path.rs)
- **役割**: FastPathエンジン（受理→キュー）
- **処理**:
  1. Riskチェック
  2. ロックフリーQueueへ投入
  3. レイテンシ計測

#### [gateway-core/src/queue.rs](gateway-core/src/queue.rs)
- **役割**: ロックフリーリングバッファ（FastPathQueue）
- **処理**:
  - 満杯なら即座にreject（バックプレッシャー）

#### [gateway-rust/src/store/sharded.rs](gateway-rust/src/store/sharded.rs)
- **役割**: シャーディングOrderStore
- **処理**:
  - account_idハッシュで分散し、RwLock競合を軽減
  - idempotencyの直列化

#### [gateway-rust/src/audit/mod.rs](gateway-rust/src/audit/mod.rs)
- **役割**: 監査ログ（append-only）
- **処理**:
  - JSONL追記、ハッシュチェーン、検証API
  - WAL enqueue / durable ACK の計測（t0/t1/t2）
  - 監査ミラーは WAL から非同期再生成（`AUDIT_MIRROR_ENABLE=1`）
  - WAL 出力先: `GATEWAY_WAL_PATH`（旧 `GATEWAY_AUDIT_PATH` 互換）
  - 監査ログ出力先: `AUDIT_LOG_PATH`（ミラーの書き込み先）
  - ミラーのhash/anchor: `AUDIT_MIRROR_HASH_PATH` / `AUDIT_MIRROR_ANCHOR_PATH`
  - ミラーは起動時に最新アンカーを再生成（再起動時の整合維持）

### 監査ミラーの動作確認

目的: WAL から監査ログ（mirror）を再生成し、hash/anchor が更新されることを確認。

```bash
# 1) 起動（別ターミナル）
AUDIT_MIRROR_ENABLE=1 \
AUDIT_HMAC_KEY=secret123 \
scripts/ops/run_dynamic_inflight.sh
```

```bash
# 2) 注文投入（別ターミナル）
TOKEN=$(JWT_SECRET=secret123 ACCOUNT_ID=1 python3 - <<'PY'
import os, json, base64, hmac, hashlib, time
secret = os.environ["JWT_SECRET"].encode()
header = {"alg":"HS256","typ":"JWT"}
payload = {"sub":os.environ["ACCOUNT_ID"],"iat":int(time.time())-10,"exp":int(time.time())+3600}
def b64url(b): return base64.urlsafe_b64encode(b).rstrip(b"=").decode()
h = f"{b64url(json.dumps(header,separators=(',',':')).encode())}.{b64url(json.dumps(payload,separators=(',',':')).encode())}"
sig = hmac.new(secret, h.encode(), hashlib.sha256).digest()
print(f"{h}.{b64url(sig)}")
PY
)

curl -s -X POST http://127.0.0.1:8082/orders \
  -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: test-mirror" \
  -H "Content-Type: application/json" \
  -d '{"symbol":"BTC","side":"BUY","type":"LIMIT","qty":1,"price":100}'
```

```bash
# 3) 生成物を確認
ls -l gateway-rust/var/gateway/audit.mirror.log \
      gateway-rust/var/gateway/audit.mirror.log.hash \
      gateway-rust/var/gateway/audit.mirror.log.anchor

tail -n 2 gateway-rust/var/gateway/audit.mirror.log
tail -n 2 gateway-rust/var/gateway/audit.mirror.log.hash
cat gateway-rust/var/gateway/audit.mirror.log.anchor
```

#### [gateway-rust/src/outbox/mod.rs](gateway-rust/src/outbox/mod.rs)
- **役割**: 監査ログ → Kafka 送信（Outbox）
- **処理**:
  - 監査ログを読み取り、Kafkaへbest-effort配信
  - offsetはfsyncで保護

#### [gateway-rust/src/bus/mod.rs](gateway-rust/src/bus/mod.rs)
- **役割**: Kafka Publisher
- **処理**:
  - rdkafkaでpublish、delivery結果を集計

#### Rust Gateway: WAL計測 / Backpressure
- **計測ポイント**:
  - t0: 受付（HTTP到達）
  - t1: WAL enqueue（監査ログにenqueue完了）
  - t2: durable ACK（fdatasync完了、SSEで通知）
- **主要メトリクス**:
  - `gateway_ack_p50_us` / `gateway_ack_p99_us` / `gateway_ack_p999_us`
  - `gateway_wal_enqueue_p50_us` / `gateway_wal_enqueue_p99_us` / `gateway_wal_enqueue_p999_us`
  - `gateway_durable_ack_p50_us` / `gateway_durable_ack_p99_us` / `gateway_durable_ack_p999_us`
  - `gateway_fdatasync_p50_us` / `gateway_fdatasync_p99_us` / `gateway_fdatasync_p999_us`
  - `gateway_fast_path_processing_p50_us` / `gateway_fast_path_processing_p99_us` / `gateway_fast_path_processing_p999_us`
  - `gateway_inflight`
  - `gateway_durable_inflight`
  - `gateway_inflight_dynamic_enabled`
  - `gateway_inflight_limit_dynamic`
  - `gateway_backpressure_soft_reject_rate_ewma`
  - `gateway_durable_commit_rate_ewma`
  - `gateway_wal_age_ms`
  - `gateway_backpressure_soft_wal_age_total`
  - `gateway_backpressure_soft_rate_decline_total`
  - `gateway_backpressure_inflight_total`
  - `gateway_backpressure_wal_bytes_total`
  - `gateway_backpressure_wal_age_total`
  - `gateway_backpressure_disk_free_total`
- **durable通知**:
  - SSE `event: order_durable` を注文/アカウントストリームへ送信
- **関連環境変数**:
  - `AUDIT_FDATASYNC` (default: true) fdatasync有効化
  - `AUDIT_ASYNC_WAL` (default: true) WAL非同期 + バッチfsync
  - `AUDIT_FDATASYNC_MAX_WAIT_US` (default: 200)
  - `AUDIT_FDATASYNC_MAX_BATCH` (default: 64)
  - `BACKPRESSURE_INFLIGHT_MAX`
  - `BACKPRESSURE_INFLIGHT_DYNAMIC`
  - `BACKPRESSURE_INFLIGHT_TARGET_WAL_AGE_SEC`
  - `BACKPRESSURE_INFLIGHT_ALPHA`
  - `BACKPRESSURE_INFLIGHT_BETA`
  - `BACKPRESSURE_INFLIGHT_MIN`
  - `BACKPRESSURE_INFLIGHT_CAP`
  - `BACKPRESSURE_INFLIGHT_TICK_MS`
  - `BACKPRESSURE_INFLIGHT_SLEW_RATIO`
  - `BACKPRESSURE_INFLIGHT_HYSTERESIS_OFF_RATIO`
  - `BACKPRESSURE_INFLIGHT_INITIAL`
  - `BACKPRESSURE_INFLIGHT_DECLINE_RATIO`
  - `BACKPRESSURE_INFLIGHT_DECLINE_STREAK`
  - `BACKPRESSURE_INFLIGHT_REJECT_BETA`
  - `BACKPRESSURE_SOFT_WAL_AGE_MS_MAX`
  - `BACKPRESSURE_WAL_BYTES_MAX`
  - `BACKPRESSURE_WAL_AGE_MS_MAX`
  - `BACKPRESSURE_DISK_FREE_PCT_MIN`
 - **計測メモ（ローカル, 2026-01-25 / HDR）**:
   - RTT(HTTP /orders) p99: **~371µs**（200 req / warmup 50）
   - Throughput: **~11.5k req/s**（10s, concurrency=4, accounts=1）
   - ACK p50/p99/p999: **12µs / 67µs / 144µs**
   - WAL enqueue p50/p99/p999: **11µs / 64µs / 142µs**
   - durable ACK p50/p99/p999: **~4.2ms / ~6.5ms / ~10.4ms**
   - fdatasync p50/p99/p999: **~2.7ms / ~3.7ms / ~6.4ms**
   - 判断: fdatasync p99 が 200µs を超えるため、永続ACKの同期方式/バッチング設計を見直す前提。
   - 備考: Exchange未接続の場合は `FASTPATH_DRAIN_ENABLE=1` でキューを消費しないと 503(QUEUE_REJECT) が増える。
 - **WALバッチ調整（HDR, 10s/4c/1acct）**:
   - 200us/64: durable p99 **~6.2ms** / p999 **~9.1ms**
   - 1000us/256: durable p99 **~6.9ms** / p999 **~10.0ms**
   - 100us/64: durable p99 **~6.5ms** / p999 **~13.8ms**（RTT/throughputも悪化）
   - 傾向: wait/batchを締めると durable p999 が改善（p99は誤差範囲）
   - 採用値: **200us/64（デフォルト化）**
 - **動的 inflight（Soft Reject）: 実行手順**
   ```bash
   cd gateway-rust
   BACKPRESSURE_INFLIGHT_DYNAMIC=1 \
   BACKPRESSURE_INFLIGHT_TARGET_WAL_AGE_SEC=1.0 \
   BACKPRESSURE_INFLIGHT_ALPHA=0.8 \
   BACKPRESSURE_INFLIGHT_BETA=0.2 \
   BACKPRESSURE_INFLIGHT_MIN=256 \
   BACKPRESSURE_INFLIGHT_CAP=10000 \
   BACKPRESSURE_INFLIGHT_TICK_MS=250 \
   BACKPRESSURE_INFLIGHT_SLEW_RATIO=0.2 \
   BACKPRESSURE_INFLIGHT_HYSTERESIS_OFF_RATIO=0.9 \
   BACKPRESSURE_INFLIGHT_INITIAL=2000 \
   GATEWAY_PORT=8081 GATEWAY_TCP_PORT=0 \
   AUDIT_ASYNC_WAL=1 AUDIT_FDATASYNC=1 \
   JWT_HS256_SECRET=secret123 \
   cargo run --bin gateway-rust
   ```
   ```bash
   curl -s http://127.0.0.1:8081/metrics | rg "gateway_fast_path_processing_p(50|99|999)_us|gateway_inflight_dynamic_enabled|gateway_inflight_limit_dynamic|gateway_durable_commit_rate_ewma"
   ```
 - **Backpressure確認**:
   - Soft: `BACKPRESSURE_SOFT_WAL_AGE_MS_MAX=1000` → 429 (`BACKPRESSURE_SOFT_WAL_AGE`)
   - Hard: `BACKPRESSURE_INFLIGHT_MAX=5` + 5s/20c で 244件拒否（`gateway_backpressure_inflight_total=244`）
 - **注記**: ヒストグラムはHDRで精密化済み（µs単位の上限近似ではない）。

#### [Main.kt](app/src/main/kotlin/app/Main.kt)
- **役割**: アプリケーションエントリーポイント
- **処理**:
  1. 環境変数読み込み (`FAST_PATH_ENABLE`, `FAST_PATH_METRICS`, `KAFKA_BRIDGE_ENABLE`)
  2. Fast Path有効時: `FastPathEngine` + `PersistenceQueue` + `ChronicleQueueWriter` + `KafkaBridge` 起動
  3. Fast Path無効時: 旧 `Engine` 起動
  4. HTTPサーバー起動（外部公開はNginxの80/443、内部はApp:8080）
  5. シャットダウンフック登録

#### [Router.kt](app/src/main/kotlin/app/engine/Router.kt)
- **役割**: HTTPリクエストルーティング
- **処理**: `POST /events?key={key}` → `FastPathEngine.publish()` or `Engine.process()`

#### [FastPathEngine.kt](app/src/main/kotlin/app/fast/FastPathEngine.kt)
- **役割**: Fast Path実装 (LMAX Disruptor #1)
- **処理**:
  1. Disruptor (65536バッファ, YieldingWaitStrategy) 初期化
  2. `publish(key, payload)`: イベントをDisruptorに投入
  3. `TradeEventHandler`: イベント処理、`PersistenceQueue.enqueue()` 呼び出し
  4. HDR Histogramでレイテンシ測定 (process/publish)

#### [PersistenceQueue.kt](app/src/main/kotlin/app/fast/PersistenceQueue.kt)
- **役割**: Persistence Queue実装 (Disruptor #2)
- **処理**:
  1. Disruptor (32768バッファ, BlockingWaitStrategy) 初期化
  2. `enqueue(key, payload)`: Fast Pathからイベント受信
  3. `PersistenceHandler`: `ChronicleQueueWriter.write()` 呼び出し
  4. HDR Histogramでレイテンシ測定 (write)

#### [ChronicleQueueWriter.kt](app/src/main/kotlin/app/kafka/ChronicleQueueWriter.kt)
- **役割**: Chronicle Queue書き込み
- **処理**: Memory-mapped WALにイベント永続化、`KafkaBridge.send()` 呼び出し

#### [KafkaBridge.kt](app/src/main/kotlin/app/kafka/KafkaBridge.kt)
- **役割**: Kafka非同期転送
- **処理**: 100イベントバッチ、50msタイムアウトでKafkaへ送信

### HTTP API

#### [HttpIngress.kt](app/src/main/kotlin/app/http/HttpIngress.kt)
- **役割**: HTTPサーバー
- **エンドポイント**:
  - `POST /events?key={key}`: イベント受信、`Router.route()` 呼び出し
  - `GET /stats`: `StatsController.getStats()`
  - `GET /metrics`: `MetricsController.getMetrics()`
  - `GET /health`: `HealthController.getHealth()`

#### [StatsController.kt](app/src/main/kotlin/app/http/StatsController.kt)
- **役割**: 統計情報取得 (JSON)

#### [MetricsController.kt](app/src/main/kotlin/app/http/MetricsController.kt)
- **役割**: Prometheusメトリクス公開

#### [HealthController.kt](app/src/main/kotlin/app/http/HealthController.kt)
- **役割**: ヘルスチェック

### SRE運用ツール

#### [quick_canary.py](scripts/quick_canary.py)
- **役割**: クイックカナリーテスト
- **処理**:
  1. `/health` でヘルスチェック
  2. 指定イベント数をランダム送信 (`POST /events?key={key}`)
  3. `/stats` からメトリクス取得
  4. ベンチマーク結果JSON生成 (bench.v1.schema.json準拠)

#### [slo_gate.py](scripts/slo_gate.py)
- **役割**: SLO検証ゲート (CI統合用)
- **処理**:
  1. ベンチマーク結果JSONをJSON Schema検証
  2. 5項目SLOチェック (p99, p999, tail_ratio, drop_count, error_rate)
  3. GitHub Step Summary生成
  4. SLO違反時はexit 1

#### [backpressure_tuner.py](scripts/backpressure_tuner.py)
- **役割**: バックプレッシャー自動チューナー
- **処理**:
  1. ベンチマーク結果JSONからdrop_count, lag, p99を分析
  2. パラメータ推奨生成 (DISRUPTOR_BUFFER_SIZE, PERSISTENCE_QUEUE_BATCH_SIZE等)
  3. 推奨優先度付け (CRITICAL/WARNING/INFO)

#### [replay_minset.py](scripts/replay_minset.py)
- **役割**: 15分最小リプレイ抽出
- **処理**: Chronicle Queue WALからインシデント時刻±15分のイベント抽出、JSON出力

#### [generate_runbook_suggest.py](scripts/generate_runbook_suggest.py)
- **役割**: RAGベース初動トリアージ
- **処理**:
  1. docs/配下をベクトル化 (Simple RAG: キーワードマッチング)
  2. インシデント症状クエリから類似事例検索
  3. Runbook提案生成 (出典付き)

#### [cost_guard.py](scripts/cost_guard.py)
- **役割**: コストガード
- **処理**: ベンチマーク結果からAPI/ストレージコスト計算、予算超過時はexit 1

#### [dlp_guard.py](scripts/dlp_guard.py)
- **役割**: PIIガード
- **処理**: 正規表現でPII/機密情報検出 (email, credit_card, ssn, phone, api_key)

#### [change_risk.py](scripts/change_risk.py)
- **役割**: 変更リスクスコアリング
- **処理**:
  1. git diffから変更行数・ファイル数・Critical Path変更を分析
  2. 0-100点スコアリング
  3. Low/Medium/High/Criticalレベル判定
  4. カナリアリリース戦略推奨

---

## 技術スタック

| レイヤー | 技術 | バージョン | 理由 |
|---------|------|----------|------|
| 言語 | Kotlin | 2.0.20 | JVM上の現代的言語 |
| ランタイム | Java | 21 | Project Loomの恩恵 |
| **Fast Path** | LMAX Disruptor | 3.4.4 | 超低レイテンシRing Buffer |
| Zero-GC | Agrona | 1.21.1 | Off-heap操作 |
| **メトリクス** | HDR Histogram | 2.1.12 | 高精度パーセンタイル計測 |
| **永続化** | Chronicle Queue | 5.25ea5 | メモリマップドファイル（off-heap） |
| メッセージング | Kafka | 3.5.1 | 分散メッセージング |
| **HTTP** | Java HttpServer | 標準 | 標準ライブラリ（軽量） |
| JSON | Jackson | 2.18.1 | JSON処理 |
| **監視** | Prometheus + Grafana | 2.47.0 + 10.1.0 | オープンソース監視標準 |
| コンテナ | Docker + Kubernetes | - | コンテナ化 |
| **ビルド** | Gradle | 8.x | 柔軟なビルドツール |
| CI/CD | GitHub Actions | - | 自動デプロイ |

---

## 設計の本質 - なぜこのアーキテクチャなのか？

### 1. LMAX Disruptorの採用理由

**課題**: Javaの標準キュー（`ArrayBlockingQueue`）はロック競合でレイテンシが悪化する（1-5ms）

**解決**: Ring Bufferによるロックフリー設計
- プロデューサー・コンシューマー間でメモリを共有
- CAS（Compare-And-Swap）による原子的操作
- CPUキャッシュラインの最適化

**結果**: レイテンシ50-100倍改善（従来: 1-5ms → Fast Path: 30μs）

### 2. Chronicle Queueの採用理由

**課題**: 高速処理と耐障害性の両立（Fast Pathに永続化処理を入れるとレイテンシ悪化）

**解決**: 非同期永続化
- Fast Pathは別スレッドのDisruptor経由でChronicle Queueに委譲
- メモリマップドファイル（mmap）による高速I/O
- クラッシュ時もファイルから復旧可能

### 3. HDR Histogramの採用理由

**課題**: 平均値では異常値が埋もれる（HFTではp99が重要）

**解決**: パーセンタイル計測
- 全リクエストのレイテンシをヒストグラムに記録
- p50、p99、p999を正確に計測
- メモリ効率的（圧縮アルゴリズム）

### 4. Immutable Data Classの採用（Phase 2で決定）

**課題**: Mutable classによるZero-GC最適化は、オブジェクトプールのオーバーヘッドで逆に遅くなった

**解決**: Immutableに戻す
- Kotlinの `data class` は高度に最適化されている
- 若いGC（Young GC）はミリ秒以下
- シンプルさとメンテナンス性を優先

**結果**: Mutableで50-69x悪化 → Immutableに戻して性能回復

### 5. Docker Compose Profilesの採用

**課題**: 開発時に不要なPrometheus/Grafanaも起動してリソースを消費

**解決**: Profilesによる環境分離
```yaml
services:
  prometheus:
    profiles: ["monitoring"]  # 明示的に有効化が必要
  grafana:
    profiles: ["monitoring"]
```

**メリット**:
- リソース効率化
- 環境の明確な分離

### 6. Grafana Auto-Provisioningの採用

**課題**: 手動でデータソースやダッシュボードを設定するのは手間でエラーが起きやすい

**解決**: Infrastructure as Code
- データソース自動登録: `monitoring/grafana/provisioning/datasources/prometheus.yml`
- ダッシュボード自動読み込み: `monitoring/grafana/dashboards/fast-path.json`

**メリット**:
- 環境構築の自動化
- 設定の再現性
- チーム間での共有が容易

---

## トラブルシューティング

### Grafanaが起動しない / "No data" と表示される

#### 問題1: Grafanaコンテナが再起動を繰り返す

**症状**:
```bash
docker ps --filter "name=hft-grafana"
# STATUS: Restarting (1) 3 seconds ago
```

**原因**: データソース設定ファイルの構文エラー、またはデータベースの破損

**解決方法**:
```bash
# 1. Grafanaを完全リセット
docker-compose --profile monitoring down
docker volume rm event-switchyard_grafana-data

# 2. データソース設定を確認
cat monitoring/grafana/provisioning/datasources/prometheus.yml
# 以下のシンプルな形式であることを確認:
# apiVersion: 1
# datasources:
#   - name: Prometheus
#     type: prometheus
#     access: proxy
#     uid: prometheus
#     url: http://prometheus:9090
#     isDefault: true
#     editable: true

# 3. クリーンな状態で再起動
docker-compose --profile monitoring up -d

# 4. 起動確認 (15秒待機)
sleep 15 && docker ps --filter "name=hft-grafana"
# STATUS: Up になっていることを確認
```

#### 問題2: ダッシュボードに "No data" と表示される

**症状**: Grafanaにログインできるが、すべてのパネルに "No data" と表示される

**原因**:
1. Prometheusがメトリクスを取得できていない
2. ダッシュボードのメトリクス名が間違っている
3. アプリケーションが起動していない

**解決方法**:
```bash
# 1. アプリケーションが起動しているか確認
curl http://localhost/metrics | head -20
# fast_path_process_latency_p99_microseconds などが表示されることを確認

# 2. Prometheusがメトリクスを取得できているか確認
curl -s 'http://localhost:9090/api/v1/query?query=fast_path_process_latency_p99_microseconds' | python3 -m json.tool
# "result" に値が入っていることを確認

# 3. Prometheusのターゲット状態を確認
curl -s http://localhost:9090/api/v1/targets | python3 -m json.tool | grep -A5 health
# "health": "up" となっていることを確認

# 4. ベンチマークを実行してメトリクスを生成
make dev-bench
# Grafanaダッシュボードをリロード (Cmd+R / Ctrl+R)
```

#### 問題3: ログインできない (ユーザー名/パスワードがわからない)

**デフォルト認証情報**:
- ユーザー名: `admin`
- パスワード: `admin`

**リセット方法** (上記で失敗する場合):
```bash
# Grafanaを完全リセット
docker-compose --profile monitoring down
docker volume rm event-switchyard_grafana-data
docker-compose --profile monitoring up -d
sleep 15

# 再度 admin/admin でログイン
```

### アプリケーションが起動しない

#### 問題: ポート80/8080が既に使用中

**症状**:
```
java.net.BindException: Address already in use
```

**解決方法**:
```bash
# 既存プロセスをkill
lsof -ti:80 | xargs kill -9

# または
make stop

# 再起動
make run
```

### ベンチマークが失敗する

#### 問題: Connection refused エラー

**症状**:
```
ERROR: <urlopen error [Errno 61] Connection refused>
```

**解決方法**:
```bash
# 1. アプリケーションが起動しているか確認
curl http://localhost/health
# {"status": "healthy"} が返ることを確認

# 2. 起動していない場合は起動
make run

# 3. 30秒待機してから再実行
sleep 30
make dev-bench
```

### Docker関連

#### 問題: Docker daemon not running

**症状**:
```
Cannot connect to the Docker daemon
```

**解決方法**:
1. Docker Desktopを起動
2. 起動完了まで待機 (1-2分)
3. 再度実行

#### 問題: イメージコンフリクト

**症状**:
```
image already exists
```

**解決方法**:
```bash
docker-compose down --remove-orphans
docker-compose --profile monitoring up -d
```

### Fast Pathが遅い (p99 > 100μs)

1. **CPU Affinity設定を確認**
   ```bash
   # fast-path-engineスレッドを専用CPUコアに割り当て
   taskset -c 0 ./gradlew run
   ```

2. **JVM Warm-up実施**
   ```bash
   # 事前に数千イベント送信
   for i in {1..5000}; do
    curl -s -X POST "http://localhost:8080/events?key=BTC" \
       -H "Content-Type: application/json" \
       -d '{"price": 50000.00, "volume": 1.5}' > /dev/null
   done
   ```

3. **GCログ確認**
   ```bash
   # GC発生を確認
  curl http://localhost/health | jq '.jvm.gc'
   ```

### ドロップが発生

1. **Ring Bufferサイズ拡大**
   - FastPathEngine: `bufferSize = 131072` (デフォルト65536)
   - PersistenceQueue: `bufferSize = 65536` (デフォルト32768)

2. **バックプレッシャー調整**
   - `FAST_PATH_FALLBACK=1` でSlow Pathへフォールバック有効化

### Chronicle Queueエラー

1. **ディスク容量確認**
   ```bash
   df -h ./data/queue
   ```

2. **ファイルパーミッション確認**
   ```bash
   ls -la ./data/queue
   ```

3. **Chronicle Queueデータクリーンアップ**
   ```bash
   # 注意: 永続化データが削除されます
   rm -rf ./data/queue/*
   ```

### メトリクス名の確認方法

アプリケーションが公開している実際のメトリクス名を確認：

```bash
# すべてのfast_pathメトリクスを表示
curl -s http://localhost/metrics | grep "^fast_path"

# 主要メトリクス:
# fast_path_process_latency_p99_microseconds
# fast_path_process_latency_p50_microseconds
# fast_path_events_total
# fast_path_drops_total
# fast_path_lag
```

---

## パフォーマンスチューニング

### JVM設定

```bash
# ヒープサイズ最適化
export JAVA_OPTS="-Xms2g -Xmx2g -XX:+UseG1GC"

# GCログ有効化
export JAVA_OPTS="$JAVA_OPTS -Xlog:gc*:file=gc.log"

# Large Pagesサポート (Linux)
export JAVA_OPTS="$JAVA_OPTS -XX:+UseLargePages"
```

### OS設定 (Linux)

```bash
# TCP設定最適化
sudo sysctl -w net.core.somaxconn=65535
sudo sysctl -w net.ipv4.tcp_max_syn_backlog=65535

# ファイルディスクリプタ上限
ulimit -n 65536
```

---

## テスト

### 単体テスト

```bash
./gradlew test
```

### 負荷テスト

```bash
# 100イベント送信
for i in {1..100}; do
  curl -s -X POST "http://localhost:8080/events?key=BTC" \
    -H "Content-Type: application/json" \
    -d '{"price": 50000.00, "volume": 1.5}' > /dev/null
done

# メトリクス確認
curl http://localhost/stats | jq '.fast_path_process_p99_us'
```

### 長時間安定性テスト

```bash
# 1分間、50 req/s
TEST_DURATION_MINUTES=1 REQUEST_RATE_PER_SEC=50 ./scripts/tools/stability_test.sh

# 60分間、100 req/s (デフォルト)
./scripts/tools/stability_test.sh

# カスタム設定: 120分間、200 req/s
TEST_DURATION_MINUTES=120 REQUEST_RATE_PER_SEC=200 ./scripts/tools/stability_test.sh
```

---

## 開発

### ビルド成果物

```bash
# 実行可能JAR生成
./gradlew shadowJar

# 生成物
ls -lh app/build/libs/app-all.jar

# 実行
java -jar app/build/libs/app-all.jar
```

---

## CI/CD

GitHub Actionsによる自動デプロイ ([.github/workflows/deploy.yml](.github/workflows/deploy.yml)):

1. **ビルド & テスト** - 全プッシュで実行
2. **SLO回帰ゲート** - カナリーテスト + SLO検証 (PR時)
3. **Dockerイメージビルド & プッシュ** - ghcr.io
4. **ステージング自動デプロイ** - `refactor_consumer_app`ブランチ
5. **本番自動デプロイ + スモークテスト** - `main`ブランチ

---

## 本番デプロイ

### Kubernetes（推奨）

```yaml
# k8s/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: hft-fast-path
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: app
        image: event-switchyard-app:latest
        env:
        - name: FAST_PATH_ENABLE
          value: "1"
        - name: FAST_PATH_METRICS
          value: "1"
        resources:
          requests:
            memory: "4Gi"
            cpu: "2000m"
          limits:
            memory: "4Gi"
            cpu: "2000m"
```

### Docker（シンプル）

```bash
# イメージビルド
docker build -t event-switchyard-app .

# 実行
docker run -d \
  -e FAST_PATH_ENABLE=1 \
  -e FAST_PATH_METRICS=1 \
  -p 80:80 \
  --name hft-app \
  event-switchyard-app
```

---

## ドキュメント

### ユーザーガイド
- **[DEPLOYMENT.md](DEPLOYMENT.md)** - デプロイメントガイド (500+行)

### SRE運用ガイド
- **[docs/specs/slo.md](docs/specs/slo.md)** - SLO仕様 (目標値・閾値・アラート)
- **[docs/runbook.md](docs/runbook.md)** - インシデント対応手順
- **[docs/change_risk.md](docs/change_risk.md)** - 変更リスク評価ガイド

### Gateway Rust: Kafka/Outbox運用

**目的**: 監査ログ(JSONL)をOutboxで追跡し、Kafkaへ耐久配信する。

**有効化条件**:
- `BUS_MODE=outbox` かつ `KAFKA_ENABLE=1` のとき Outbox が起動

**主要設定**:
- `GATEWAY_AUDIT_PATH` (default: `var/gateway/audit.log`)
- `OUTBOX_OFFSET_PATH` (default: `var/gateway/outbox.offset`)
- `OUTBOX_BACKOFF_BASE_MS` / `OUTBOX_BACKOFF_MAX_MS`
- `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `KAFKA_CLIENT_ID`

**配信対象**:
- `OrderAccepted`, `CancelRequested` のみKafkaへ送信
- それ以外の監査イベントはスキップ (outbox_events_skipped)

**監視指標 (/metrics)**:
- `gateway_outbox_*` (read/publish/errors/backoff)
- `gateway_kafka_*` (delivery ok/err/dropped)
- `gateway_outbox_offset_resets_total` が増えたら要注意

**停止条件の目安**:
- `gateway_outbox_publish_errors_total` が継続増加
- `gateway_kafka_delivery_err_total` が増加し続ける
- `gateway_outbox_backoff_current_ms` が `OUTBOX_BACKOFF_MAX_MS` に張り付き
- `gateway_outbox_offset_resets_total` が急増

**手動復旧手順**:
1. Kafka到達性を確認 (`KAFKA_BOOTSTRAP_SERVERS`)
2. `OUTBOX_OFFSET_PATH` を確認 (破損時は `.bad.<ts>` に退避される)
3. 必要なら `OUTBOX_OFFSET_PATH` を 0 にしてフル再送
4. 再送を避ける場合は監査ログのサイズ(バイト)に合わせてオフセットを設定
5. 再起動して outbox が進むことを確認

**注意**:
- Outboxは at-least-once。再送時は下流で冪等化が必要。

### ADR (Architecture Decision Records)
- **[docs/adr/001-slo-regression-gate.md](docs/adr/001-slo-regression-gate.md)** - SLO回帰ゲート導入
- **[docs/adr/002-backpressure-auto-tuner.md](docs/adr/002-backpressure-auto-tuner.md)** - バックプレッシャー自動チューナー
- **[docs/adr/003-minimal-replay-strategy.md](docs/adr/003-minimal-replay-strategy.md)** - 15分最小リプレイ戦略
- **[docs/adr/004-rag-based-triage.md](docs/adr/004-rag-based-triage.md)** - RAGベース初動トリアージ
- **[docs/adr/005-cost-pii-guards.md](docs/adr/005-cost-pii-guards.md)** - コスト・PII ガード導入
- **[docs/adr/006-change-risk-scoring.md](docs/adr/006-change-risk-scoring.md)** - 変更リスクスコアリング

### 実装ドキュメント
- **[docs/GRAFANA_IMPLEMENTATION_LEARNINGS.md](docs/GRAFANA_IMPLEMENTATION_LEARNINGS.md)** - Grafana実装の学習ログ
- **[PROJECT_STATUS.md](PROJECT_STATUS.md)** - プロジェクト全体の進捗・完成度
- **[PHASE4_RESULTS.md](PHASE4_RESULTS.md)** - Phase 4: レイテンシ最適化
- **[PHASE5_PROMETHEUS_GRAFANA.md](PHASE5_PROMETHEUS_GRAFANA.md)** - Phase 5: Prometheus/Grafana連携
- **[PHASE6_CONTAINERIZATION.md](PHASE6_CONTAINERIZATION.md)** - Phase 6: コンテナ化・デプロイ自動化
- **[PHASE7_PRODUCTION_READY.md](PHASE7_PRODUCTION_READY.md)** - Phase 7: 本番環境対応
- **[PHASE8_VERIFICATION.md](PHASE8_VERIFICATION.md)** - Phase 8: 検証結果
- **[IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md)** - 全Phase完全サマリー
- **[HFT_FUNDAMENTALS.md](HFT_FUNDAMENTALS.md)** - HFT基礎知識
- **[HFT_TECH_EXPLAINED.md](HFT_TECH_EXPLAINED.md)** - HFT技術詳細

---

## コントリビューション

**開発フロー**:
1. ブランチ作成: `git checkout -b feature/xxx`
2. 実装・テスト: `make test`
3. ベンチマーク: `make dev-bench`
4. 性能ゲート: `make gate`（p99 < 100μs必須）
5. PR作成

**CI/CDパイプライン**:
- GitHub Actions: `.github/workflows/`
- ビルド→テスト→ベンチマーク→性能ゲート自動実行

---

## ライセンス

MIT

---
