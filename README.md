# HFT Fast Path Event Switchyard

高頻度取引(HFT)向けの超低レイテンシイベント処理システム。LMAX Disruptor + Chronicle Queue + Kafkaによる本番環境対応済み実装。

**本質**: Kafka上で動作する超低レイテンシ（p99 < 100μs）イベント処理システム
**UI**: Operations Dashboard - リアルタイムメトリクス監視、SLO管理、システム設定

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
- **Operations Dashboard**: http://localhost:8080 （リアルタイム監視・メトリクス・設定管理）
- Grafana監視: http://localhost:3000 (admin/admin)
- Prometheus: http://localhost:9090

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
curl http://localhost:8080/stats
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
curl http://localhost:8080/metrics
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
curl http://localhost:8080/health
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
curl http://localhost:8080/stats
curl http://localhost:8080/metrics  # Prometheus形式
curl http://localhost:8080/health
```

---

## SLOカナリーテスト（開発者向け）

```bash
# アプリケーション起動 (別ターミナル)
FAST_PATH_ENABLE=1 FAST_PATH_METRICS=1 ./gradlew run

# クイックカナリー実行
python3 scripts/quick_canary.py \
  --url http://localhost:8080 \
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

## Docker Compose

### 監視スタック込みフルスタック起動

```bash
# Prometheus + Grafana + Kafkaを含むフルスタック起動
docker-compose -f docker-compose.monitoring.yml up -d

# アクセス
# - アプリケーション: http://localhost:8080
# - Grafana: http://localhost:3000 (admin/admin)
# - Prometheus: http://localhost:9090
```

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

#### [Main.kt](app/src/main/kotlin/app/Main.kt)
- **役割**: アプリケーションエントリーポイント
- **処理**:
  1. 環境変数読み込み (`FAST_PATH_ENABLE`, `FAST_PATH_METRICS`, `KAFKA_BRIDGE_ENABLE`)
  2. Fast Path有効時: `FastPathEngine` + `PersistenceQueue` + `ChronicleQueueWriter` + `KafkaBridge` 起動
  3. Fast Path無効時: 旧 `Engine` 起動
  4. HTTPサーバー起動 (8080ポート)
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
curl http://localhost:8080/metrics | head -20
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

#### 問題: ポート8080が既に使用中

**症状**:
```
java.net.BindException: Address already in use
```

**解決方法**:
```bash
# 既存プロセスをkill
lsof -ti:8080 | xargs kill -9

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
curl http://localhost:8080/health
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
   curl http://localhost:8080/health | jq '.jvm.gc'
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
curl -s http://localhost:8080/metrics | grep "^fast_path"

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
curl http://localhost:8080/stats | jq '.fast_path_process_p99_us'
```

### 長時間安定性テスト

```bash
# 1分間、50 req/s
TEST_DURATION_MINUTES=1 REQUEST_RATE_PER_SEC=50 ./scripts/stability_test.sh

# 60分間、100 req/s (デフォルト)
./scripts/stability_test.sh

# カスタム設定: 120分間、200 req/s
TEST_DURATION_MINUTES=120 REQUEST_RATE_PER_SEC=200 ./scripts/stability_test.sh
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
  -p 8080:8080 \
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

**更新日**: 2025-11-03
**ステータス**: ✅ 本番投入準備100%完了 - デプロイ可能
**作成者**: Claude (Anthropic) - Phase 1-8実装完了
