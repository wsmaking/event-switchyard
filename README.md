# HFT Fast Path Event Switchyard

## 現在の主軸
- 実運用/検証の主軸は Rust 実装（`gateway-rust`）。
- 注文経路は 2 系統。
  - `/orders`: durable 前提の既存経路（idempotency + audit/WAL + bus）。
  - `/v3/orders`: PureHFT Phase1 の volatile 受理経路（低遅延優先）。
- 設計・契約・計測ログは `docs/ops/` に集約。

## リポジトリの見取り図
- Rust Gateway 本体: `gateway-rust/`
- 共通コア: `gateway-core/`
- 運用/計測スクリプト: `scripts/ops/`
- 設計/契約ドキュメント: `docs/ops/`, `docs/arch/`
- 旧 Kotlin/Java モジュール（比較・互換用）: `app/`, `gateway/`, `backoffice/`

## クイックスタート（Rust）

### 1. ビルド
```bash
cd gateway-rust
cargo build --release
cd ..
```

### 2. 起動（ローカル検証向け）
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

### `/orders`（durable 経路）
- `POST /orders`
- `GET /orders/{order_id}`
- `GET /orders/client/{client_order_id}`
- `POST /orders/{order_id}/cancel`

### `/v3/orders`（PureHFT volatile 経路）
- `POST /v3/orders`
- 応答は `VOLATILE_ACCEPT` / `SOFT_REJECT` / `KILLED` を返す。
- 目標は「深部保護しながら live ack を短く保つ」こと。

`/v3/orders` の典型リクエスト:
```bash
curl -sS -X POST http://127.0.0.1:29001/v3/orders \
  -H 'Authorization: Bearer <JWT>' \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-1' \
  -d '{"symbol":"AAPL","side":"BUY","type":"LIMIT","qty":100,"price":15000,"timeInForce":"GTC","clientOrderId":"demo-1"}'
```

## `/v3/orders` 制御ロジック（実装済み）
- queue 使用率 `>= 85%`: `429`（soft reject）
- queue 使用率 `>= 95%` または kill 中: `503`（killed）
- `try_enqueue` が `Full/Closed`: `503` + kill 昇格
- kill auto recover はヒステリシスで復帰
  - `V3_KILL_AUTO_RECOVER=true`
  - `queue_pct <= V3_KILL_RECOVER_PCT`
  - kill 継続時間 `>= V3_KILL_RECOVER_AFTER_MS`

詳細図:
- `docs/ops/pure_hft_current_design_visualization.md`

## 主要メトリクス

### 共通
- `gateway_ack_p50_us`
- `gateway_ack_p99_us`

### `/v3` カウンタ・状態
- `gateway_v3_accepted_total`
- `gateway_v3_rejected_soft_total`
- `gateway_v3_rejected_killed_total`
- `gateway_v3_queue_depth`
- `gateway_v3_queue_capacity`
- `gateway_v3_queue_utilization_pct`
- `gateway_v3_kill_switch`
- `gateway_v3_kill_recovered_total`

### `/v3` ステージ分解
- `gateway_v3_stage_parse_p99_us`
- `gateway_v3_stage_risk_p99_us`
- `gateway_v3_stage_risk_position_p99_us`
- `gateway_v3_stage_risk_margin_p99_us`
- `gateway_v3_stage_risk_limits_p99_us`
- `gateway_v3_stage_enqueue_p99_us`
- `gateway_v3_stage_serialize_p99_us`

## よく使う環境変数（Rust）
- `GATEWAY_PORT`（default: `8081`）
- `GATEWAY_TCP_PORT`（default: `9001`）
- `QUEUE_CAPACITY`（default: `65536`）
- `JWT_HS256_SECRET`
- `FASTPATH_DRAIN_ENABLE`
- `FASTPATH_DRAIN_WORKERS`
- `V3_INGRESS_QUEUE_CAPACITY`（default: `65536`）
- `V3_KILL_AUTO_RECOVER`（default: `true`）
- `V3_KILL_RECOVER_PCT`（default: `60`, max `94`）
- `V3_KILL_RECOVER_AFTER_MS`（default: `3000`）
- `V3_RISK_PROFILE`（`light|medium|heavy`）
- `V3_RISK_LOOPS`（未指定時は profile 既定値）
- `V3_RISK_MARGIN_MODE`（`legacy|incremental`）

## ベンチ/比較コマンド（現行）

### `/v3/orders` 単体負荷
```bash
HOST=127.0.0.1 \
PORT=29001 \
DURATION=15 \
CONNECTIONS=300 \
THREADS=8 \
LATENCY=1 \
WRK_PATH=/v3/orders \
JWT_HS256_SECRET=secret123 \
scripts/ops/wrk_gateway_rust.sh
```

### Phase2 比較（baseline vs injected）
```bash
RUNS=5 WARMUP_RUNS=1 scripts/ops/run_pure_hft_phase2_compare.sh
```

出力:
- `var/results/pure_hft_phase2_compare_*.tsv`
- `var/results/pure_hft_phase2_compare_*.summary.txt`
- `var/results/pure_hft_phase2_compare_*.noise.txt`

補足:
- `WARMUP_RUNS` は集計対象から除外（中央値は測定runのみで算出）。
- `gateway_live_ack_accepted_p99_us`（accepted-only）を同時収集。

### Pure absolute gate（Linux専用, 1回）
```bash
make pure-hft-absolute-gate
```

### Pure absolute gate 定常実行（Linux専用, 複数回）
```bash
RUNS=6 INTERVAL_SEC=600 MIN_PASS_RATIO=1.0 make pure-hft-absolute-gate-loop
```

出力:
- `var/results/pure_hft_gate_*.summary.txt`（各run）
- `var/results/pure_hft_gate_loop_*.tsv`
- `var/results/pure_hft_gate_loop_*.summary.txt`
- `var/results/pure_hft_gate_*.noise.txt`

## Make ターゲット（要点）
- `make rust-check`
- `make rust-test`
- `make rust-run`
- `make perf-gate-rust`
- `make perf-gate-rust-full`
- `make perf-gate-rust-ci`
- `make perf-gate-rust-nightly`
- `make perf-gate-rust-bless`
- `make preset-switch-gate`
- `make pure-hft-absolute-gate`
- `make pure-hft-absolute-gate-loop`
- `make fdatasync-ab`
- `make fdatasync-adaptive-sweep`
- `make adaptive-profile-search`

## ドキュメント導線

### PureHFT
- 契約ドラフト: `docs/ops/pure_hft_contract_draft.md`
- 現行実装可視化: `docs/ops/pure_hft_current_design_visualization.md`
- 進捗ログ: `docs/ops/pure_hft_phase_progress.md`

### Lossless / Durability
- 契約ドラフト: `docs/ops/lossless_contract_draft.md`
- 可視化: `docs/ops/lossless_contract_visualization.md`
- 10k–15k計画: `docs/ops/lossless_10k_15k_plan.md`
- 改善サマリ: `docs/ops/perf_improvement_summary.md`

### 全体学習計画
- `docs/arch/LEARNING_GUIDE.md`

## Legacy（Kotlin/Java）について
- `app/`, `gateway/`, `backoffice/` は残しているが、現行の性能検証主軸は Rust。
- 旧スタックのコマンド（`make run`, `make run-gateway`, `make run-backoffice` など）は互換/比較用途。
