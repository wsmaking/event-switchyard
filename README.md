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
