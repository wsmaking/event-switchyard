# Event Switchyard

このリポジトリの正本 README は本ファイルのみ。

## 現在のシステム

主軸は Rust 実装の [gateway-rust](gateway-rust/) です。いまの system は、単純な低遅延注文 gateway だけではなく、次の 3 層を持っています。

- `v3 hot path`
  - `POST /v3/orders`
  - latency-first の volatile accept 経路
- `classic order path`
  - `POST /orders`
  - 照会、cancel、replace、amend を持つ durable 寄り経路
- `strategy / quant layer`
  - `StrategyIntent` の `adapt / submit / shadow`
  - execution algo runtime
  - replay / catch-up
  - alpha re-decision

要するに、現在の gateway は

- 高速な v3 受注
- strategy intent の dry-run / actual submit
- `TWAP / VWAP / POV` の parent-child runtime
- restart 後の replay / catch-up
- terminal UI での運用監視

までを扱います。

## いま重要な境界

### execution algo

`TWAP / VWAP / POV` は `adapt` で plan を返し、`submit` で parent runtime を作って child order を自動発注します。

- parent runtime は Gateway が保持
- child feedback は parent に集約
- restart 後は `GatewayManagedResume` で scheduled child を再開可能

### short-lived alpha

短命な alpha 系 intent は `NoAutoResume` 前提です。

- Gateway は restart 後に古い intent を勝手に再送しない
- `StrategyExecutionFact` を replay / catch-up で返す
- 上流 execution engine が `RecoveryContext + current market` で再判断する

この設計の中心は、

- `注文事実` は Gateway が持つ
- `次に何を打つか` は execution 側が決める

です。

## 主な HTTP endpoint

### core

- `GET /health`
- `GET /metrics`

### v3

- `POST /v3/orders`
- `GET /v3/orders/{session_id}/{session_seq}`

### classic orders

- `POST /orders`
- `GET /orders/{order_id}`
- `GET /orders/client/{client_order_id}`
- `POST /orders/{order_id}/cancel`
- `POST /orders/{order_id}/replace`
- `POST /orders/{order_id}/amend`

### strategy

- `GET /strategy/config`
- `PUT /strategy/config`
- `POST /strategy/intent/adapt`
- `POST /strategy/intent/submit`
- `POST /strategy/intent/shadow`
- `POST /strategy/shadow`
- `GET /strategy/shadow/{shadow_run_id}/summary`
- `GET /strategy/shadow/{shadow_run_id}/{intent_id}`
- `GET /strategy/runtime/{parent_intent_id}`
- `GET /strategy/replay/execution/{execution_run_id}`
- `GET /strategy/catchup/execution/{execution_run_id}`
- `GET /strategy/replay/intent/{intent_id}`
- `GET /strategy/catchup/intent/{intent_id}`

## strategy runtime と recovery の見方

### adapt / submit

- `adapt`
  - dry-run
  - effective policy や `algoPlan` を返す
- `submit`
  - actual submit
  - v3 または algo runtime に接続される

### replay / catch-up

`StrategyExecutionFact` の status は次の 5 つです。

- `REJECTED`
- `UNCONFIRMED`
- `DURABLE_ACCEPTED`
- `DURABLE_REJECTED`
- `LOSS_SUSPECT`

`catch-up` は raw fact だけでなく latest state を返します。execution 側はこれを `RecoveryContext` に集約し、`filled / open / failed / unknown / unsent` を分けて再判断します。

### alpha re-decision

alpha 側では

- `UNCONFIRMED / LOSS_SUSPECT` があれば hold
- stale な market basis なら abort
- `filled + open` を引いた residual にだけ fresh intent を作る

が基本です。

## operator surface

現状の運用面は 4 つです。

### 1. runtime の進行を見る

`/strategy/runtime/{parent_intent_id}`

- algo parent status
- child の `SCHEDULED / DISPATCHING / VOLATILE_ACCEPTED / DURABLE_ACCEPTED / REJECTED / SKIPPED`

### 2. raw fact を見る

`/strategy/replay/*`

- 監査
- デバッグ
- fact の時系列確認

### 3. recovery / re-decision を見る

`/strategy/catchup/*` と [strategy_catchup_reader.rs](gateway-rust/src/bin/strategy_catchup_reader.rs)

- latest decision state
- recovery context
- re-decision input / result

### 4. 常時監視する

[strategy_ops_tui.rs](gateway-rust/src/bin/strategy_ops_tui.rs)

- catch-up latest state
- `AlphaRecoveryContext`
- `AlphaReDecision`
- algo runtime child progress
- selected metrics

を 1 画面で見ます。

### 5. 常駐 re-decision を回す

[strategy_redecision_orchestrator.rs](gateway-rust/src/bin/strategy_redecision_orchestrator.rs)

- catch-up cursor を永続化
- execution 側の market input ingress から現在の signal を読む
- `AlphaReDecision` を評価
- 必要なら `adapt -> submit`

### 6. market input ingress

[strategy_market_input_server.rs](gateway-rust/src/bin/strategy_market_input_server.rs)

- execution / intent scope ごとの最新 market input を保持
- orchestrator が HTTP で取得
- alpha producer は `PUT /alpha-input/...` で更新

## リポジトリ見取り図

- [gateway-rust](gateway-rust/)
  - 主実装
- [gateway-core](gateway-core/)
  - 共通コア
- [scripts/ops](scripts/ops/)
  - gate, load, quant, monitoring script
- [docs/ops](docs/ops/)
  - 契約、設計、運用正本
- [mini-exchange](mini-exchange/)
  - quant evaluation 用市場モデル
- [app](app/), [gateway](gateway/), [backoffice](backoffice/)
  - 旧比較資産

## quick start

### 1. build

```bash
cargo build --manifest-path gateway-rust/Cargo.toml --release
```

Kafka bus を有効化する場合だけ:

```bash
cargo build --manifest-path gateway-rust/Cargo.toml --release --features kafka-bus
```

### 2. start

```bash
GATEWAY_PORT=8081 \
GATEWAY_TCP_PORT=0 \
JWT_HS256_SECRET=secret123 \
KAFKA_ENABLE=0 \
FASTPATH_DRAIN_ENABLE=1 \
FASTPATH_DRAIN_WORKERS=4 \
./gateway-rust/target/release/gateway-rust
```

### 3. health / metrics

```bash
curl -sS http://127.0.0.1:8081/health
curl -sS http://127.0.0.1:8081/metrics | head
```

## Java Replay Stack

UI から注文し、OMS / BackOffice の最終 out まで追う Java replay 環境は別導線で起動します。

### 1. backend start

```bash
scripts/ops/run_java_replay_stack.sh
```

起動後の endpoint:

- `app-java`: `http://localhost:8080`
- `oms-java`: `http://localhost:18081`
- `backoffice-java`: `http://localhost:18082`

状態は `var/java-replay/` に保存され、再起動後も replay scenario の結果を引き継ぎます。

### 2. frontend start

```bash
cd frontend
npm run dev
```

### 3. smoke

```bash
scripts/ops/smoke_java_replay_stack.sh
```

### 4. stop

```bash
scripts/ops/stop_java_replay_stack.sh
```

## strategy quick start

### 1. adapt

```bash
curl -sS http://127.0.0.1:8081/strategy/intent/adapt \
  -H 'content-type: application/json' \
  --data-binary @/tmp/strategy_intent.json
```

### 2. submit

```bash
curl -sS http://127.0.0.1:8081/strategy/intent/submit \
  -H 'content-type: application/json' \
  --data-binary @/tmp/strategy_submit.json
```

### 3. catch-up reader

```bash
cargo run --manifest-path gateway-rust/Cargo.toml --bin strategy_catchup_reader -- \
  --base-url http://127.0.0.1:8081 \
  --execution-run-id run-1 \
  --template-intent contracts/fixtures/strategy_intent_v2.json \
  --market-desired-signed-qty 60 \
  --market-max-decision-age-ns 1000000
```

### 4. TUI

```bash
cargo run --manifest-path gateway-rust/Cargo.toml --bin strategy_ops_tui -- \
  --base-url http://127.0.0.1:8081 \
  --execution-run-id run-1 \
  --parent-intent-id parent-1 \
  --template-intent contracts/fixtures/strategy_intent_v2.json \
  --market-desired-signed-qty 60 \
  --market-max-decision-age-ns 1000000
```

### 5. re-decision orchestrator

まず market input ingress を起動して current signal を投入します。

```bash
cargo run --manifest-path gateway-rust/Cargo.toml --bin strategy_market_input_server -- \
  --listen 127.0.0.1:18082
```

```bash
curl -sS -X PUT http://127.0.0.1:18082/alpha-input/execution/run-export-1 \
  -H 'content-type: application/json' \
  --data-binary @contracts/fixtures/strategy_redecision_market_input_v1.json
```

その上で orchestrator を回します。

```bash
cargo run --manifest-path gateway-rust/Cargo.toml --bin strategy_redecision_orchestrator -- \
  --config contracts/fixtures/strategy_redecision_orchestrator_v1.json \
  --once
```

## 主な binary

- `gateway-rust`
  - main gateway
- `export_strategy_intent`
  - strategy intent fixture/export helper
- `strategy_catchup_reader`
  - cursor-tracking catch-up / re-decision reader
- `strategy_ops_tui`
  - terminal operator monitor
- `strategy_redecision_orchestrator`
  - persistent catch-up / market input / adapt-submit orchestrator
- `strategy_market_input_server`
  - execution-side market decision ingress

## 現役 script

### v3 / perf

- `scripts/ops/build_gateway_rust_release.sh`
- `scripts/ops/check_v3_local_strict_gate.sh`
- `scripts/ops/check_v3_stable_gate.sh`
- `scripts/ops/check_v3_absolute_gate.sh`
- `scripts/ops/check_v3_capability_gate.sh`
- `scripts/ops/check_v3_durable_tail_gate.sh`
- `scripts/ops/check_v3_crash_replay_gate.sh`
- `scripts/ops/check_v3_long_soak_gate.sh`
- `scripts/ops/check_v3_replica_gate.sh`
- `scripts/ops/run_v3_absolute_gate_loop.sh`
- `scripts/ops/run_v3_open_loop_probe.sh`
- `scripts/ops/run_v3_phase2_compare.sh`
- `scripts/ops/run_v3_phase2_compare_when_quiet.sh`
- `scripts/ops/run_v3_capacity_sweep.sh`
- `scripts/ops/open_loop_v3_load.py`
- `scripts/ops/open_loop_v3_tcp_load.py`
- `scripts/ops/perf_noise_guard.sh`
- `scripts/ops/wrk_gateway_rust.sh`

### quant / strategy

- `scripts/ops/check_quant_eval_gate.sh`
- `scripts/ops/check_quant_eval_ci_gate.sh`
- `scripts/ops/check_quant_policy_runtime_gate.sh`
- `scripts/ops/check_quant_policy_durable_gate.sh`
- `scripts/ops/export_quant_strategy_intent_fixture.sh`
- `scripts/ops/export_quant_strategy_intent_snapshot.sh`
- `scripts/ops/export_quant_strategy_intent_batch.sh`
- `scripts/ops/run_quant_gateway_capture_batch.sh`
- `scripts/ops/run_collect_quant_feedback_shadow_from_gateway.sh`
- `scripts/ops/collect_quant_feedback_shadow_from_gateway.py`
- `scripts/ops/run_join_quant_feedback_with_mini_exchange.sh`
- `scripts/ops/run_join_quant_feedback_with_mini_exchange_batch.sh`
- `scripts/ops/join_quant_feedback_with_mini_exchange.py`
- `scripts/ops/join_quant_feedback_with_mini_exchange_batch.py`
- `scripts/ops/run_mini_exchange_quant_bridge.sh`
- `scripts/ops/run_mini_exchange_quant_batch.sh`

### AI assist

- `scripts/ops/run_ai_perf_probe.py`
- `scripts/ops/ai_incident_agent.py`
- `scripts/ops/ai_triage_notify.py`
- `scripts/ops/ai_rag_index.py`
- `scripts/ops/ai_rag_query.py`

## monitoring

Prometheus / Grafana は `docker-compose.yml` の monitoring profile で起動します。

```bash
docker compose -f docker-compose.yml --profile monitoring up -d prometheus grafana
curl -sS http://127.0.0.1:9090/-/ready
curl -sS http://127.0.0.1:3000/api/health
```

よく見るもの:

- `gateway_v3_hotpath_accepted_p99_ns`
- `gateway_v3_durable_confirm_p99_us`
- `gateway_v3_loss_suspect_total`
- `gateway_strategy_runtime_parent_count`
- `gateway_quant_feedback_queue_depth`

## AI triage

gate 失敗時の補助分析は以下で手動実行できます。

```bash
python3 scripts/ops/ai_incident_agent.py --run-name <RUN_NAME> --results-dir var/results
python3 scripts/ops/ai_triage_notify.py --triage-json var/results/<RUN_NAME>.triage.json
```

## まず読む文書

- [docs/ops/INDEX.md](docs/ops/INDEX.md)
- [docs/ops/catchup_fasttrack.md](docs/ops/catchup_fasttrack.md)
- [docs/ops/contract_draft.md](docs/ops/contract_draft.md)
- [docs/ops/current_design_visualization.md](docs/ops/current_design_visualization.md)
- [docs/ops/quant_runtime_status_20260320.md](docs/ops/quant_runtime_status_20260320.md)
- [docs/ops/quant_dynamic_verification_and_catchup.md](docs/ops/quant_dynamic_verification_and_catchup.md)
- [docs/ops/quant_alpha_redecision_replay_catchup_design_20260320.md](docs/ops/quant_alpha_redecision_replay_catchup_design_20260320.md)
- [docs/ops/1hour_securities_business_replay_scenarios_amendments.md](docs/ops/1hour_securities_business_replay_scenarios_amendments.md)

## 運用上の注意

- `ack p99` 単独で品質判定しない
- `accepted_rate` と reject rate を必ず併記する
- alpha re-decision では `unknown exposure` を過小評価しない
- `SubmitFreshIntent` は `機械的なやり直し` ではなく `current market を入れた再判断`
- benchmark / gate 結果は `var/results/` と正本 docs に紐付けて残す

## 公開方針

このリポジトリは、技術実績の共有および説明のために一時的に公開しています。

## ライセンス

ソースコードの使用、改変、再配布、商用利用は許可していません。詳細は [LICENSE](LICENSE) を参照してください。

## README policy

- 管理対象 README は `README.md` を正本とする
- サブディレクトリ README は廃止し、必要情報は本ファイルか `docs/ops/` 正本へ集約する
- 旧資料は `docs/old/` と `docs/ops/old/` に退避済み
