# Quant Dynamic Verification and Catchup Guide

## 目的

この文書は、quant / strategy runtime を読み始めるときに、`いきなり全体を眺める` のではなく、`test -> endpoint -> debugger` の順で理解するための手順。

この順にする理由は次。

- test で pure logic と契約を切り分けられる
- endpoint で実際の JSON と route を確認できる
- debugger は最後に必要箇所だけ使えばよい

## 先に理解しておくこと

- `adapt` は dry-run
- `submit` は actual v3 submit
- `TWAP / VWAP / POV` は現時点では `adapt` で plan を返すだけ
- `submit` はまだ `STRATEGY_POLICY_RUNTIME_NOT_IMPLEMENTED` で reject
- `HIGH / CRITICAL` urgency は soft admission のみ bypass

## 推奨順

1. targeted test で planner を確認
2. targeted test で handler 契約を確認
3. `adapt` を手で叩いて `algoPlan` を確認
4. `submit` を手で叩いて未実装境界を確認
5. 必要なら breakpoint で中に入る

## Step 0: branch と作業状態の確認

```bash
git branch --show-current
git status --short
```

想定。

- algo planning を読むときは `codex/quant-algo-runtime`
- durable gate を読むときは `codex/quant-policy-durable-gate`
- gate 運用を見るだけなら `main`

## Step 1: planner の test

まずは pure logic。

```bash
cargo test --manifest-path gateway-rust/Cargo.toml twap_plan_splits_evenly -- --nocapture
cargo test --manifest-path gateway-rust/Cargo.toml vwap_plan_uses_curve_weights -- --nocapture
cargo test --manifest-path gateway-rust/Cargo.toml pov_plan_requires_capacity -- --nocapture
```

ここで見ること。

- `TWAP` は equal split か
- `VWAP` は curve weight を使っているか
- `POV` は capacity 不足で reject になるか

読むファイル。

- `gateway-rust/src/strategy/algo.rs`

## Step 2: handler 契約の test

次に route 境界。

```bash
cargo test --manifest-path gateway-rust/Cargo.toml strategy_intent_adapter_returns_twap_algo_plan -- --nocapture
cargo test --manifest-path gateway-rust/Cargo.toml strategy_intent_submit_rejects_unimplemented_algo_policy -- --nocapture
```

ここで見ること。

- `adapt` は `algoPlan` を返すか
- `submit` はまだ reject か
- reject reason が `STRATEGY_POLICY_RUNTIME_NOT_IMPLEMENTED` か

読むファイル。

- `gateway-rust/src/server/http/strategy.rs`
- `gateway-rust/src/server/http/orders.rs`

## Step 3: runtime safety gate

algo ではないが、strategy が runtime に効く既存例として先に見る価値がある。

```bash
make quant-policy-runtime-gate
make quant-policy-durable-gate
```

ここで見ること。

- `HIGH` urgency は soft を bypass するか
- hard は bypass しないか
- ingress 側と durable 側の両方で安全境界が揃っているか

読むファイル。

- `scripts/ops/check_quant_policy_runtime_gate.sh`
- `scripts/ops/check_quant_policy_durable_gate.sh`
- `gateway-rust/src/server/http/orders.rs`

## Step 4: `adapt` を手で叩く

gateway をローカル起動した状態で確認する。

base URL は最近の gate と同じなら `http://127.0.0.1:18081`。

`/tmp/twap_intent.json` を作る。

```bash
cat >/tmp/twap_intent.json <<'JSON'
{
  "schemaVersion": 1,
  "intentId": "manual-twap-1",
  "accountId": "acc-1",
  "sessionId": "sess-1",
  "symbol": "AAPL",
  "side": "BUY",
  "type": "LIMIT",
  "qty": 100,
  "limitPrice": 100,
  "timeInForce": "IOC",
  "urgency": "HIGH",
  "executionPolicy": "TWAP",
  "riskBudgetRef": {
    "budgetId": "budget-1",
    "version": 1
  },
  "modelId": "model-1",
  "algo": {
    "sliceCount": 4,
    "sliceIntervalNs": 100,
    "startAtNs": 1000
  },
  "createdAtNs": 100,
  "expiresAtNs": 100000
}
JSON
```

叩く。

```bash
BASE=http://127.0.0.1:18081
curl -sS "$BASE/strategy/intent/adapt" \
  -H 'content-type: application/json' \
  --data-binary @/tmp/twap_intent.json
```

ここで見ること。

- `algoPlan` がある
- `policy` が `TWAP`
- `childCount` が `4`
- `slices[].qty` が `25,25,25,25`

## Step 5: `submit` を手で叩く

`submit` は wrapper が必要。

```bash
cat >/tmp/twap_submit.json <<'JSON'
{
  "intent": {
    "schemaVersion": 1,
    "intentId": "manual-twap-1",
    "accountId": "acc-1",
    "sessionId": "sess-1",
    "symbol": "AAPL",
    "side": "BUY",
    "type": "LIMIT",
    "qty": 100,
    "limitPrice": 100,
    "timeInForce": "IOC",
    "urgency": "HIGH",
    "executionPolicy": "TWAP",
    "riskBudgetRef": {
      "budgetId": "budget-1",
      "version": 1
    },
    "modelId": "model-1",
    "algo": {
      "sliceCount": 4,
      "sliceIntervalNs": 100,
      "startAtNs": 1000
    },
    "createdAtNs": 100,
    "expiresAtNs": 100000
  }
}
JSON
```

```bash
curl -sS "$BASE/strategy/intent/submit" \
  -H 'content-type: application/json' \
  --data-binary @/tmp/twap_submit.json
```

ここで見ること。

- まだ reject になる
- reason が `STRATEGY_POLICY_RUNTIME_NOT_IMPLEMENTED`

この結果は正常。

## Step 6: debugger を使う場所

最初から広く貼らない。次だけでよい。

- `gateway-rust/src/server/http/strategy.rs`
  - `resolve_effective_policy`
  - `handle_post_strategy_intent_adapt`
  - `handle_post_strategy_intent_submit`
- `gateway-rust/src/strategy/algo.rs`
  - `build_algo_execution_plan`

## 読む観点

次が説明できればキャッチアップとして十分。

- `StrategyIntent` は prediction ではなく execution intent か
- `ExecutionConfigSnapshot` は read-mostly config か
- `effectivePolicy` はどの優先順で決まるか
- `adapt` と `submit` の責務がどう分かれているか
- `TWAP / VWAP / POV` は今どこまで入っているか
- `urgency` はどこまで runtime に効くか
- hard / kill / risk の境界をどう守っているか

## いま test で分かること / 分からないこと

### test で分かること

- pure planner の分割ロジック
- handler の契約
- bypass などの境界条件
- 回帰の有無

### test で分からないこと

- 実 route 配線
- 実 JSON の見た目
- 起動時設定や env 差分
- 実運用時の観測のしやすさ

だから順番は `test -> endpoint -> debugger` がよい。
