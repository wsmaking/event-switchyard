# Quant Runtime Status as of 2026-03-20

## 位置付け

この文書は、2026-03-20 時点の quant / strategy / mini-exchange 周辺の到達点を固定するための status anchor。

目的は 3 つ。

- 何が `main` に入っているかを明確にする
- 何が未マージの研究枝かを明確にする
- 次に着手するべき本丸がどこかを明確にする

## 現在の基準

### `main`

`main` の先頭は `1f7eacb`。

ここまでに入っているもの。

- quant boundary skeleton
- quant evaluation integration
- quant eval gate
- quant policy runtime gate
- mini-exchange と quant evaluation pipeline

要するに、`評価基盤` と `urgency による soft admission 制御の安全境界` までは `main` に入っている。

### 未マージ branch 1: `codex/quant-policy-durable-gate`

先頭は `e06f706`。

ここでやっていること。

- durable controller soft に対する `HIGH` urgency bypass の gate
- durable backlog soft に対する `HIGH` urgency bypass の gate
- durable confirm age soft に対する `HIGH` urgency bypass の gate
- ただし durable hard は bypass しないことの固定

注意点。

- 実装バグではなく test harness 側の不備を 1 件修正済み
- bypass 後に enqueue まで進む test が `build_test_state()` を使っていて `V3_INGRESS_CLOSED` になっていた
- 現在は live ingress receiver を持つ state に修正済み
- `scripts/ops/check_quant_policy_durable_gate.sh` は pass

### 未マージ branch 2: `codex/quant-algo-runtime`

先頭は `eeea37f`。

ここでやっていること。

- algo execution planning contract の追加
- `StrategyIntent.algo` の追加
- `TWAP / VWAP / POV` の child-order plan を `adapt` で返せる状態
- `submit` はまだ `STRATEGY_POLICY_RUNTIME_NOT_IMPLEMENTED` で reject 維持

この branch の意味。

- `algo execution` の runtime 本体ではない
- まだ `planning contract` と `parent/child 境界` の段階

## すでに固まったもの

### 1. quant boundary

- `StrategyIntent`
- `ExecutionConfigSnapshot`
- `FeedbackEvent`
- `ShadowRecord`
- `/strategy/config`
- `/strategy/intent/adapt`
- `/strategy/intent/submit`
- `/strategy/intent/shadow`

### 2. quant evaluation

- quant intent export
- gateway submit / feedback / shadow capture
- mini-exchange compare
- single join / batch join
- quant eval gate

### 3. runtime safety gate

- ingress queue soft reject に対する urgency bypass
- durable soft reject に対する urgency bypass
- hard / kill / risk は bypass しない

ここまでで、`strategy intent を gateway runtime に安全に効かせる入口` はかなり固まっている。

## 本丸

本丸は `algo execution runtime`。

具体的には次。

- parent execution store
- scheduler
- child order dispatch
- child feedback の parent 集約
- `TWAP / VWAP / POV` の actual runtime
- venue / routing との接続

つまり、今までは

- 受ける
- 評価する
- 優先度で admission を変える

までで、これからは

- 実際にどう執行するか

に入る段階。

## 今どこまで理解できていれば十分か

次が説明できれば、現状の anchor として十分。

- `StrategyIntent` は prediction ではなく execution intent である
- `adapt` は dry-run、`submit` は actual v3 submit である
- `Passive / Aggressive` は最小 hook まで入っている
- `TWAP / VWAP / POV` は `adapt` では plan が返るが `submit` はまだ reject である
- `HIGH` urgency は soft admission だけ bypass できる
- hard / kill / risk の安全境界は維持している
- mini-exchange は本番取引所ではなく評価用市場モデルである

## 次にやるべきこと

優先順は次。

1. `codex/quant-policy-durable-gate` を main に入れる
2. `codex/quant-algo-runtime` を読みながら仕様理解する
3. その後で algo execution runtime 本体へ進む

## 関連文書

- `docs/ops/quant_vs_hft_gateway_boundary.md`
- `docs/ops/mini_exchange_v0_1_spec.md`
- `docs/ops/mini_exchange_v0_1_implementation_plan.md`
- `docs/ops/quant_math_stats_learning_map.md`
