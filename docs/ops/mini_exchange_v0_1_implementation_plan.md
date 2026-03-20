# Mini Exchange v0.1 実装計画

更新日: 2026-03-20
対象ブランチ: `codex/mini-exchange-v0-1`
前提仕様: `docs/ops/mini_exchange_v0_1_spec.md`

## 1. 目的

- 板、約定、price-time priority、partial fill、cancel の意味を Rust 実装で追えるようにする
- strategy / execution policy 側の評価土台として、最小の市場構造を repo 内に持つ
- HFT gateway 本体とは分離した学習用 crate として実装し、責務を混ぜない

## 2. 実装方針

- 置き場所は repo 直下の独立 crate `mini-exchange/`
- ネットワーク、永続化、GUI は入れない
- 単一スレッド、1 銘柄、LIMIT / MARKET / cancel に限定
- データ構造は price level ごとの FIFO を明示的に持つ

## 3. 構成

### 3.1 crate

- `mini-exchange/Cargo.toml`
- `mini-exchange/src/lib.rs`

### 3.2 主要型

- `Side`
- `OrderType`
- `OrderInput`
- `Trade`
- `BookSnapshot`
- `SubmitOutcome`
- `CancelOutcome`
- `MatchingEngine`

## 4. コア責務

### 4.1 Validation

- LIMIT なのに price なしを reject
- MARKET なのに price ありを reject
- qty <= 0 を reject
- duplicate order_id を reject

### 4.2 Matching

- BUY/SELL 両方向
- LIMIT/MARKET
- resting order 側価格で約定
- partial fill
- price-time priority

### 4.3 Book State

- bids は価格降順
- asks は価格昇順
- 同価格帯 FIFO
- last_price 更新
- snapshot で aggregate qty を返す

### 4.4 Cancel

- 板上の active order のみ cancel
- filled / expired / 不存在は reject

## 5. フェーズ

### Phase 1

- crate skeleton
- validation
- Case 1

### Phase 2

- LIMIT / MARKET matching
- Case 2, 3, 4, 7

### Phase 3

- cancel
- Case 5, 6

## 6. 完了条件

- spec の Case 1〜7 が test で通る
- snapshot / trades / last_price が検証できる
- strategy 側から読んだときに、passive/aggressive の意味を説明できる最低土台になる

