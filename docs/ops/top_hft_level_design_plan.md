# TOP HFTレベル設計改善計画（v3）

更新日: 2026-03-14
対象ブランチ: `codex/top-hft-design`

## 1. 目的
- 計測区間を変えずに、`ack_accepted` の実性能を TOP HFT 実務水準へ寄せる。
- 目標は「同一条件（10k RPS）での `ack_accepted p99` を一桁us帯へ収束」。
- 設計改善を段階導入し、都度ゲートで回帰を止める。

## 2. 固定する計測前提（逃げない条件）
- サーバ側主指標: `server_hotpath_accepted_p99_ns`
- 補助指標: `server_live_ack_accepted_p99_us`, `client_e2e_accepted_p99_us`
- 負荷条件の基準:
  - `TARGET_RPS=10000`
  - `accepted_rate >= 0.99`
  - `rejected_killed_total == 0`
  - `loss_suspect_total == 0`

## 3. 現状の課題
- 現状は `ack_accepted p99` が概ね 16us 前後で、10us未満安定に届いていない。
- ホットパス内にまだ「可変コスト（認証、分岐、共有状態アクセス）」が残る。
- プロセス単位チューニングは入っているが、スレッド/コア局所性は不十分。

## 4. 改善方針（設計）

### 4.1 入口プロトコルと認証
- 固定長バイナリ + 永続接続を標準経路に固定（HTTPは比較用）。
- JWT検証をホットパスから除外:
  - 接続確立時に1回だけ検証
  - 接続内は短い `session token` 参照のみ
- `account_id` / `session_id` は接続コンテキストへ固定し、都度文字列処理を削減。

### 4.2 スレッドモデルとコア局所性
- `single-writer shard` を維持しつつ、以下を追加:
  - ingress worker / shard worker / tx worker をコア固定
  - Linuxでは `taskset` だけでなくスレッド単位 affinity を導入
- shard間共有を減らし、「同一データは同一コア」で処理。

### 4.3 ホットパス完全無アロケーション
- 事前確保:
  - リクエスト/レスポンスバッファ
  - タスクオブジェクト pool
  - 文字列変換不要な固定長ID表現
- 受理経路で `String` clone/format を禁止し、IDは固定長バイナリで運搬。

### 4.4 ロック削減
- 共有状態を shard ローカルへ寄せる。
- 集計は周期バッチで非同期反映（hot path では原則 lock-free）。
- confirm/durable 系は ACK経路から継続分離し、観測はメトリクスで担保。

### 4.5 durable 経路の徹底分離
- ACK側の責務は「受理判定とキュー投入」までに固定。
- durableの圧迫は別系統ガードで制御し、ACK遅延に直結させない。
- 可視化は `durable_ack_path_guard_enabled` と backpressure 系時系列で監査。

### 4.6 Linux低レイヤ最適化
- `SO_BUSY_POLL`（既存）を運用標準化。
- 追加対象:
  - IRQ affinity
  - RPS/XPS
  - C-state/P-state固定
  - NIC ring / socket buffer の最適化
- これらはコード外手順として runbook 化する。

### 4.7 計測の厳密化
- `hotpath_ns` と `e2e_us` を分離運用（既存導入済み）。
- 次段で `rdtscp` 計時を導入し、`now_nanos` と二重記録して整合検証。
- 採用条件:
  - TSC安定性チェック
  - コア跨ぎ補正ルール
  - 異常時の自動フォールバック

## 5. 段階導入ロードマップ

### Phase 1（短期: すぐ着手）
- 接続時認証キャッシュの強化（requestごとのclone削減）
- 追加アロケーションの削減
- `ack_accepted p99` を 14us未満へ

### Phase 2（中期: 構造変更）
- スレッド単位コア固定
- shardローカル化を拡大
- `ack_accepted p99` を 12us未満へ

### Phase 3（中長期: TOP HFT寄せ）
- `rdtscp` 計時導入
- 無アロケ徹底 + lock削減完了
- 10k条件で `ack_accepted p99 < 10000ns` を継続検証

## 6. 合否ゲート（この計画で守る条件）
- `completed_rps >= 10000`
- `accepted_rate >= 0.99`
- `server_hotpath_accepted_p99_ns <= target`
- `rejected_killed_total == 0`
- `loss_suspect_total == 0`

`target` は段階的に下げる:
- Step A: 14000ns
- Step B: 12000ns
- Step C: 10000ns

## 7. 実装対象（repoマッピング）
- サーバ本体:
  - `gateway-rust/src/server/http/mod.rs`
  - `gateway-rust/src/server/http/orders.rs`
  - `gateway-rust/src/server/http/metrics.rs`
- 負荷/ゲート:
  - `scripts/ops/run_v3_open_loop_probe.sh`
  - `scripts/ops/check_v3_local_strict_gate.sh`
  - `scripts/ops/check_v3_capability_gate.sh`
  - `scripts/ops/open_loop_v3_tcp_load.py`
- ドキュメント:
  - `README.md`
  - `docs/ops/current_design_visualization.md`
  - 本書

## 8. リスクと回避策
- リスク: 低レイヤ最適化で再現性が落ちる
  - 回避: preset固定 + runbook固定 + A/Bログ保存
- リスク: ackは速いが受理率が崩れる
  - 回避: `accepted_rate` を必須ゲート化
- リスク: durable異常の見逃し
  - 回避: durable系時系列と警告閾値を維持

