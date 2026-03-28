# Java OMS / BackOffice 実装ロードマップ

最終更新: 2026-03-28

## 目的

`gateway-rust` を前段の低レイテンシ受理系として維持しつつ、後段の

- OMS
- BackOffice

を **Java 21** で実装し、注文、リスク拘束、取引所イベント収束、残高・ポジション反映、照合、運用までを一気通貫で再現する。

加えて、**フロント UI から注文を出し、最終 out まで人が追える業務再現環境** を完成条件に含める。

このロードマップは、現行リポジトリの実装状況を前提に、

- 何をどの順で作るか
- モジュールをどう分けるか
- どの段階で「業務再現」と言えるか

を明確にするための実装計画である。

## 現在の実装状況

2026-03-28 時点で、次までは実装済み。

- `app-java/` `oms-java/` `backoffice-java/` の Java モジュール追加
- `frontend -> app-java -> oms-java -> backoffice-java` の replay 縦導線
- UI からの replay scenario 実行
- `final out` での order / timeline / reservation / fills / balance delta / positions / raw event ref 表示
- OMS / BackOffice の file snapshot による restart 復元
- `scripts/ops/run_java_replay_stack.sh` などの起動 / 停止 / smoke 導線
- `gateway-rust` の `audit.log` を Java OMS / BackOffice が tail する intake
- `oms-java:/stats /reconcile /orphans` `backoffice-java:/stats /reconcile /ledger /orphans`
- `app-java:/api/ops/overview` `app-java:/api/ops/audit/replay`
- frontend からの ops 可視化と audit replay 起動
- orphan / DLQ の保存と UI 可視化

一方で、まだ未完なのは次である。

- `gateway-rust` outbox / Kafka からの Java 側 event intake
- sequence / orphan pending / DLQ の本格化
- UI の SSE / WebSocket 反映
- audit log ではなく bus event / exchange truth で OMS / BackOffice を収束させる本線

## 前提

### 実装上の制約

- Kotlin 版の `app/` `gateway/` `backoffice/` は凍結し、新規業務実装は Java に一本化する
- なるだけ早く進めるため、実装は少ない PR で縦に通す
- 新たな外部ライブラリは追加しない
- 既存 `gateway-rust` の hot path 性能を破壊しない

### 維持する境界

- `gateway-rust`
  - 受理
  - hot path
  - backpressure
  - audit WAL
  - outbox
  - SSE
- Java OMS
  - 注文状態管理
  - 余力拘束 / 解放
  - event 正規化
  - 顧客参照 API
  - replay / reconcile / ops
- Java BackOffice
  - fill ledger
  - balances
  - positions
  - realized PnL
  - statement / reconciliation

### 置かないもの

- 取引所 matching engine の本格実装
- 清算 / 受渡 / 会計のフル再現
- 本番 UI の作り込み

ただし、業務再現用の最小 UI / BFF は対象に含める。

## 現状の整理

現状のリポジトリには、業務再現の部品はすでに一部ある。

- `gateway-rust` には注文受付、監査 WAL、outbox、execution report 反映がある
- `backoffice` には fills 起点の balances / positions / realized PnL がある
- `contracts/` には order state / risk / position cap の契約がある

一方で、次が不足している。

- OMS 本体の注文状態テーブルと注文一覧 API
- reserve / release を持つ余力モデル
- event ID / sequence / source を持つ業務イベント契約
- cancel / replace / amend の venue 連携完結
- duplicate / out-of-order / DLQ を含む consumer 安全性
- gateway truth / exchange truth / backoffice truth の照合

## 採用方針

### 言語 / フレームワーク

- Java 21
- JDK HTTP Server
- Kafka Client
- Flyway
- jOOQ

### 永続化方針

- PostgreSQL を正本 DB にする
- OMS の状態遷移更新は JPA ではなく jOOQ 中心にする
- BackOffice も jOOQ 中心にする
- JPA は採用しないか、参照系だけに限定する

理由:

- 注文状態遷移と冪等制御は SQL を明示した方が安全
- duplicate / sequence / compare-and-set を扱いやすい
- 面接・設計説明でもトランザクション境界を示しやすい

## 推奨リポジトリ構成

### 新規モジュール

- `app-java/`
- `oms-java/`
- `backoffice-java/`

### 既存モジュールの扱い

- `gateway-rust/`
  - 継続利用
- `gateway/`
  - Kotlin 版凍結
  - 旧比較資産として維持
- `backoffice/`
  - Kotlin 版凍結
  - 旧比較資産として維持するか、`backoffice-java/` 完了後に置換
- `app/`
  - Kotlin 版凍結
  - UI 接続の旧比較資産として維持し、Java 側へ置換する

### Java モジュールの責務

#### `oms-java`

- gateway event の intake
- order aggregate 更新
- reservation 更新
- order query API
- ops API
- OMS outbox

#### `backoffice-java`

- fill / fee / cash ledger
- balances / positions / realized PnL
- backoffice query API
- reconcile job
- statement export の土台

## 目標アーキテクチャ

```text
client / strategy
    -> gateway-rust
        -> audit WAL
        -> outbox -> Kafka
            -> oms-java
                -> OMS DB
                -> OMS outbox -> Kafka
                    -> backoffice-java
                        -> BackOffice DB
            -> backoffice-java (raw execution/fill系を直接読む経路は必要に応じて併用)
```

設計原則:

- `gateway-rust` は速く受ける
- OMS が注文の業務状態を収束させる
- BackOffice が約定後の金額・ポジションを収束させる
- 監査と照合は別経路で持つ
- Java 側の追加処理は Kafka / DB / read API 側に閉じ、`gateway-rust` の同期 hot path に割り込ませない
- UI / BFF の都合で `gateway-rust` に重い業務参照を持ち込まない

## UI 起点の業務再現環境で追加で必要なもの

### 目標体験

- フロント UI から注文を送信できる
- 即時に accepted / rejected が見える
- その後の partial fill / fill / cancel / expire が追える
- 最終 out として、注文結果、余力変化、残高変化、ポジション変化、PnL 変化を確認できる
- シナリオを reset して同じ流れを繰り返し再現できる

### 現状のギャップ

- `frontend` は注文 UI を持つが、表示状態が簡略化されている
- `app/` は in-memory の注文保持が中心で、OMS 正本を UI に返していない
- 注文タイムライン、reservation、fill 明細、balance / position / PnL 差分を同一導線で確認できない
- デモ口座初期化、シナリオ再実行、最終 out 画面がない

### 追加実装項目

- UI 向け BFF
  - `app/` を `app-java/` へ置き換える
  - `gateway-rust`、`oms-java`、`backoffice-java` を UI 向けに束ねる
- UI 注文表示モデルの拡張
  - `ACCEPTED`
  - `PARTIALLY_FILLED`
  - `CANCEL_PENDING`
  - `CANCELED`
  - `EXPIRED`
  - `REJECTED`
  - `FILLED`
- 注文詳細 / 最終 out API
  - `GET /ui/orders/{id}`
  - `GET /ui/orders/{id}/timeline`
  - `GET /ui/orders/{id}/final-out`
- 口座サマリ API
  - `GET /ui/accounts/{id}/overview`
  - `GET /ui/accounts/{id}/activity`
- リアルタイム配信
  - OMS / BackOffice 更新を SSE か WebSocket で UI に反映する
- デモ制御 API
  - `POST /ui/demo/reset`
  - `POST /ui/demo/seed-account`
  - `POST /ui/demo/scenarios/{name}/run`
- 最終 out 画面で見せる情報
  - order timeline
  - reservation lifecycle
  - fills
  - balance delta
  - position delta
  - realized PnL delta
  - raw event reference

### このセクションの位置付け

ここで必要なのは、本番 BackOffice 画面の作り込みではない。

必要なのは、

- 注文を出せる
- 状態遷移を追える
- 最終結果を見られる
- reset して再現できる

という、業務説明とデモに必要な最小一気通貫環境である。

### 実装ポリシー

- 初期実装では既存依存だけを使い、フレームワーク導入は増やさない
- まず Java モジュールを独立起動可能にし、その後に OMS / BackOffice の業務核を縦に通す
- PR は「土台」「OMS 最小核」「BackOffice 最小核」「UI replay」のように少数に絞る

## フェーズ別ロードマップ

## Phase 0 基盤整備

### 目的

Java 実装に入る前に、イベント契約とモジュール土台を固定する。

### 実装項目

- `settings.gradle` に `oms-java` と `backoffice-java` を追加
- Java モジュール雛形を作成
- Docker Compose に PostgreSQL と Kafka の Java 用設定を追加
- `contracts/` に業務イベント v2 を追加
- event envelope に以下を追加
  - `eventId`
  - `eventType`
  - `schemaVersion`
  - `sourceSystem`
  - `aggregateId`
  - `aggregateSeq`
  - `occurredAt`
  - `ingestedAt`
  - `accountId`
  - `orderId`
  - `venueOrderId`
  - `correlationId`
  - `causationId`

### 完了条件

- Java モジュールが起動する
- Kafka からイベントを読める
- PostgreSQL migration が通る
- 新 event schema が固定される

## Phase 1 OMS 最小核

### 目的

「注文の正本状態は OMS にある」と言える最小構成を作る。

### DB テーブル

- `oms_orders`
- `oms_order_events`
- `oms_event_dedup`
- `oms_consumer_checkpoint`
- `oms_outbox`
- `oms_dead_letter`

### 状態

- `PENDING_ACCEPT`
- `ACCEPTED`
- `REJECTED`
- `PARTIALLY_FILLED`
- `FILLED`
- `CANCEL_PENDING`
- `CANCELED`
- `EXPIRED`
- `AMEND_PENDING`

### 実装項目

- `OrderAccepted`
- `OrderRejected`
- `ExecutionReportPartial`
- `ExecutionReportFilled`

の consumer 実装

- order detail API
  - `GET /orders/{id}`
- order list API
  - `GET /orders`
- event idempotency
  - `event_id` 単位
- aggregate sequence check
  - `aggregate_seq` 単位

### 完了条件

- duplicate event を二重反映しない
- same order の sequence 逆転を検知できる
- 注文詳細 API で OMS 状態が参照できる

## Phase 2 リスク拘束モデル

### 目的

accepted 後の余力拘束 / 解放を OMS 側で持つ。

### DB テーブル

- `oms_account_buying_power`
- `oms_reservations`
- `oms_reservation_events`

### 実装項目

- accepted 時に reservation 作成
- reject / cancel / expire 時に release
- fill 時に reserved から actual cash movement へ変換
- API
  - `GET /accounts/{id}/buying-power`
  - `GET /accounts/{id}/reservations`

### 重要な業務ルール

- available / reserved / consumed を分ける
- cancel 中は即 release しない
- cancel accepted で release
- partial fill 後は残予約だけ残す

### 完了条件

- reserve / release の状態遷移が order state と矛盾しない
- reservation 単位で追跡できる

## Phase 3 取引所イベント収束

### 目的

cancel / replace / amend を含む注文ライフサイクルを OMS に収束させる。

### 実装項目

- `CancelRequested`
- `CancelAccepted`
- `CancelRejected`
- `AmendRequested`
- `AmendAccepted`
- `AmendRejected`
- `Expired`

の業務イベント追加

- `gateway-rust` 側で cancel / amend venue 連携を完結させる
- OMS 側で cancel/fill race を明示的に処理する
- orphan event 保留テーブルを持つ
  - order meta 未到着
  - sequence gap

### 完了条件

- cancel と fill の競合シナリオを再現できる
- out-of-order イベントを即 discard せず保留できる
- orphan event の再処理 job がある

## Phase 4 BackOffice 最小核

### 目的

約定から金額・ポジションを再計算できる Java BackOffice を作る。

### DB テーブル

- `bo_order_meta`
- `bo_fills`
- `bo_balances`
- `bo_positions`
- `bo_realized_pnl`
- `bo_event_dedup`
- `bo_consumer_checkpoint`
- `bo_dead_letter`

### 実装項目

- fill consumer
- fee 計算
- realized PnL
- balances / positions API
  - `GET /balances`
  - `GET /positions`
  - `GET /fills`
  - `GET /pnl`

### 完了条件

- same fill の duplicate を二重計上しない
- `filled_qty_total` 単調性を守る
- account 単位で残高 / ポジション / PnL を参照できる

## Phase 4.5 UI 業務再現環境

### 目的

フロント UI から注文し、OMS / BackOffice の最終結果まで人が追える最小環境を作る。

### 実装項目

- UI 向け BFF を Java で実装
- 既存 `app/` の in-memory order view を廃止し、OMS / BackOffice 読み取りに寄せる
- UI 表示用の注文状態モデルを OMS 状態に揃える
- API
  - `POST /ui/orders`
  - `GET /ui/orders`
  - `GET /ui/orders/{id}`
  - `GET /ui/orders/{id}/timeline`
  - `GET /ui/orders/{id}/final-out`
  - `GET /ui/accounts/{id}/overview`
  - `POST /ui/demo/reset`
- UI 更新用 SSE / WebSocket を追加
- 最終 out 画面
  - order status
  - execution report
  - reservation result
  - balance / position / PnL 反映

### 完了条件

- UI から注文送信できる
- accepted / rejected を即時表示できる
- partial / fill / cancel / expire をリアルタイムで追える
- 1 注文単位で最終 out を確認できる
- reset 後に同じシナリオを再実行できる

## Phase 5 Reconciliation / Ops

### 目的

「壊れた時に戻せる」運用面を揃える。

### 実装項目

- OMS reconcile
  - OMS state vs gateway audit
  - OMS state vs venue report
- BackOffice reconcile
  - BackOffice state vs fill ledger
  - BackOffice state vs OMS reservation consumption
- ops API
  - `POST /ops/reconcile/orders/{id}`
  - `POST /ops/replay/events/{id}`
  - `POST /ops/requeue/dlq/{id}`
- metrics
  - consumer lag
  - duplicate discard count
  - sequence gap count
  - orphan pending count
  - reconcile mismatch count
  - DLQ size

### 完了条件

- mismatch を検知して手動再処理できる
- DLQ を再投入できる
- lag と不整合を監視できる

## Phase 6 ハードニング

### 目的

業務再現として説明できる品質まで上げる。

### 実装項目

- restart recovery
- checkpoint recovery
- replay test suite
- long soak test
- failure injection
  - OMS DB down
  - Kafka redelivery
  - out-of-order fill
  - cancel accepted late arrival

### 完了条件

- restart 後に state が復元される
- replay で同じ結果になる
- chaos 系の代表シナリオを通せる

## API スコープ

### OMS API

- `GET /orders/{id}`
- `GET /orders`
- `GET /orders/{id}/events`
- `GET /accounts/{id}/buying-power`
- `GET /accounts/{id}/reservations`
- `POST /ops/reconcile/orders/{id}`
- `POST /ops/replay/events/{id}`

### BackOffice API

- `GET /balances`
- `GET /positions`
- `GET /fills`
- `GET /pnl`
- `GET /ledger`
- `GET /reconcile`

### UI / Replay API

- `POST /ui/orders`
- `GET /ui/orders`
- `GET /ui/orders/{id}`
- `GET /ui/orders/{id}/timeline`
- `GET /ui/orders/{id}/final-out`
- `GET /ui/accounts/{id}/overview`
- `GET /ui/accounts/{id}/activity`
- `POST /ui/demo/reset`
- `POST /ui/demo/seed-account`
- `POST /ui/demo/scenarios/{name}/run`

## テスト戦略

### 1. 契約テスト

- event schema
- order state transition fixture
- risk / reservation fixture

### 2. Repository テスト

- duplicate event
- aggregate sequence mismatch
- optimistic update
- replay from checkpoint

### 3. Integration テスト

- `gateway-rust -> Kafka -> oms-java`
- `gateway-rust -> Kafka -> oms-java -> Kafka -> backoffice-java`

### 4. E2E シナリオ

- `frontend -> UI BFF -> gateway-rust -> oms-java -> backoffice-java`
- accepted -> full fill
- accepted -> partial -> cancel accepted
- accepted -> cancel rejected -> later fill
- duplicate fill redelivery
- out-of-order fill before order accepted
- expire
- UI で注文後に final out を確認
- reset 後に同一シナリオを再実行

## 実装順の推奨

順番は固定する。

1. `oms-java` 雛形と event contract v2
2. OMS order state と order query
3. OMS reservation
4. `gateway-rust` cancel / amend venue 完結
5. `backoffice-java` fill ledger
6. UI BFF / final out / demo reset
7. reconcile / DLQ / ops

この順にしないと、UI を先に作っても正本のない見せかけ画面になり、BackOffice を先に作っても業務正本が曖昧なままになる。

## 直近の着手タスク

最初の 2 週間でやるべきものは次の通り。

### Week 1

- `oms-java/` を追加
- Java モジュール起動
- Flyway 初期 migration
- Kafka consumer 雛形
- `bus_event_v2` schema 作成

### Week 2

- `oms_orders`
- `oms_order_events`
- `oms_event_dedup`
- `OrderAccepted` consumer
- `GET /orders/{id}`

ここまで終われば、Java OMS の最小核に着手できたと言える。

## マイルストーン

### M1

OMS が `OrderAccepted / Partial / Filled / Rejected` を保持できる

### M2

OMS が reservation を持てる

### M3

cancel / amend / expire を含めて OMS が収束できる

### M4

BackOffice が fills から balances / positions / PnL を作れる

### M5

フロント UI から注文し、最終 out まで確認できる

### M6

reconcile / replay / DLQ を含めて運用再現できる

## 完了判定

次を満たしたら、「注文、リスクチェック、取引所連携、BackOffice までの一気通貫業務再現」ができたと判定する。

- フロント UI から注文できる
- gateway-rust が注文を受ける
- OMS が注文状態を正本として保持する
- OMS が reservation を保持する
- venue event で cancel / amend / fill が収束する
- BackOffice が balances / positions / PnL を作る
- UI から 1 注文の最終 out を追える
- reset 後に同じシナリオを再現できる
- duplicate / out-of-order / replay に耐える
- reconcile と ops API がある

## 補足

この計画では、Java は単なる CRUD サービスではない。

- OMS は業務状態の正本
- BackOffice は金額とポジションの正本

として置く。

したがって、実装優先度は

- API の見た目

ではなく、

- event contract
- transaction boundary
- idempotency
- replay
- reconciliation

を先に固めるべきである。
