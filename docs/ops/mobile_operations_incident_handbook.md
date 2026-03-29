# Mobile Operations Incident Handbook

## 目的

mobile で運用工学を学ぶときに、単なる用語暗記ではなく、  
「何が起きたら」「何を先に見て」「どこまで自動で、どこから人判断か」を固定するための手引きである。

## 最初の切り分け

事故に見える事象が出たら、最初に次の 4 つを分ける。

1. 入力が来ていない
2. 入力は来ているが projection が追いついていない
3. projection は進んでいるが順序前提が欠けている
4. projection 不能で手当待ち

この repo では、それぞれを次で見る。

- 入力の進み具合: `offset`
- projection の進み具合: `aggregate progress`
- 順序前提待ち: `pending orphan`
- 手当待ち: `DLQ`

## 見る順番

### 1. live state

- OMS state
- BackOffice state
- venue session
- drop copy equivalent
- go / no-go
- sequence gap
- pending orphan
- dead letter

これで、止まっているのか、詰まっているのか、壊れているのかを分ける。

### 2. reconcile

- OMS reconcile issue
- BackOffice reconcile issue

注文状態側のズレか、台帳側のズレかをここで切る。

### 3. final-out

- status
- timeline
- fills
- reservation release
- balance effect

利用者に何が見えるかを確認する。  
運用メトリクスより前に、利用者へ何を説明するかを固める。

### 4. raw event / source

- eventRef
- source
- raw payload

最後に、画面表示の根拠を確認する。

### 5. control plane

- `GET /api/ops/venue-sessions`
- `GET /api/ops/rollout-state`
- `GET /api/ops/go-no-go`

ここでは、projection 以前に「いま trade flow を進めてよいか」を固定する。

## 典型シナリオ

### sequence gap 増加

症状:

- pending orphan が増え続ける
- offset は進んでいる
- order / ledger の一部が欠けて見える

最初の問い:

- accepted 前提が欠けていないか
- aggregate progress が止まっていないか
- fill-first を pending に逃がせているか

やること:

- pending orphan 件数と対象 order を確認
- raw event の sequence を確認
- replay と reconcile を混同しない

### DLQ 増加

症状:

- dead letter が増える
- pending ではなく DLQ に落ちる
- 再投入しても再度落ちる可能性がある

最初の問い:

- payload が壊れているか
- consumer schema 想定が古いか
- additive ではない schema 変更をしていないか

やること:

- eventRef を保存
- raw payload を保存
- schema 差分確認後に requeue

### market data stale

症状:

- 注文は流れる
- 価格が変わらない
- execution quality と risk の説明が薄くなる

最初の問い:

- stale 表示が出ているか
- arrival benchmark と current price を混同していないか
- venue state を通常時のまま読んでいないか

やること:

- stale 表示を優先
- risk / execution を保守モードで読む
- current price で past execution を塗り替えない

### venue session / drop copy divergence

症状:

- execution session が `RUNNING` ではない
- `drop copy equivalent` が `DIVERGED`
- `go / no-go` が `NO_GO`

最初の問い:

- session 断なのか、projection 断なのか
- post-trade 側だけ遅れていないか
- statement release を止めるべきか

やること:

- `/api/ops/venue-sessions` を先に見る
- `drop copy equivalent` が `DIVERGED` なら books release を止める
- `rollout-state` と `go-no-go` を同時に確認する

## schema 変更時の原則

- 新規項目は additive を原則とする
- default 値と optional を先に決める
- old payload でも replay が通ることを確認する
- UI ラベルを変えても raw event / source field は変えない

## capacity の見方

- hot path latency
- pending orphan / DLQ の増加傾向
- replay 所要時間
- final-out completeness

重要なのは、lag の数字ではなく、  
「利用者へ説明可能な状態まで戻る時間」である。

## mobile での使い方

- `/mobile/operations` で live state と incident drill を読む
- `/mobile/architecture` で責務境界と runbook の順番を読む
- `/mobile/orders/:id` で final-out と raw event を読む
- `/mobile/posttrade` で ledger と statement の違いを読む
- `incident matrix drill` で session / divergence / throttle / halt を反復する
- `local soak` で steady-state 観測を継続する

## 実装アンカー

- `app-java/src/main/java/appjava/http/OpsApiHandler.java`
- `oms-java/src/main/java/oms/audit/GatewayAuditIntakeService.java`
- `backoffice-java/src/main/java/backofficejava/audit/GatewayAuditIntakeService.java`
- `scripts/ops/drill_business_mainline_projection_recovery.sh`
- `scripts/ops/drill_business_incident_matrix.sh`
- `scripts/ops/run_business_soak_local.sh`
