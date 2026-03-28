# Business Mainline Operations Runbook

この runbook は、`frontend -> app-java -> gateway-rust -> oms-java -> backoffice-java` を
Kafka / PostgreSQL / TCP venue simulator 付きで動かすための最小運用手順である。

対象は次の説明である。

- 起動
- 正常性確認
- 注文から最終 out までの確認
- pending / DLQ / sequence gap の見方
- Java projection 再起動と復旧
- 停止

## 前提

- Docker Desktop が起動済み
- Java 21
- Node.js 20 系
- Rust toolchain
- port `8080` `8081` `18081` `18082` `9092` `5432` `9901` が空いている

## 起動

```bash
scripts/ops/run_business_mainline_stack.sh
```

起動後の endpoint:

- `app-java`: `http://localhost:8080`
- `gateway-rust`: `http://localhost:8081`
- `oms-java`: `http://localhost:18081`
- `backoffice-java`: `http://localhost:18082`

UI を見る場合:

```bash
cd frontend
npm run dev
```

## 正常性確認

まず smoke を通す。

```bash
scripts/ops/smoke_business_mainline_stack.sh
```

次に ops gate を通す。

```bash
scripts/ops/check_business_mainline_ops.sh
```

正常時の期待:

- `omsStats.state = RUNNING`
- `backOfficeStats.state = RUNNING`
- `omsBusStats.state = RUNNING`
- `backOfficeBusStats.state = RUNNING`
- `pendingOrphanCount = 0`
- `deadLetterCount = 0`
- `sequenceGaps = 0`
- reconcile `issues = []`

## 注文から最終 Out まで

1. UI から注文する
2. `Orders` に注文が出る
3. `Final Out` に timeline / reservation / fills / positions / raw event ref が揃う
4. `Ops` に pending / DLQ / sequence gap が増えていないことを確認する

CLI だけで見る場合:

```bash
curl -fsS http://localhost:8080/api/orders
curl -fsS http://localhost:8080/api/orders/<orderId>/final-out
curl -fsS http://localhost:8080/api/ops/overview
```

## Pending / DLQ / Sequence Gap の見方

### pending orphan

- `OrderAccepted` より先に `ExecutionReport` が来た
- `aggregateSeq` に gap がある

対処:

1. `Ops` で pending 件数を見る
2. 先行イベントが後から到着するなら自然解消を待つ
3. 残る場合は `Requeue Pending` を実行する

### DLQ

- JSON 不正
- aggregate sequence 不正
- replay 時の parse failure

対処:

1. `Ops` で DLQ entry を見る
2. payload と reason を確認する
3. 修正可能なら `Requeue DLQ` を実行する
4. 再投入不能なら調査対象として残す

### sequence gap

`sequenceGaps > 0` は、`aggregateSeq` が `lastApplied + 1` ではないイベントが来たことを意味する。

確認箇所:

- `OMS Intake -> Seq Gap`
- `BackOffice Intake -> Seq Gap`
- pending entry の `reason`

対処:

1. 先行 sequence のイベントが未着か確認する
2. 到着後に pending が自然解消するか確認する
3. 解消しない場合は `Requeue Pending` を実行する
4. それでも残る場合は raw payload を確認し、DLQ へ送る

## Projection 再起動と復旧

Java projection の restart recovery drill:

```bash
scripts/ops/drill_business_mainline_projection_recovery.sh
```

この drill は `app-java` `oms-java` `backoffice-java` を停止してから再起動し、
ops gate が再び通ることを確認する。

## 停止

```bash
scripts/ops/stop_business_mainline_stack.sh
```

## 補足

- `gateway-rust` hot path はこの runbook では変更しない
- OMS / BackOffice の順序制御は `aggregateSeq` ベース
- bus event の pending / DLQ 再投入でも、raw bus payload を再解釈して順序制御を維持する
