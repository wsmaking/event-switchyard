# Business Flow Debug Guide

## 目的

- 注文投入から BackOffice 反映までを Docker なしで一気通貫に追うこと
- Java 側は JDWP attach、Rust 側は process attach で breakpoint を当てること
- `final-out`、fills、ledger を同じ注文 ID で確認すること

## 起動

1. VS Code で task `Business Debug Stack: Start` を実行
2. もしくはターミナルで `scripts/ops/run_business_debug_stack.sh`
3. 起動後に `Attach business Java services`
4. Rust を見る場合は `Attach gateway-rust`

### debug port

- `app-java`: `5005`
- `oms-java`: `5006`
- `backoffice-java`: `5007`
- `tcp-exchange-sim`: `5008`

## breakpoint の貼りどころ

### 1. 注文受理

- `gateway-rust/src/server/http/orders/classic.rs`
- `handle_order_with_contract`

見るポイント:

- JWT 認証を通過しているか
- Risk と durable append の順序
- exchange queue へ積まれているか

### 2. OMS 反映

- `oms-java/src/main/java/oms/audit/GatewayAuditIntakeService.java`
- `applyAccepted`
- `applyExecutionReport`

見るポイント:

- audit line がどの event type として解釈されたか
- `OrderAccepted` と `ExecutionReport` の反映順
- `aggregateSeq` が gap なく進んでいるか

### 3. BackOffice 反映

- `backoffice-java/src/main/java/backofficejava/audit/GatewayAuditIntakeService.java`
- `applyAccepted`
- `applyExecutionReport`

見るポイント:

- fill 反映で overview / positions / fills / ledger がどう動くか
- partial と full fill で ledger の行がどう増えるか
- orphan や DLQ に落ちていないか

### 4. 画面向け集約

- `app-java/src/main/java/appjava/http/OrderApiHandler.java`
- `/api/orders/{id}/final-out` 分岐

見るポイント:

- OMS raw events
- reservations
- BackOffice fills / positions / account overview
- execution quality が 1 つの payload にどうまとまるか

## 動作確認

### 自動 smoke

- VS Code task `Business Debug Stack: Smoke`
- もしくは `scripts/ops/smoke_business_debug_stack.sh`

この script は次をまとめて確認する。

- `app-java -> gateway-rust` で実注文を送る
- `final-out` が `FILLED` まで進む
- OMS order events が返る
- BackOffice overview / positions / fills / ledger が返る

### 手動確認

1. breakpoint を貼る
2. `curl -X POST http://localhost:8080/api/orders ...`
3. breakpoint で止まるたびに step 実行
4. 最後に `GET /api/orders/{id}/final-out`
5. `GET http://localhost:18082/fills?orderId=...`
6. `GET http://localhost:18082/ledger?accountId=acct_demo&orderId=...&limit=20`

## 停止

- VS Code task `Business Debug Stack: Stop`
- もしくは `scripts/ops/stop_business_debug_stack.sh`

## state dir

- `var/business-debug`

ここに debug 用の audit log、offset、log file を分離して置く。
