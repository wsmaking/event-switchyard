# Event Switchyard 設計図（差し替え後・完全版）

## 1. このシステムの立ち位置（前提）

Event Switchyard は **取引所（Exchange）ではない**。  
本プロジェクトは **Strategy(App) + Execution Gateway/SOR(Gateway) + BackOffice** の3層構成で、  
**Gateway が Execution Gateway / Smart Order Router（SOR）** として動作する。

- App（手動 or 自動）から注文を受信
- リスクチェックを同期的に実施（受理/却下）
- 受理した注文をFast Pathに載せて外部取引所へ送信
- 約定結果（Execution Report）を受信
- 監査証跡（append-only）を保持（replay可能）
- バックオフィス・監視系へ非同期配信
- 外部公開はGatewayのみ（App/BackOffice は内部ネットワークに配置）
- HTTPレスポンスは受理（ACK）まで
- 約定・部分約定・取引所拒否は 非同期通知
  - WebSocket / SSE / Kafka / Polling 等

---

## 2. 全体フロー（技術視点）

### 2.1 コンポーネント構成

```mermaid
flowchart LR
  C[Strategy / App] -->|POST /orders| G[HttpGateway]
  G --> OS[OrderService]
  OS --> PR[PreTradeRisk]

  PR -->|OK| FPQ[(FastPath Queue)]
  PR -->|NG| REJ[Reject Response]

  FPQ --> FPE[FastPathEngine]
  FPE --> SOR[Smart Order Router]
  SOR -->|send order| EX[Exchange API]
  EX -->|execution report| ERH[ExecutionReportHandler]

  ERH --> AQ[(Chronicle Queue / Audit Log)]
  ERH --> BUS[(Kafka / Event Log)]
  ERH --> SSE[SSE Hub]
  BUS --> BO[BackOffice / Settlement / Reporting]
  BUS --> MON[Metrics / Tracing]

  MON --> GATE[Perf Gate]
  GATE --> CI[CI Pipeline]

  G -->|202 Accepted: orderId| C
  SSE -->|execution report| C
```

### 2.2 技術シーケンス（Fast Path / Async Path 分離）

```mermaid
sequenceDiagram
  participant App as Strategy/App
  participant Gw as HttpGateway
  participant Svc as OrderService
  participant Risk as PreTradeRisk
  participant Q as FastPathQueue
  participant FP as FastPathEngine
  participant Sor as SOR
  participant Ex as ExchangeAPI
  participant ER as ExecReportHandler
  participant AQ as AuditLog
  participant K as Kafka
  participant Push as WS/SSE

  App->>Gw: POST /orders
  Gw->>Svc: accept(order)
  Svc->>Risk: validate(order)

  alt Risk OK
    Risk-->>Svc: OK
    Svc->>Q: enqueue(order)
    Svc-->>Gw: accepted(orderId)
    Gw-->>App: 202 Accepted

    Q->>FP: dequeue
    FP->>Sor: route(order)
    Sor->>Ex: send(order)
    Ex-->>ER: executionReport

    ER->>AQ: append-only audit
    ER->>K: publish TradeExecuted / OrderUpdate
    ER-->>Push: notify execution
  else Risk NG
    Risk-->>Svc: NG(reason)
    Svc-->>Gw: reject
    Gw-->>App: 4xx Rejected
  end
```

### 2.3 現行実装のソースフロー（app側）

- エントリーポイント: `app/src/main/kotlin/app/Main.kt` -> `app/src/main/kotlin/app/http/HttpIngress.kt`
- 注文API: `app/src/main/kotlin/app/http/OrderController.kt` -> `app/src/main/kotlin/app/engine/Router.kt`（ローカル判定） -> `app/src/main/kotlin/app/order/OrderExecutionService.kt` -> `app/src/main/kotlin/app/clients/gateway/GatewayClient.kt`（/orders）
  - リアルタイム更新: `app/src/main/kotlin/app/clients/gateway/GatewaySseClient.kt` -> `app/src/main/kotlin/app/http/OrderController.kt`
  - フォールバック: `OrderExecutionService.fetchOrder` で PENDING の再同期
- 自動戦略: `app/src/main/kotlin/app/strategy/StrategyAutoTrader.kt` -> `app/src/main/kotlin/app/fast/TradingFastPath.kt` -> `app/src/main/kotlin/app/fast/handlers/OrderSubmissionHandler.kt` -> `app/src/main/kotlin/app/order/OrderExecutionService.kt` -> `GatewayClient`
- ローカルFast Path（任意）: `app/src/main/kotlin/app/engine/Router.kt` -> `app/src/main/kotlin/app/fast/FastPathEngine.kt`
  - 永続化: `app/src/main/kotlin/app/fast/PersistenceQueue.kt` -> `app/src/main/kotlin/app/kafka/ChronicleQueueWriter.kt` -> `app/src/main/kotlin/app/kafka/KafkaBridge.kt`
  - 即時マッチ: `app/src/main/kotlin/app/http/MarketDataController.kt` -> `app/src/main/kotlin/app/fast/OrderBook.kt`
- マーケットデータAPI: `app/src/main/kotlin/app/http/MarketDataController.kt`
- WebSocket板配信: `app/src/main/kotlin/app/http/WebSocketController.kt` -> `app/src/main/kotlin/app/http/MarketDataController.kt`
- ポジションAPI: `app/src/main/kotlin/app/http/PositionController.kt` -> `app/src/main/kotlin/app/clients/backoffice/BackOfficeClient.kt`
- 戦略設定API: `app/src/main/kotlin/app/http/StrategyController.kt` -> `app/src/main/kotlin/app/strategy/StrategyConfigService.kt` -> `app/src/main/kotlin/app/strategy/StrategyConfigStore.kt`（Postgres）
- ヘルス/メトリクス: `app/src/main/kotlin/app/http/HealthController.kt` / `app/src/main/kotlin/app/http/MetricsController.kt`

### 2.4 HttpIngress のルーティング全体像（app側）

```mermaid
flowchart TB
  Ingress[HttpIngress]

  Ingress -->|/events| EventsHandler[Events Handler]
  Ingress -->|/stats| Stats[StatsController]
  Ingress -->|/health| Health[HealthController]
  Ingress -->|/metrics| Metrics[MetricsController]
  Ingress -->|/api/admin| Admin[AdminController]

  Ingress -->|/api/market-data| MarketData[MarketDataController]
  Ingress -->|/api/orders| OrderCtl[OrderController]
  Ingress -->|/api/positions| PositionCtl[PositionController]
  Ingress -->|/api/strategy| StrategyCtl[StrategyController]
  Ingress -->|/ws/market-data| WsCtl[WebSocketController]
  Ingress -->|/| StaticCtrl[StaticFileController]

  OrderCtl --> ExecSvc[OrderExecutionService]
  ExecSvc --> GatewayClient[GatewayClient]
  GatewayClient --> Gateway[Gateway HTTP]

  PositionCtl --> BackOfficeClient[BackOfficeClient]
  PositionCtl --> MarketData

  WsCtl --> MarketData

  GatewaySse[GatewaySseClient] --> OrderCtl
  Strategy[StrategyAutoTrader] --> ExecSvc
  Strategy --> MarketData
  StrategyCtl --> StratSvc[StrategyConfigService]
  Strategy --> StratSvc
  StratSvc --> StratStore[StrategyConfigStore]
  StratStore --> AppDb[(Postgres)]
```

---

## 3. 業務フロー（金融ドメイン視点）

```mermaid
sequenceDiagram
  actor Trader as トレーダー
  participant App as トレーディングアプリ
  participant ES as Event Switchyard<br/>(Exec Gateway + SOR)
  participant Ex as 取引所API
  participant BO as バックオフィス
  participant Comp as コンプライアンス

  Note over Trader,Comp: 受理(ACK)と約定(Execution)は分離

  Trader->>App: 注文入力（BTC 指値）
  App->>ES: 新規注文送信
  ES-->>App: 202 Accepted(orderId)

  Note over ES: Fast Path（SLO対象）<br/>受理〜キュー投入まで

  ES->>Ex: 注文送信
  Ex-->>ES: 約定 / 部分約定 / 拒否（Execution Report）

  ES-->>App: WS/SSEで約定通知（orderId相関）
  App-->>Trader: 約定結果表示

  Note over ES: Async Path（監査・後処理）

  ES->>BO: Kafkaで約定・注文更新送信
  BO->>Comp: 監査・報告用データ連携
```

---

## 4. アクター別責務

```mermaid
graph TB
  subgraph FO[フロントオフィス]
    Trader[トレーダー]
    App[TradingApp]
  end

  subgraph ESYS[Execution Gateway（中核）]
    Gw[HttpGateway]
    Svc[OrderService]
    Risk[PreTradeRisk]
    FPQ[(FastPath Queue)]
    FP[FastPathEngine]
    SOR[SOR]
    ER[ExecReportHandler]
    Audit[(Chronicle Queue)]
    Bus[(Kafka/Event Log)]
    SSE[SSE Hub]
  end

  subgraph EXCH[外部]
    Ex[Exchange API]
  end

  subgraph BOFF[バックオフィス]
    BO[BackOffice]
    Comp[Compliance]
  end

  Trader --> App --> Gw --> Svc --> Risk
  Risk -->|OK| FPQ --> FP --> SOR --> Ex --> ER
  Risk -->|NG| App
  ER --> Audit
  ER --> Bus --> BO --> Comp
  ER --> SSE --> App
```

---

## 5. Fast Path 判定ルール（最小）

```mermaid
flowchart TD
  A[Order Received] --> B{PreTradeRisk OK?}
  B -- No --> X[Reject]
  B -- Yes --> C{FastPath Eligible?}

  C -->|Limit Order<br/>Qty <= Threshold<br/>Low Queue Depth| FP[FastPath]
  C -->|Otherwise| SP[Slow Path]

  FP --> D[Low Latency Send]
  SP --> E[Deferred / Throttled]
```

---

## 6. 注文ライフサイクル（状態遷移）

```mermaid
stateDiagram-v2
  [*] --> 受信
  受信 --> リスクチェック
  リスクチェック --> 却下
  リスクチェック --> 受理

  受理 --> 送信済
  送信済 --> 部分約定
  送信済 --> 約定
  部分約定 --> 約定

  受理 --> 取消
  送信済 --> 取消

  約定 --> 監査記録
  監査記録 --> バックオフィス連携
  バックオフィス連携 --> [*]

  却下 --> [*]
  取消 --> [*]
```

---

## 7. 監査証跡（append-only）と再構成（replay）

```mermaid
graph TB
  O[Order Accepted]
  X[Execution Report]

  AQ[(Append-only Audit Log)]
  Replay[Replay / State Rebuild]

  K[(Kafka/Event Log)]
  BO[BackOffice]
  Comp[Compliance]

  O --> X --> AQ --> Replay
  X --> K --> BO --> Comp
```

---

## 8. リアルタイムリスク（Pre / Post 分離）

```mermaid
flowchart LR
  App --> ES

  ES --> PR[Pre-Trade Risk]
  PR -->|OK| ACK[202 Accepted]
  PR -->|NG| REJ[Reject]

  ES --> Ex[Exchange]
  Ex --> ER[Execution Report]

  ER --> PTR[Post-Trade Risk]
  PTR --> Alert[Alerts]
  Alert --> RM[Risk Manager]
```

---

## 9. 性能SLO

- **SLO-FAST-001**: `Gateway受信 -> OrderService/Risk -> FastPath Queue投入` の p99
- **SLO-EXEC-001**: `取引所送信 -> Execution Report受信` は外部依存のため別枠（参考値）
- **SLO-ASYN-001**: `Execution Report受信 -> Audit append` の p99（監査の詰まり検知）

```mermaid
flowchart TD
  A[Gateway recv] --> B[OrderService / Risk]
  B --> C[FastPath enqueue]
  C --> D[Send to Exchange]
  D --> E[Execution report]
  E --> F[Audit append]
  E --> G[Kafka publish]

  subgraph SLO_FAST[SLO-FAST-001]
    A --> B --> C
  end

  subgraph SLO_ASYNC[SLO-ASYN-001]
    E --> F
  end
```

---

## 10. フロント配信のセキュリティ要件

- TLS終端: Nginx/ALBでTLSを終端し、HSTSを有効化する。
- セキュリティヘッダ: CSP、X-Frame-Options、X-Content-Type-Options、Referrer-Policy、Permissions-Policy を明示する。
- 認証/認可: UIはJWT(Bearer)を前提。Cookie利用時は SameSite/HttpOnly/Secure を必須化する。
- 配信分離: バケットは非公開、公開は Nginx/CloudFront のみ。署名URL/Origin Access Control を前提。
- ルーティング: /api と /ws は同一オリジンで中継し、CORS依存を避ける。
- 監視/防御: WAF/Rate Limit/アクセスログの整備。異常検知をアラート化する。
- キャッシュ: index.html は短TTL、assets は長TTL + ハッシュ付きでキャッシュ破棄を明確化する。

## 11. 通しフロー（実装ベースの可視化）

```mermaid
flowchart LR
  subgraph Sync[同期パス（ACKまで）]
    In[HTTP /orders] --> Risk[Pre-Trade Risk]
    Risk -->|OK| Q[(FastPath Queue)]
    Risk -->|NG| Reject[Reject 4xx]
    Q --> Ack[202 Accepted]
  end

  subgraph Async[非同期パス（執行/監査/配布）]
    Q --> FP[FastPathEngine]
    FP --> Ex[Exchange / Sim]
    Ex --> ER[ExecutionReport]
    ER --> Audit[Audit Log]
    ER --> SSE[SSE: execution_report]
    ER --> Bus[Kafka Bus]
    Bus --> BO[BackOffice Consumer]
    BO --> Ledger[(Ledger)]
    BO --> Store[Positions/Balances]
  end

  subgraph Ops[運用/復旧]
    Store --> Reconcile[/reconcile/]
    Store --> Stats[/stats/]
    Ledger --> Replay[Replay on boot]
    Recovery[recovery scripts] --> Stats
    Recovery --> Reconcile
  end
```

要点
- 同期境界は `ACKまで`。その後は必ず非同期で進む。
- Audit Log が「正（source of truth）」で、BackOffice は Bus を起点に Ledger に積み上げる。
- 復旧は `Replay → Reconcile → PASS/FAIL` の順で正しさを確認する。

## 12. 負荷想定と検証方針（実務目線）

目的別の検証（何を見るか）
- 受理性能（同期境界）: `ACKまでのp99` を守れるか。Fast Path enqueue までを計測。
- 執行遅延（非同期）: `送信→ExecutionReport受信` の分布。外部要因と切り分け。
- BackOffice整合: `replay→reconcile` が PASS すること。正しさが最優先。
- 配信安定性: SSE/Kafka の drop/lag を観測。継続稼働で劣化しないか。

アーキ別の目安（概算の粒度）
- Gateway(ACK): 1k / 10k / 100k orders で p99 と 503 率を見る。
- SSE: 同時接続数（1k/5k/10k）と keepalive で切断率を見る。
- Kafka: publish/sec と lag（1k/5k/10k ev/s）を観測。
- BackOffice: consumer で 1h ソーク（50〜100 ev/s）し、reconcile の一致を確認。

判断（何を切り詰めるか）
- ACK p99 が落ちるなら: 非同期へ逃がす/キュー容量と拒否条件を明確化。
- Kafka lag が膨らむなら: best-effort の範囲を明記、BackOffice を優先復旧。
- reconcile がズレるなら: まず冪等/重複排除と replay の設計を修正。

## 13. インフラ拡張の将来候補

- 状態共有: InMemory を永続ストアへ（RDB/KV/オブジェクト）移行。
- 分散運用: Gateway/BackOffice の水平スケール（K8s/Service/LB）。
- 高速キャッシュ: Redis（セッション・idempotency・短期参照）。
- 観測基盤: Prometheus/Grafana の本番運用とアラート整備。
- DR/バックアップ: スナップショットと復元の自動化。

---

## ソース理解の読む順（短縮版）

1. `app/src/main/kotlin/app/http/HttpIngress.kt`（Appの入口と依存の全体像）
2. `app/src/main/kotlin/app/http/OrderController.kt`（注文の受付→Gateway連携）
3. `gateway/src/main/kotlin/gateway/http/HttpGateway.kt`（Gateway API入口）
4. `gateway/src/main/kotlin/gateway/engine/FastPathEngine.kt`（執行・SSE通知・監査）
5. `backoffice/src/main/kotlin/backoffice/Main.kt`（BackOffice起動・リプレイ）
6. `backoffice/src/main/kotlin/backoffice/kafka/BackOfficeConsumer.kt`（台帳更新の源）
7. `backoffice/src/main/kotlin/backoffice/ledger/FileLedger.kt`（台帳・リプレイ/差分）

---

## Security/Refactor TODO（要整理）

セキュリティ要件（実運用レディの不足分）:
- 認証・認可: JWT/mTLS、アカウント単位の権限分離
- 通信暗号化: HTTPS/TLS終端、セキュリティヘッダ
- CORS/CSRF: Origin制限、CSRFトークン
- レート制限: IP/アカウント単位、リクエストサイズ上限
- 入力検証: 注文パラメータの厳格チェック、許可値の限定
- ネットワーク分離: Gatewayのみ公開、BackOffice/DBは内部
- 秘密情報管理: 秘密鍵/DBパスワードの保護・ローテ
- 監査ログ保全: 改ざん耐性（ハッシュチェーン/署名）
- 監視/アラート: 認証失敗・不正アクセス・異常検知

リファクタ（可読性/運用性）:
- `gateway/src/main/kotlin/gateway/risk/SimplePreTradeRisk.kt` の引数肥大 → RiskConfigで集約
- `gateway/src/main/kotlin/gateway/http/HttpGateway.kt` の依存束ね → GatewayDeps/Builder化
- `gateway/src/main/kotlin/gateway/order/OrderService.kt` の依存束ね → OrderDeps化
- `gateway/src/main/kotlin/gateway/Main.kt` の組み立て責務 → Module/Factory化
- 環境変数パースの分散 → `RiskConfig.fromEnv()` / `StrategyConfigDefaults` で集約

