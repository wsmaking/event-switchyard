# AGENTS.md

目的
- このリポの「やったこと」「今の状態」「残タスク」「目標」を引き継ぎ可能にする。
- 次スレッドでも同じ理解で再開できるように要点を固定する。

目標（ゴールのアーキ）
- Execution Gateway / SOR（取引所ではない）。
- 同期境界は ACK (HTTP 202) まで。約定は非同期（SSE/Kafka）。
- 監査ログは append-only、replay 可能。
- BackOffice 台帳と復旧チェックで“正しさ”を証明する。

現在の状態（要約）
Gateway
- HTTP API: orders, cancel, order/account events。
- FastPathEngine が SOR 経由で sim/tcp に送信し、audit + bus + SSE を発行。
- Audit log: JSON Lines、replay、events API。
- SSE: order/account stream、replay buffer、resync_required fallback。
- Kafka: best-effort（KAFKA_ENABLE=1）、drop/queue metrics。
- Metrics: accept->sent / accept->execution_report の遅延。

BackOffice
- Kafka consumer が ledger と positions/balances を更新。
- Ledger: append-only file + 起動時 replay。
- HTTP APIs: /positions, /balances, /fills, /pnl, /ledger, /stats, /reconcile。
- 重複排除: OrderAccepted は既存 orderId を無視、Fill は filledQtyTotal 差分。
- Reconcile: ledger 期待値 vs store 実値を比較。

Gateway Rust（gateway-rust）
- Rust実装の注文ゲートウェイ。Kotlin Gateway互換を目指したHTTP API + 低レイテンシTCP受信。
  - 現状のHTTPは /orders, /orders/{id}, /orders/{id}/cancel, /orders/{id}/events, /orders/{id}/stream, /accounts/{id}/events, /stream, /health, /metrics。
- FastPathEngine: risk check → queue へ投入、レイテンシ計測。
- ShardedOrderStore + OrderIdMap でアカウント単位の高速検索。
- SSEはbroadcastベース + リングバッファでリプレイ/リシンク対応。
- Exchange連携はJSONLのTCPクライアント（KotlinのTCP Exchange互換）。
- Kafka送信は outbox 経由（`BUS_MODE=outbox`）。macOSは `PKG_CONFIG_PATH=/opt/homebrew/lib/pkgconfig` が必要。
- Kafkaキーは `order_id` を優先し、無ければ `account_id`。キー単位の順序性を維持。
- outboxはKafka送信成功時のみオフセットを進め、失敗時はその行で停止する。
- outboxはバックオフで再試行し、上限回数は持たない（`OUTBOX_BACKOFF_MAX_MS`で上限待ち時間を制御）。オフセットは fsync で保護し、破損時はバックアップに退避。
- 監査ログは `AUDIT_HMAC_KEY` 設定時にハッシュチェーンを併記し `/audit/verify` で検証可能。KMS（`AUDIT_KMS_KEY_ID`）で復号も可能。
- 監査アンカー: `/audit/anchor` でチェーンルート/最新ハッシュをスナップショット化し外部保管に備える。

運用スクリプトとショートカット
- scripts/backoffice_recovery_check.sh
  - /health, /stats, /positions, /balances, /ledger, /reconcile を一括確認
  - 停滞検知 (BACKOFFICE_STALE_SEC)
  - JWT自動生成 (JWT_HS256_SECRET + BACKOFFICE_ACCOUNT_ID)
  - PASS/FAIL (BACKOFFICE_PASSFAIL=1)
- scripts/backoffice_replay_verify.sh
  - ledger replay 起動 → /reconcile で整合確認
- scripts/gateway_backoffice_e2e.sh
  - 注文受理 → orderId/status 検証 → 待機 → recovery check
- Makefile
  - make backoffice-recovery
  - make gateway-backoffice-e2e
  - make perf-gate-rust
- scripts/ops/wrk_gateway_rust.sh: wrkベースのRust計測（throughput/latency両対応）

リポ内ルール
- docs/arch/*.md と docs/arch/system_design_interview_questions.txt はローカル用途。
- docs/arch/* はコミットしない（.gitignore で *.md が除外）。
- 仕様変更は contracts/anchor_spec_v1.txt に書いてコミットする。

ブランチ運用
- main 最新から切る。
- 機能ごとにコミット → PR → 早めにマージ。
- docs/arch の追記は未コミットのまま。

進捗（概算）
- 全体: 約 75%（正しさ/運用を詰めた段階）。

大手金融レベルに寄せる残タスク（優先順）
1) 正しさ/原子性
   - outbox/WAL による send + audit の原子性
   - cancel/exec の厳密な冪等・順序保証
2) BackOffice 永続化
   - InMemory から永続ストアへ
   - snapshot/compaction で replay 時間の上限を保証
3) 監査の改ざん耐性
   - ハッシュチェーン/署名/外部保管
   - 監査ログのアクセス制御
4) セキュリティ/統制
   - JWT鍵ローテーション、aud/iss/clock skew、RBAC/ABAC
5) HA/DR
   - multi-AZ、RPO/RTO、バックアップ/復元自動化
6) 観測/SLO
   - アラート基準、runbook 本番化
7) スケール
   - SSE/Kafka/Queue の負荷試験
   - BusEvent/ledger の schema versioning

最短案（戦略 = 旧app / 執行 = Gateway+BackOffice）
- 旧appは注文生成に集中（Disruptorチェーン）。
- 旧appの永続化は無効化し、正本は Gateway/BackOffice に統一。
- 旧app → Gateway への注文送信アダプタを追加。

実装理解の読み順（最短）
1) gateway/http/HttpGateway.kt
2) gateway/order/OrderService.kt + gateway/risk/PreTradeRisk.kt + gateway/queue/FastPathQueue.kt
3) gateway/engine/FastPathEngine.kt + exchange simulator + SOR
4) gateway/audit/* + gateway/http/SseHub.kt + gateway/kafka/KafkaEventPublisher.kt
5) backoffice/Main.kt + kafka/BackOfficeConsumer.kt + ledger/* + http/HttpBackOffice.kt
6) scripts/*（recovery / replay verify / e2e）
7) gateway-rust/src/main.rs + server/http.rs + engine/fast_path.rs + server/tcp.rs
8) gateway-rust/src/store/* + sse/mod.rs + exchange/*

補足
- docker-compose の Kafka は 1 broker / 1 partition（topic-init の PARTITIONS=1）。
- Compose は単一ホスト運用。マルチホストは K8s/ECS/Nomad が必要。

処理の流れ（実装ベース）
Gateway (Kotlin)
- HTTP POST /orders → OrderService.acceptOrder → InMemoryOrderStore へ Accepted 保存
  → FastPathQueue enqueue → 202 Accepted 返却
- FastPathEngine が queue を消費し OrderSent を監査/Bus/SSE に発行
  → Exchange(SOR→Sim/TCP) へ送信
- ExecutionReport 受信 → OrderStore 更新 → 監査/Bus/SSE 発行
- SSEは注文/アカウント単位でバッファリプレイ、古すぎる場合 resync_required
- AuditLogは JSONL append-only → 起動時 replay で OrderStore を復元

BackOffice
- Kafka Consumer が BusEvent を受信
  - OrderAccepted: OrderMeta を ledger + store へ
  - ExecutionReport: filledQtyTotal 差分で Fill を ledger + store へ
- FileLedger は append-only、起動時 replay で store を再構築
- Snapshotter（InMemory時）で state を保存し replay の短縮
- /reconcile が ledger 期待値と store 実値を比較

Gateway Rust
- HTTP POST /orders → JWT auth → FastPathEngine.process_order
  → ShardedOrderStore に snapshot 保存 → 202 Accepted
- TCP server は固定長バイナリで process_order を直接呼ぶ
- Exchange worker が queue から JSONL で取引所へ送信
- SSE は broadcast + リングバッファでリプレイ/リシンク対応
- outbox がKafka送信の成否を管理（成功時のみオフセット前進）

TODO（HFTとして足りない部分: 個人開発で実務耐性を目指す/教材化の観点）
- SLO計測の精度引き上げ: Rustはp95/p999追加済。Kotlin側のヒストグラム化/Micrometer導入は未。
- Perf Gateの自動化: bench/perf_gate をCIに組込み、p99回帰検知とベースライン管理
- Idempotency-Key実装: Rustは対応済。Kotlin側は未。
- キャンセル/状態遷移の現実化: Rustは対応済。Kotlin側は未。
- Audit改ざん耐性の最小版: Rustはハッシュチェーン/HMAC + `/audit/verify` 対応済。Kotlin側/外部保管の自動化は未。
- BackOffice永続化の導入: InMemory→RDB移行、Flywayでスキーマ管理、replay時間の上限設計
- Rate Limit強化: 全体制限→アカウント別トークンバケット化、429率をメトリクス化
- SSE再同期UX強化: resync_required のガイドとクライアントサンプルを追加
- Kafka代替のローカルEventPublisher: ファイル追記+ブロードキャストでBusを模擬
- Rust側の最適化継続: ShardedStore/ID map/Arena/Slabでアロケ抑制、TCP p99検証
- 原子性: accept→audit→send の一体化（outbox/WAL）と再送制御
- 厳密な冪等・順序: cancel/exec の並び保証と重複排除の強化
- 永続化: Gateway/BackOffice のスナップショット/compaction 完備と復旧時間の上限
- 監査の改ざん耐性: 外部保管の自動化 + アクセス制御
- HFT運用の基盤: 時刻同期（NTP/PTP）、時刻ソース監視、clock skew対策
- メッセージング信頼性: Kafka/SSE/queue の backpressure と損失時の再取得戦略
- パフォーマンス検証: p99/p999の再現性、CPU affinity、GC/allocの可視化
- リスク/レート統制: アカウント・銘柄単位の上限/クールダウン/日次制限
- セキュリティ/統制: JWT鍵ローテ/issuer/audience/RBAC、監査ログの保護
- HA/DR: multi-AZ、RPO/RTO、バックアップ/復元自動化（個人規模での簡易版）
