mini-exchange v0.2

目的
- 板、約定、price-time priority、partial fill、cancel の最小実装
- quant / execution policy を評価する前段の学習用・検証用市場
- v0.2 では queue position、thin book 指標、slippage、cancel latency、market data delay、risk reject を追加

使い方
1. script file を使う
   cargo run --manifest-path mini-exchange/Cargo.toml -- --file mini-exchange/examples/v0_2_features_demo.txt

2. stdin で流す
   printf 'SUBMIT S1 SELL LIMIT 100 50 1\nSUBMIT B1 BUY MARKET - 50 2\nBOOK\n' \
     | cargo run --manifest-path mini-exchange/Cargo.toml

コマンド
- SUBMIT <order_id> <BUY|SELL> <LIMIT|MARKET> <price|-> <qty> <ts>
- CANCEL <order_id> [ts]
- ADVANCE <ts>
- BOOK
- BOOK_DELAYED <ts>
- TRADES
- LAST
- QUEUE <order_id>
- METRICS
- SLIPPAGE
- SET_CANCEL_LATENCY <ticks>
- SET_MD_DELAY <ticks>
- SET_RISK <MAX_ORDER_QTY|MAX_MARKET_QTY|MAX_NOTIONAL> <value|NONE>

出力
- ACCEPT <order_id>
- CANCELLED <order_id>
- CANCEL_PENDING <order_id> <apply_at_ts>
- REJECT <order_id> <reason>
- TRADE <buy_order_id> <sell_order_id> <price> <qty>
- BIDS / ASKS / LAST_PRICE

v0.2 追加観測
- QUEUE: 板上 order の queue position と ahead_qty
- METRICS: spread、top depth、thin book heuristic、imbalance
- SLIPPAGE: 成行/marketable order の aggregate slippage

policy 比較 harness
- cargo run --manifest-path mini-exchange/Cargo.toml --example compare_policies
- cargo run --manifest-path mini-exchange/Cargo.toml --example compare_quant_intent
- cargo run --manifest-path mini-exchange/Cargo.toml --example compare_quant_intent -- --format json
- cargo run --manifest-path mini-exchange/Cargo.toml --example compare_quant_intent -- --format csv
- passive は同一制約下で non-crossing な limit へ変換
- aggressive は cap があれば crossing limit、なければ market
- twap は等分 child order
- vwap は weight vector に応じた child order
- pov は opposite top3 depth に participation bps を掛けた child order

quant-clean bridge
- scripts/ops/run_mini_exchange_quant_bridge.sh \
  --intent /Users/fujii/Desktop/dev/event-switchyard-quant-clean/path/to/strategy_intent.json
- default seed は `mini-exchange/examples/seed_book.json`
- `--format text|json|csv`
- `--policies intent|all|PASSIVE,AGGRESSIVE`
- `--output path` で report を保存

batch runner
- scripts/ops/run_mini_exchange_quant_batch.sh \
  --input-dir mini-exchange/examples/quant_clean_batch \
  --output-dir /tmp/mini-exchange-reports
- 各 intent ごとに `txt/json/csv` を保存
- まとめとして `batch_summary.csv` と `policy_summary.csv` を保存

feedback / shadow join
- scripts/ops/run_join_quant_feedback_with_mini_exchange.sh \
  --report-json /tmp/export-aggressive-buy-report.json \
  --feedback-jsonl /tmp/quant-feedback.jsonl \
  --shadow-record-json /tmp/shadow-record.json \
  --shadow-summary-json /tmp/shadow-summary.json \
  --output /tmp/joined-quant-mini.json
- `scenarioId` と `intentId` をキーに、mini-exchange report と quant feedback/shadow を突き合わせる
- `matchedPolicy` が取れれば、その policy の `executedQty` / `restingQty` / `slippage` を feedback 側の actual policy と一緒に読める

feedback / shadow batch join
- scripts/ops/run_join_quant_feedback_with_mini_exchange_batch.sh \
  --reports-dir /tmp/mini-exchange-reports \
  --captures-dir /tmp/quant-gateway-captures/captures \
  --output-dir /tmp/mini-exchange-joined
- 各 scenario ごとの joined JSON と `batch_summary.csv` を保存
- collector の `manifest.json` を読んで scenarioId ごとに capture を引き当てる

gateway capture batch
- quant 側で `scripts/ops/run_quant_gateway_capture_batch.sh` を使うと、intent export と gateway submit/feedback/shadow 回収を一度に回せる
- 出力は `intents/` と `captures/` に分かれ、batch join は `captures/` を入力にする

比較観点
- `executed_qty`: どれだけ即時に約定したか
- `resting_qty`: どれだけ板に残ったか
- `slippage_events` / `total_notional_slippage`: aggressive 系のコスト
- `queuePosition`: passive 系が板のどこに並んだか
- `book_after`: policy 実行後に板がどう変わったか
- `spread` / `top depth` / `thin book`: 市場への影響と残存流動性
- `last_price`: 最終約定価格

読み方
- passive は `resting_qty` と `queuePosition` を重視
- aggressive は `executed_qty` と `slippage` を重視
- twap/vwap/pov は `child_count` と `book_after` の崩れ方を見る
- `DEFAULT` 比較では単一 policy の良し悪しではなく、同じ intent を複数 policy で並べて差分を見る
