# Mini Exchange v0.1

## 目的
板・約定・price-time priority・slippage・cancel の意味を、最小実装で理解する。
この仕様は、実務未経験でも市場構造と執行の芯を掴むための学習用ミニ取引所を対象とする。

## スコープ
今回入れるもの:
- 1銘柄のみ
- BUY / SELL
- LIMIT / MARKET
- price-time priority
- partial fill
- cancel
- last price
- 板スナップショット出力
- 約定ログ出力
- 単一スレッド

今回入れないもの:
- 複数銘柄
- 口座残高管理
- 手数料
- 信用取引
- 清算 / 決済
- 永続化
- ネットワーク
- GUI
- 高度なリスク制御
- 複雑な注文種別（IOC/FOK/Stop など）

## 想定ユースケース
ユーザは注文を投入し、その結果として:
- 板にどう並ぶか
- どの順番で約定するか
- 成行でどの価格まで食うか
- cancel で何が消えるか
を観察する。

## 用語
- 板: 未約定注文の並び
- 買い板: 買い注文の集合。高い価格が優先
- 売り板: 売り注文の集合。安い価格が優先
- 約定: 売買成立
- last price: 直近約定価格
- price-time priority: 価格優先、同価格なら時刻優先
- partial fill: 注文の一部だけ約定すること

## 入力形式
各注文は以下の属性を持つ。

```text
order_id: str
side: BUY | SELL
type: LIMIT | MARKET
price: int | None
qty: int
ts: int
```

制約:
- LIMIT のときは price 必須
- MARKET のときは price は None
- qty は正の整数
- ts は単調増加を前提にしてよい
- order_id は一意

## 出力
最低限、以下を出力する。

### 1. 約定ログ
```text
TRADE buy_order_id sell_order_id price qty
```

### 2. 板スナップショット
```text
BIDS
99: 100
98: 300

ASKS
100: 50
101: 200
```

### 3. 現在値
```text
LAST_PRICE: 100
```

### 4. 注文受付 / cancel 結果
```text
ACCEPT order_id
CANCELLED order_id
REJECT order_id reason
```

## 板のデータ構造
板は以下の概念を満たす必要がある。

### 買い板
- 高い価格が先に評価される
- 同価格帯では FIFO

### 売り板
- 安い価格が先に評価される
- 同価格帯では FIFO

推奨表現:
```text
bids: price -> queue[order]
asks: price -> queue[order]
```

ここで重要なのは、単なる `price -> total_qty` ではなく、
**同価格帯の注文順序を保持すること**。

## 約定ルール
### 共通
- 反対板と価格条件が合えば約定する
- 約定価格は、先に板に載っていた resting order 側の価格を採用する
- 約定数量は `min(残数量, 反対注文残数量)`
- 一部のみ約定した場合、残数量を保持する
- 数量が 0 になった注文は板から削除する

### LIMIT BUY
次の条件を満たす間、売り板にぶつける:
- 売り板が存在する
- 最良売り価格 <= 自注文 price

ぶつけ終わって残数量があれば、買い板に載せる。

### LIMIT SELL
次の条件を満たす間、買い板にぶつける:
- 買い板が存在する
- 最良買い価格 >= 自注文 price

ぶつけ終わって残数量があれば、売り板に載せる。

### MARKET BUY
売り板の最良価格から順に食う。
売り板が尽きるか、自注文残数量が 0 になるまで続ける。
未約定残が残っても、板には載せず失効させる。

### MARKET SELL
買い板の最良価格から順に食う。
買い板が尽きるか、自注文残数量が 0 になるまで続ける。
未約定残が残っても、板には載せず失効させる。

## cancel ルール
入力:
```text
cancel(order_id)
```

仕様:
- 未約定残がある注文のみ cancel 対象
- 板上に存在すれば削除
- すでに全量約定済みなら cancel 不可
- 存在しない order_id は reject

出力例:
```text
CANCELLED O123
REJECT O999 NOT_FOUND
```

## reject ルール
最低限以下は reject する。

- LIMIT なのに price がない
- MARKET なのに price がある
- qty <= 0
- 重複 order_id
- 存在しない order_id の cancel

## last price の定義
- 直近の約定価格
- 約定がまだ一度もなければ None

## 板表示ルール
- BIDS は価格降順
- ASKS は価格昇順
- 同価格帯の内部順序は板表示では省略可
- ただしテストでは FIFO を別途確認する

## テストケース
最低限以下を通す。

### Case 1: LIMIT が板に載る
1. BUY LIMIT 99 x 100
期待:
- 約定なし
- bids に 99:100 が載る

### Case 2: 成行買いが最安売りから食う
初期:
- SELL LIMIT 100 x 50
- SELL LIMIT 101 x 80
入力:
- BUY MARKET x 100
期待:
- 100 で 50 約定
- 101 で 50 約定
- last price = 101
- 101 に 30 残る

### Case 3: 同価格 FIFO
初期:
- BUY LIMIT 99 x 100 (A, ts=1)
- BUY LIMIT 99 x 100 (B, ts=2)
入力:
- SELL MARKET x 150
期待:
- A が先に 100 約定
- B が次に 50 約定

### Case 4: partial fill 後に板に残る
初期:
- SELL LIMIT 100 x 200
入力:
- BUY LIMIT 100 x 50
期待:
- 50 約定
- 売り 100 に 150 残る

### Case 5: cancel 成功
初期:
- BUY LIMIT 99 x 100 (O1)
入力:
- cancel(O1)
期待:
- 板から削除
- CANCELLED O1

### Case 6: 存在しない cancel
入力:
- cancel(NO_SUCH_ORDER)
期待:
- REJECT

### Case 7: 指値が反対板にぶつかる
初期:
- SELL LIMIT 100 x 100
入力:
- BUY LIMIT 101 x 60
期待:
- 100 で 60 約定
- 売り 100 に 40 残る

## 実装方針
推奨クラス:
- Order
- PriceLevel
- OrderBook
- MatchingEngine

最低限の責務:
- Order: 注文情報と残数量
- OrderBook: 板管理
- MatchingEngine: 受付、約定、cancel、出力

## 完了条件
以下を満たしたら v0.1 完了とする。

- LIMIT / MARKET / cancel が動く
- 約定ログが出る
- last price が更新される
- price-time priority がテストで確認できる
- partial fill が再現できる
- thin book で成行が複数価格帯を食うことを再現できる

## 次バージョン候補
v0.2 で追加候補:
- queue position 可視化
- thin book 指標
- slippage 集計
- cancel latency シミュレーション
- market data 遅延
- risk reject
