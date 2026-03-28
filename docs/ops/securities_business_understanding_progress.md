# 証券業務理解ハンドブック

更新日: 2026-03-29

## 位置づけ

この文書は、単なる用語メモではなく、次を一気通貫で握るためのハンドブックである。

- 証券口座の利用者が何を期待しているか
- フロント業務で何が重要な判断になるか
- OMS / BackOffice / Risk / Ops をどう切り分けるか
- この repo のどこを読めば、その判断が実装として見えるか

目標水準は、トップティア外資フロント業務の設計レビューで

- 業務フローを曖昧にせず
- リスクと運用を分けて話し
- どこが真実のソースで、どこが投影なのか

を短く言い切れる状態である。

## この repo をどう使うか

学習順は次で固定する。

1. 利用者の注文が何を期待しているかを理解する
2. 注文状態の収束を理解する
3. 約定後の台帳と残高の収束を理解する
4. 順序ずれ、再送、保留、DLQ を理解する
5. 最後に risk と option を理解する

この順を崩すと、数字だけ追って業務の物語を失う。

## 最初に握るべき前提

### 1. 注文は「送ったら終わり」ではない

利用者が見ているのは送信ボタンではなく、次の結果である。

- 受け付けられたか
- いくら残っているか
- 何株約定したか
- 取り消せるか
- 最終的に残高と保有はどう変わったか

つまり、利用者が本当に欲しいのは `注文送信 API` ではなく、`注文の生存期間を最後まで追えること` である。

### 2. 「注文が通った」と「残高が変わる」は別の話

ここを混同すると設計が壊れる。

- 注文が通る
  - OMS の話
  - accepted / working / terminal の話
- 残高が変わる
  - BackOffice の話
  - fill / fee / cash / position / realized PnL の話

accepted で cash を動かすのは危険であり、fill で ledger を起こすのが基本になる。

### 3. 速く受けることと、正しく説明できることは別の設計課題

`gateway-rust` の責務は hot path を守って速く受けること。  
一方で業務説明、再計算、reconcile、ops は Java 側で持つ。

この repo はその境界を見せるために次の分離を採っている。

- `gateway-rust`
  - 受理
  - hot path
  - venue control
  - outbox / audit
- `oms-java`
  - 注文状態
  - reservation
  - aggregateSeq
  - pending orphan / DLQ
- `backoffice-java`
  - ledger
  - balances
  - positions
  - realized PnL
- `app-java`
  - final-out
  - ops overview
  - mobile 学習 API

## 証券口座の利用者目線での業務シナリオ

### シナリオ1: 現物買いの成行注文

利用者の期待:

- ボタンを押したらすぐ受け付けてほしい
- いくらで何株買えたか見たい
- 残高と保有株数が正しく変わってほしい

システムで起きること:

- Gateway が注文を受理する
- OMS が accepted にする
- venue / replay から fill が返る
- BackOffice が cash と position を更新する
- UI は final-out で timeline と台帳影響を見せる

重要な説明:

- accepted と filled は違う
- 約定前は reservation、約定後に cash / position が確定

### シナリオ2: 指値注文を出して待つ

利用者の期待:

- 指定価格より悪い価格では約定しない
- 待機中であることが見える
- 一部だけ約定した場合、残数量が分かる

システムで起きること:

- OMS は working 状態を持つ
- partial fill が入る
- remaining quantity が減る
- reservation も縮小する

重要な説明:

- partial fill は terminal ではない
- ledger は fill 分だけ先に進み、残数量は注文状態として残る

### シナリオ3: 残りを取り消す

利用者の期待:

- まだ約定していない分だけ止めてほしい
- 取消済みになったらもう約定しないでほしい
- 拘束していた余力が戻ってほしい

システムで起きること:

- cancel request
- venue cancel / cancel-replace
- OMS terminal 化
- reservation release

重要な説明:

- cancel 前に入った fill と cancel 完了は競合しうる
- だから sequence を見て apply 順を守る

### シナリオ4: 余力不足で拒否される

利用者の期待:

- なぜ拒否されたか分かること
- 変な半端状態が残らないこと

システムで起きること:

- risk / reservation 前提で受理前に reject
- OMS / BackOffice に半端な truth を残さない

重要な説明:

- 拒否時は「何も起きていない」ように見えることが正しい
- ただし audit は残す

### シナリオ5: 価格急変や取引所状態変化

利用者の期待:

- エラーならエラーとして整然と返ること
- 不確定な状態のまま勝手に再送されないこと

システムで起きること:

- venue reject
- timeout
- loss suspect
- replay / catch-up / operator judgement

重要な説明:

- 不確定状態は「自動で握りつぶさない」
- operator / strategy 側の再判断余地を残す

## 業務を握るために最低限知っておくべきルール

### 注文ルール

- 成行は価格を指定しないが、約定価格の責任が消えるわけではない
- 指値は価格を守る代わりに未約定が残る
- IOC / FOK は執行意図の違いであり、単なる API パラメータではない
- cancel は「存在しなかったことにする」操作ではなく、残数量に対する操作
- amend は新規注文ではなく、既存注文の生存期間の延長線上にある

### 市場構造ルール

- 板は price-time priority が基本
- partial fill は例外ではなく常態
- queue position は暗黙の価値であり、cancel / replace はそれを失い得る
- best execution は「とにかく最安値・最高値」ではなく、約定可能性、速度、コスト、影響を含む

### リスクルール

- reservation は注文拘束であり、margin そのものではない
- gross / net / concentration / kill switch は別々に意味を持つ
- risk 数字は必ず前提付きで読む
  - どのデータか
  - どの保有期間か
  - どの信頼水準か
  - 何を省略しているか

### 台帳ルール

- accepted では ledger を確定しない
- fill 起点で cash / position / realized PnL を動かす
- unrealized PnL は評価差であり、確定損益ではない
- reservation release は fill または cancel / expire に紐づいて説明できなければならない

### 運用ルール

- offset と aggregate progress は別
- pending orphan は「待てば解ける順序問題」
- DLQ は「人が介入すべき失敗」
- replay は入力を再供給すること
- reconcile は truth 同士の差分を説明すること

## 役割ごとに、何を見ているか

### 口座利用者

見るもの:

- 注文が通ったか
- 何株約定したか
- 平均価格
- 残数量
- 残高と保有

嫌うもの:

- 状態が分からない
- 約定したのに残高が変わらない
- 取消済なのに動く

### トレーダー / セールストレーダー

見るもの:

- fill rate
- slippage
- arrival price からのずれ
- remaining quantity
- cancel / replace の効き方

嫌うもの:

- child order の物語が見えないこと
- parent progress が曖昧なこと

### リスク担当

見るもの:

- gross / net
- concentration
- symbol limit
- notionals
- kill switch
- reservation と risk model の切り分け

嫌うもの:

- 受理系と risk model の言葉が混ざること
- どの時点で reject したのか不明なこと

### 運用担当

見るもの:

- pending orphan
- DLQ
- aggregateSeq gap
- replay の成否
- reconcile issue

嫌うもの:

- どこからどこまでが自動復旧で、どこからが人判断か曖昧なこと

### BackOffice / ファイナンス担当

見るもの:

- cash delta
- reservation release
- positions
- realized PnL
- ledger entry

嫌うもの:

- accepted ベースで数字が動くこと
- fill と台帳の紐づけが切れていること

### コンプライアンス / 監査

見るもの:

- 誰がいつ何を送ったか
- cancel / replace の履歴
- どのイベントを根拠に表示したか
- operator 介入の履歴

嫌うもの:

- UI には見えるが監査の根拠がないこと
- raw event と表示結果の対応が取れないこと

## この repo で本当に見るべきファイル

### 注文の見え方

- `app-java/src/main/java/appjava/http/OrderApiHandler.java`
  - final-out の入口
- `app-java/src/main/java/appjava/order/OrderService.java`
  - 注文集約の見え方

### 注文状態の収束

- `oms-java/src/main/java/oms/audit/GatewayAuditIntakeService.java`
  - aggregateSeq
  - pending orphan
  - replay
- `oms-java/src/main/resources/db/migration/V3__oms_aggregate_progress.sql`
  - どこまで apply 済か

### 台帳の収束

- `backoffice-java/src/main/java/backofficejava/audit/GatewayAuditIntakeService.java`
  - fill 反映
  - orphan / DLQ
- `backoffice-java/src/main/java/backofficejava/ledger/*`
  - ledger の形

### front-to-back の見え方

- `frontend/src/components/mobile/MobileOrderStudyView.tsx`
  - timeline
  - balance / ledger
- `frontend/src/components/mobile/MobileArchitectureView.tsx`
  - 運用と境界
- `frontend/src/components/mobile/MobileCardsView.tsx`
  - 判断基準の反復

### hot path を守る境界

- `gateway-rust/src/outbox`
- `gateway-rust/src/server/http/orders/classic.rs`
- `gateway-rust/src/exchange/control.rs`

## 「理解している」と言えるための説明項目

次を、図なしで 60 秒から 120 秒で言える必要がある。

### 注文フロー

- 利用者が注文する
- Gateway が受理する
- OMS が注文状態を持つ
- 約定が来る
- BackOffice が ledger を確定する
- UI は final-out でそれを束ねる

### 取消フロー

- cancel request は残数量に対する操作
- fill との race がある
- sequence を見て apply 順を守る
- reservation release を確認する

### 障害フロー

- event は out-of-order になりうる
- pending orphan と DLQ を分ける
- replay と reconcile の違いを言う
- 自動再開しない理由を言う

### risk フロー

- reservation と margin を分ける
- historical VaR の前提を言う
- option payoff と Greeks を線形商品と対比して言う

## 何を知っているべきか

次は「あとで」ではなく、最初から意識して学ぶべき項目である。

- 注文タイプごとの業務意図
- 板、スプレッド、厚み、一部約定
- best execution の考え方
- slippage と benchmark
- gross / net / concentration / kill switch
- ledger と PnL の違い
- realized / unrealized
- pending orphan / DLQ / replay / reconcile
- audit trail と operator action
- option payoff、delta、gamma、theta、vega の直感
- asset class ごとの違い
  - equities
  - FX
  - rates
  - credit

## 学習のしかた

### 1. まず利用者の言葉で説明する

「何株買えたか」「残りはどうなったか」「お金はいつ動くか」で話す。

### 2. 次に業務の言葉で説明する

accepted、working、partial fill、cancel、fill、ledger、reconcile で話す。

### 3. 最後に実装の言葉で説明する

outbox、aggregateSeq、pending orphan、DLQ、projection、final-out で話す。

この順番を逆にしない。

## 現時点の進捗チェック

- [x] 注文の生存期間を説明できる
- [x] OMS と BackOffice の境界を説明できる
- [x] reservation と margin を分けて説明できる
- [x] pending orphan と DLQ の違いを説明できる
- [x] final-out の読み順を説明できる
- [x] option payoff と Greeks の入口を説明できる
- [ ] best execution と benchmark の説明をさらに厚くする
- [ ] asset class ごとの差分を具体例付きで厚くする
- [ ] fee / tax / settlement / corporate action の整理を足す
- [ ] repo の実装アンカーを card / drill 側へさらに反映する

## この文書の使い方

- mobile を見る前に 1 回通す
- mobile のカードで詰まったら該当節へ戻る
- 新しいカードや drill を足すときは、まずこの文書に業務説明を書く

この順で、実装メモではなく業務理解の土台として使う。
