# 証券業務・実装理解ハンドブック

更新日: 2026-03-29

## この文書の目的

この文書は、金融知識がゼロでも、次を一続きで理解できるようにするための正本である。

- 証券会社の利用者が、注文ボタンの先で何を期待しているか
- 証券業務で何が本当に難しいのか
- なぜ OMS、BackOffice、Risk、Ops を分けるのか
- この repo で、その判断がどこに実装されているか

目標は、トップティア外資フロント水準の設計レビューや業務説明の場で、単なる単語暗記ではなく、

- 利用者の言葉
- 業務の言葉
- 実装の言葉

をずらさずに話せる状態である。

この文書は、用語集ではない。順番に読めば、

1. 金融の基礎
2. 1本の注文で起きること
3. なぜこのアーキテクチャなのか
4. どこが壊れやすいのか
5. どう説明すれば「分かっている」と言えるのか

までをつなぐことを狙う。

## まず、この repo の守備範囲

この repo は、教育用・業務理解用として次を重視している。

- 現物株の注文ライフサイクル
- 受注から OMS、BackOffice、Ops までの front-to-back の流れ
- 順序ずれ、replay、pending orphan、DLQ、reconcile
- 利用者画面で最終結果を説明する final-out

逆に、意図的に簡略化しているものもある。

- 実際の清算・決済の全工程
- 税金、手数料、法人口座の複雑な会計
- 証券貸借、空売り規制、borrow / locate
- 企業アクション
- 複数通貨、金利商品、クレジット商品の本格評価

つまり、この repo は「証券会社のすべて」ではない。  
しかし、注文業務、台帳、運用、リスク、説明責務の核心は十分に学べる。

## 金融知識ゼロから最初に握るべきこと

### 1. 証券会社は、何を仲介しているのか

利用者は、証券会社のアプリで「株を買う」「株を売る」と考える。  
しかし実際には、証券会社は次の役割を束ねている。

- 利用者の注文を受ける
- その注文が受けられるか事前チェックする
- 取引所や外部市場に発注する
- 約定結果を利用者へ返す
- 現金、保有株、損益、監査証跡を更新する
- 障害や順序ずれが起きたときに整合を取り戻す

利用者から見ると「株を買えたか」がすべてだが、システム側では

- 受けてよいか
- どこへ出すか
- 何株約定したか
- 残りはどうするか
- 残高と保有をどう確定するか

という別々の論点がある。

### 2. 利用者が本当に見ているもの

利用者は API やキューを見ていない。見ているのは、次である。

- 注文が受け付けられたか
- いくらで何株買えたか
- 残りはまだ待っているのか
- 取り消せるのか
- 口座残高はいくら減ったか
- 保有株数はいくつ増えたか
- 失敗したなら、なぜ失敗したのか

したがって、システム設計の中心は「注文を送ること」ではなく、

- 注文の生存期間を追えること
- 状態を最後まで説明できること
- 台帳上の結果と結びつけられること

になる。

### 3. 最初に覚えるべき用語

#### 注文

利用者が「この銘柄を、この条件で買いたい / 売りたい」と指定する意思。

重要なのは、

- 銘柄
- 売買区分
- 数量
- 価格条件
- 執行条件

である。

#### 成行

価格を固定せず、今市場で取れる相手とすぐに約定しに行く注文。  
「とにかく早く執行したい」意図に向くが、約定価格の不確実性は高い。

#### 指値

これより悪い価格では執行したくない、という価格条件付き注文。  
価格は守るが、約定しない可能性がある。

#### 約定

売り手と買い手がマッチして、実際に取引が成立したこと。  
「注文を出した」ことと「約定した」ことは別。

#### 残数量

注文数量のうち、まだ約定していない部分。  
取消は、この残数量に対して行う。

#### 余力

利用者が新たに取引に使える資金や許容量。  
この repo ではまず、注文拘束としての reservation を学ぶ。

#### reservation

注文を受けた瞬間に、使いすぎを防ぐため一時的に拘束する数量や金額。  
これは portfolio margin と同義ではない。

#### position

口座が今どれだけ保有しているか。  
買い約定なら増え、売り約定なら減る。

#### realized PnL

実際に約定・決済された結果として確定した損益。

#### unrealized PnL

まだ確定していない評価損益。保有中の値洗い差分。

### 4. 「注文」「約定」「残高変動」は別物

最初にここを切り分けないと、全体が分からなくなる。

#### 注文が通る

- 受理された
- working になった
- partial fill した
- cancel された

これは OMS の世界である。

#### 残高が変わる

- cash が減る / 増える
- position が増える / 減る
- realized PnL が確定する
- ledger に記録される

これは BackOffice の世界である。

accepted の段階で cash を確定させると危ない。  
なぜなら、まだ「約定していない」からである。

### 5. 実務で本当に大事な時間軸

金融では、「今ボタンを押した」だけでは足りない。最低でも次の時間軸を分けて考える。

- 送信した時点
- 受理された時点
- 約定した時点
- 台帳が更新された時点
- 法的な決済が完了する時点

この repo は教育用に、主に

- 受理
- 約定
- 台帳反映

を中心に扱う。  
実務ではさらに settlement、照合、ファイナンス計上が続く。

## 取引の舞台裏: 市場と注文の基本

### 1. 取引所と板

現物株は通常、取引所やそれに準ずる venue に出される。  
そこには

- いくらで買いたい人がいるか
- いくらで売りたい人がいるか

の並び、つまり板がある。

よく出てくる概念は次である。

- bid
  - 買いたい価格
- ask
  - 売りたい価格
- spread
  - bid と ask の差
- depth
  - その価格帯にどれだけ数量がいるか

### 2. priority

多くの市場では、基本は price-time priority である。

- まず価格が良い注文が優先
- 同価格なら先に並んだ注文が優先

このため、cancel / replace は単なる値変更ではない。  
並び順、つまり queue position を失うことがある。

### 3. partial fill は例外ではない

初心者は「100株出したら100株執行される」と考えがちだが、現実には違う。

- 40株だけ約定
- 残り60株は待機
- その後さらに 20 株だけ約定

は普通に起こる。

だから、注文状態と台帳状態を別管理にする必要がある。

### 4. best execution は「最安値だけ」ではない

強い説明では、best execution を単なる価格最適化として語らない。

見るべきなのは、

- 価格
- 執行速度
- 約定可能性
- 市場インパクト
- 手数料
- venue 特性

である。

「価格だけ良いが執行できない」より、「少し見劣りしても執行確度が高い」方が良い場合がある。

## 1本の注文で何が起きるか

以下では、現物買い注文を例に、利用者・業務・実装の3層を一気通貫で見る。

### シナリオA: 現物買いの成行注文

前提:

- 利用者は 7203 を 100 株買いたい
- 口座には十分な買付余力がある
- 市場は開いている

#### 利用者の頭の中

利用者は「買う」を押したら、こう思っている。

- すぐ通ってほしい
- どの価格で買えたか知りたい
- 残高が減って保有が増えてほしい

#### システムで実際に起きること

1. フロントから注文要求が来る
2. gateway が形式・認証・受理条件を見る
3. 受理できるなら受ける
4. OMS が注文状態を accepted / working に進める
5. reservation が拘束される
6. venue から fill が返る
7. OMS が filled に進む
8. BackOffice が cash / position / realized PnL / ledger を更新する
9. app-java が final-out として束ねて返す

#### 利用者に見せるべき順序

利用者や運用担当に説明するときは、次の順に読む。

1. 注文 status
2. timeline
3. fills
4. reservation の拘束と解放
5. cash delta
6. position 変化
7. raw event

いきなり cash を見に行くと、物語が崩れる。

#### 実装アンカー

- 注文集約
  - `/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/http/OrderApiHandler.java`
- OMS の順序制御
  - `/Users/fujii/Desktop/dev/event-switchyard/oms-java/src/main/java/oms/audit/GatewayAuditIntakeService.java`
- BackOffice の台帳反映
  - `/Users/fujii/Desktop/dev/event-switchyard/backoffice-java/src/main/java/backofficejava/audit/GatewayAuditIntakeService.java`

### シナリオB: 指値注文が一部だけ約定する

前提:

- 利用者は 100 株の買い指値を置く
- 板にはその価格で 30 株しかいない

起きること:

- 30 株だけ fill
- 残り 70 株は still working
- reservation は縮小
- ledger は 30 株分だけ進む

ここで重要なのは、「注文」と「会計」が違う速度で進むことだ。

- 注文の残り 70 株はまだ生きている
- cash / position は 30 株分だけ動く

つまり、partial fill は terminal ではない。  
しかし ledger は先に進む。

弱い説明:

- 一部約定です

強い説明:

- 注文のライフサイクルはまだ継続中だが、経済効果は fill 分だけ先に確定している

### シナリオC: 残りを取り消す

取消は「注文を消す」操作ではない。  
残数量を止める操作である。

partial fill 後の cancel では、次が競合する。

- 先に cancel complete が来るか
- 先に最後の fill が来るか

ここで aggregateSeq を見ずに apply すると壊れる。

壊れ方の例:

- すでに fill された分まで消したように見える
- reservation release が二重になる
- position と order state が食い違う

だからこの repo では、

- sequence gap は pending orphan に保留
- 解釈不能や不整合は DLQ

と分けている。

### シナリオD: 余力不足で拒否される

拒否は「何か半分だけ残る」状態であってはならない。

正しい拒否とは、

- 利用者に理由が説明される
- OMS に working order が残らない
- BackOffice に cash / position が動かない
- audit だけは残る

ことである。

強い説明では、reject を単なる HTTP 失敗として語らない。  
「何を残し、何を残さないか」を言える必要がある。

### シナリオE: venue timeout や不確定状態

実務では、「通ったのか落ちたのか確信が持てない」状態が一番危険である。

たとえば、

- venue 送信後に応答が来ない
- 回線が一瞬切れた
- cancel の結果が返らない

がある。

ここでやってはいけないのは、勝手に「たぶん失敗」と決めつけて再送することだ。

なぜなら、

- 実は通っていた
- 再送で二重発注になる

可能性があるからである。

この repo が採っている考え方は、

- 不確定状態を明示する
- replay / catch-up で事実を集める
- 必要なら operator judgement を残す

である。

## この repo のアーキテクチャ

### 全体像

この repo は、大きく4つに分けている。

- `gateway-rust`
  - 受理
  - hot path
  - venue control
  - outbox / audit
- `oms-java`
  - 注文状態の正本
  - reservation
  - aggregateSeq
  - pending orphan / DLQ
- `backoffice-java`
  - fill 起点の台帳
  - balances
  - positions
  - realized PnL
- `app-java`
  - final-out
  - ops overview
  - mobile 学習 API

### なぜ 1 個のサービスにまとめないのか

よくある誤りは、「全部 1 サービス、1 DB に入れれば簡単」と考えることだ。  
短期的にはそう見えるが、業務説明と障害切り分けが壊れやすい。

この repo は、次を意図して分離している。

- 速く受ける責務
- 注文状態を収束させる責務
- 会計・台帳を確定する責務
- 利用者や運用に説明する責務

これらは同時に重要だが、同じ場所でやると壊れ方が読みにくい。

### 採った設計

#### 1. hot path は Rust に寄せる

理由:

- 受理と venue control は速さと単純さが重要
- 業務照会や replay logic を同居させると、責務が濁る

#### 2. 業務状態は Java 側に寄せる

理由:

- OMS / BackOffice / Ops は、説明責務と再計算責務が重い
- sequence、orphan、DLQ、reconcile を可視化しやすい

#### 3. OMS と BackOffice を分ける

理由:

- OMS は注文の物語
- BackOffice は経済結果と台帳

だからである。

#### 4. outbox / audit / bus を分ける

理由:

- durable emission
- 説明責務
- consumer 配信

の故障モードが違うからである。

#### 5. final-out で利用者向けに束ねる

理由:

- 業務正本は分離されていてよい
- ただし、利用者や運用には 1 枚で見せる必要がある

からである。

### 意図的に採らなかった設計

#### 1. accepted で ledger を動かす設計

採らない理由:

- まだ約定していない
- reject / timeout / cancel との整合が崩れる

#### 2. 届いた順に event を apply する設計

採らない理由:

- out-of-order は現実に起こる
- fill-first、cancel-first で projection が壊れる

#### 3. 「exactly-once があるから順序や再送を考えなくてよい」という考え方

採らない理由:

- 実務では consumer 再開、再送、operator requeue がある
- order identity と event identity は別

#### 4. 不確定状態を自動で握りつぶす設計

採らない理由:

- 二重発注
- 表示と監査の乖離
- 人判断の痕跡消失

につながるからである。

#### 5. reservation を margin と呼ぶ設計

採らない理由:

- reservation は operational control
- margin は risk model

であり、前提が違うからである。

## 証券業務で絶対に混同してはいけないもの

### 1. reservation と margin

reservation は、注文受理時に「今この注文を受けてよいか」を見る operational control である。  
margin は、ポートフォリオ全体のリスク前提を使う risk model である。

違いは次である。

| 項目 | reservation | margin |
| --- | --- | --- |
| 主目的 | 受理可否の即時制御 | 口座全体のリスク説明 |
| 使う前提 | 注文金額、数量、簡易ルール | 相関、ボラ、保有期間、信頼水準 |
| 時間軸 | 注文直前・直後 | 継続的なリスク評価 |
| 失敗すると困ること | 過剰発注 | リスクの見誤り |

### 2. accepted と filled

accepted:

- 受けた
- まだ経済効果は確定していない

filled:

- 実際に執行された
- cash / position / realized PnL の根拠になる

### 3. realized と unrealized

realized:

- 確定済み
- 売却や約定結果に紐づく

unrealized:

- 保有資産の時価評価差
- まだ確定していない

### 4. replay と reconcile

replay:

- 入力イベントを再供給すること

reconcile:

- 正本同士の差分を調べ、説明すること

同じではない。

### 5. pending orphan と DLQ

pending orphan:

- 前提イベント待ち
- 順序問題
- 待てば解ける可能性が高い

DLQ:

- 解釈不能
- 不整合
- 人が見るべき失敗

## 実務で差が出る、入り組んだ論点

ここから先は、単語を知っているだけでは足りない。  
業務を本当に理解しているかが表れやすい論点である。

### 1. cancel は「消しゴム」ではない

弱い理解:

- cancel すると注文がなくなる

強い理解:

- cancel は残数量に対する操作
- すでに入った fill は消えない
- cancel complete と last fill は race する

### 2. amend は単なる update ではない

価格や数量を変えると、venue では queue position や優先順位に影響する。  
だから、この repo では単純書き換えより cancel-replace 的に扱う。

見るべきファイル:

- `/Users/fujii/Desktop/dev/event-switchyard/gateway-rust/src/server/http/orders/classic.rs`
- `/Users/fujii/Desktop/dev/event-switchyard/gateway-rust/src/exchange/control.rs`

### 3. order identity と event identity を分ける

同じ注文に紐づいて、複数の event が発生する。

- accepted
- partial fill
- fill
- cancel requested
- cancel completed

だから、

- order_id
- client_order_id
- eventId
- aggregateSeq

を混ぜてはいけない。

### 4. offset が進んでいても、業務状態が正しいとは限らない

Kafka やログの offset だけ見て「追いついている」と言うのは弱い。

見るべきは次である。

- offset
- aggregate progress
- pending orphan
- DLQ
- reconcile issue

offset は「読んだ位置」にすぎず、projection が正しく収束した保証ではない。

### 5. audit trail は、単なるログではない

監査で必要なのは、

- 誰が
- いつ
- 何を送って
- 何が返って
- どの表示がその結果なのか

を後から辿れることである。

したがって、UI に見えていることと raw event が対応しなければならない。

### 6. 運用で一番怖いのは「分からないのに進める」こと

現実の障害で危ないのは、完全 failure より ambiguous state である。

たとえば、

- 発注は出たかもしれない
- cancel は通ったかもしれない
- fill は一部入っているかもしれない

この「かもしれない」を勝手に成功/失敗に丸めると事故になる。

強い設計は、

- 不確定状態を露出する
- replay / reconcile / operator action を分ける
- 再送より前に事実確認の導線を持つ

をやる。

## 役割ごとに、何を見ているか

### 利用者

気にすること:

- 注文が通ったか
- いくらで何株買えたか
- 残数量
- 残高と保有
- エラー理由

嫌うこと:

- ステータスが分からない
- 約定したのに残高が変わらない
- cancel 済みなのにまだ動く

### トレーダー / セールストレーダー

気にすること:

- fill rate
- slippage
- queue position への影響
- child order の効き方
- cancel / replace の副作用

嫌うこと:

- parent / child の物語が見えないこと
- arrival price からのずれを説明できないこと

### リスク担当

気にすること:

- gross / net
- symbol limit
- concentration
- notional
- kill switch
- reservation と margin の境界

嫌うこと:

- 受理ルールと risk model が混ざること
- reject 理由が曖昧なこと

### 運用担当

気にすること:

- pending orphan
- DLQ
- aggregateSeq gap
- replay 成否
- reconcile issue
- operator intervention の痕跡

嫌うこと:

- 自動復旧と人判断の境界が曖昧なこと
- 「再起動したらなんとなく直った」しか言えないこと

### BackOffice / ファイナンス

気にすること:

- fill と cash delta の対応
- reservation release
- positions
- realized PnL
- ledger entry

嫌うこと:

- accepted ベースで数字が動くこと
- 台帳と注文履歴が結びつかないこと

### 監査 / コンプライアンス

気にすること:

- 誰がいつ何を送ったか
- cancel / replace 履歴
- どの raw event を根拠に表示しているか
- operator action の記録

嫌うこと:

- UI 表示はあるが、裏付けが無いこと
- 後から再構成できないこと

## この repo で実装を見る順番

### 1. 利用者に見せる最終結果

- `/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/http/OrderApiHandler.java`
- `/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/order/OrderService.java`

ここでは final-out の組み立てを見る。  
利用者に何をどう見せるかの入口である。

### 2. 注文状態の収束

- `/Users/fujii/Desktop/dev/event-switchyard/oms-java/src/main/java/oms/audit/GatewayAuditIntakeService.java`
- `/Users/fujii/Desktop/dev/event-switchyard/oms-java/src/main/resources/db/migration/V3__oms_aggregate_progress.sql`

ここでは、

- aggregateSeq
- pending orphan
- DLQ
- replay

を確認する。

### 3. 台帳の収束

- `/Users/fujii/Desktop/dev/event-switchyard/backoffice-java/src/main/java/backofficejava/audit/GatewayAuditIntakeService.java`
- `/Users/fujii/Desktop/dev/event-switchyard/backoffice-java/src/main/java/backofficejava/ledger`

ここでは、

- fill 起点の ledger
- reservation release
- cash / position / realized PnL

を確認する。

### 4. hot path の境界

- `/Users/fujii/Desktop/dev/event-switchyard/gateway-rust/src/outbox`
- `/Users/fujii/Desktop/dev/event-switchyard/gateway-rust/src/server/http/orders/classic.rs`
- `/Users/fujii/Desktop/dev/event-switchyard/gateway-rust/src/exchange/control.rs`
- `/Users/fujii/Desktop/dev/event-switchyard/contracts/bus_event_v2.schema.json`

ここでは、

- どこで durable emission するか
- どこまでを hot path として守るか
- Java 側へどの contract で渡すか

を見る。

### 5. 学習用の mobile 表現

- `/Users/fujii/Desktop/dev/event-switchyard/frontend/src/components/mobile/MobileOrderStudyView.tsx`
- `/Users/fujii/Desktop/dev/event-switchyard/frontend/src/components/mobile/MobileArchitectureView.tsx`
- `/Users/fujii/Desktop/dev/event-switchyard/frontend/src/components/mobile/MobileCardsView.tsx`
- `/Users/fujii/Desktop/dev/event-switchyard/frontend/src/offline/mobileOfflineStore.ts`

ここでは、業務説明をどの順序で見せるかを見る。

## この repo の過不足

### いま強い部分

現時点の repo は、次の理解にはかなり強い。

- 現物株の注文ライフサイクル
- accepted と filled の違い
- OMS と BackOffice の責務分離
- reservation と台帳確定の違い
- aggregateSeq、pending orphan、DLQ、replay、reconcile
- final-out を使った利用者向け説明
- hot path と業務説明責務の分離

言い換えると、

- 1 本の注文がどう生まれ
- どう約定し
- どう残高と保有に反映され
- どこで壊れ
- どう運用で戻すか

はかなり追える。

### まだ不足している部分

一方で、次はまだ十分ではない。

- 市場構造の厚み
  - auction
  - halt
  - LULD
  - dark / lit venue
  - fee / rebate
  - arrival price、implementation shortfall、TCA
- 機関投資家フロー
  - parent / child order
  - care order
  - algo wheel
  - allocation
  - block / average price 配分
- post-trade の厚み
  - clearing
  - settlement
  - fail 管理
  - fee / tax
  - statement / confirm
  - books and records
- risk の厚み
  - liquidity risk
  - stress governance
  - backtesting
  - scenario design
  - margin methodology
  - factor / concentration の深掘り
- multi-asset
  - futures
  - FX
  - rates
  - credit
  - cross-asset の valuation / settlement 差分
- production engineering breadth
  - FIX session
  - market data normalization
  - drop copy
  - throttling
  - entitlement
  - rollout / incident / capacity

つまり、この repo は「現物株の front-to-back を深く理解する」にはかなり良いが、  
「市場構造、機関投資家フロー、post-trade、multi-asset まで含めた広い実務地図」としてはまだ足りない。

## 次に repo に取り込む優先順位

ここでは、追加する順番を固定する。  
目的は、「新しい知識を増やす」ことではなく、「既存の注文・台帳・運用の骨格に、実務の厚みを順に重ねる」ことである。

### 優先度 1: 市場構造と執行品質

最初に厚くするべきなのはここである。  
理由は、現物株の注文フロー理解を最も強くし、既存の OMS / final-out / mobile 表現と一番噛み合うからだ。

追加すべき論点:

- 板の厚みと一部約定の関係
- queue position
- spread と market impact
- arrival price
- slippage
- implementation shortfall
- IOC / FOK の業務意図
- venue 状態変化
  - halt
  - auction
  - LULD

repo に入れるべきもの:

- mobile に `市場構造` 画面を追加する
- order 画面に
  - arrival price
  - fill quality
  - slippage
  - remaining quantity の意味
  を追加する
- replay scenario に
  - gap-up open
  - halt
  - auction reopen
  - thin liquidity
  を追加する

この優先度で増やすファイル:

- `frontend/src/components/mobile/MobileMarketStructureView.tsx` を新設
- `frontend/src/components/mobile/MobileOrderStudyView.tsx` を拡張
- `frontend/src/offline/mobileOfflineStore.ts` に market structure cards / drills を追加

### 優先度 2: 機関投資家フロー

次に入れるべきは、注文を「個人のワンタップ」から「執行判断」へ広げる層である。

現状:

- [x] mobile に執行判断画面を追加
- [x] parent / child order、participation、care / DMA、allocation の教育用モデルを追加
- [x] app-java に live / on-device 両対応の `institutional-flow` API を追加

追加すべき論点:

- parent / child order
- participation rate
- schedule-based execution
- care order
- DMA と high-touch の違い
- average price 配分
- block execution

repo に入れるべきもの:

- mobile に `執行設計` 画面を追加する
- child order と parent progress を可視化する
- `TWAP / VWAP / POV` を業務意図から説明する
- fill rate、slippage、残数量戦略の判断カードを追加する

この優先度で増やすファイル:

- `frontend/src/components/mobile/MobileInstitutionalFlowView.tsx`
- `frontend/src/offline/mobileRoadmapOffline.ts`
- `app-java/src/main/java/appjava/mobile/MobileRoadmapService.java`

### 優先度 3: post-trade とファイナンス

注文と約定だけでは不十分で、業務ではその後も続く。  
ここを押さえると「システムは約定したのに、なぜまだ仕事が終わらないのか」が理解しやすくなる。

現状:

- [x] mobile に post-trade 画面を追加
- [x] execution / allocation / clearing / settlement / books and records の教育用 timeline を追加
- [x] fee / tax / statement preview / corporate action hook を追加
- [x] app-java に `post-trade` API を追加

追加すべき論点:

- trade date と settlement date
- clearing
- fail
- fee / tax
- confirm / statement
- books and records
- exception 処理

repo に入れるべきもの:

- mobile に `post-trade` 画面を追加する
- timeline を
  - order
  - execution
  - ledger
  - settlement
  の 4 層に分けて見せる
- fee / tax / confirm を教育用に追加する

この優先度で増やすファイル:

- `frontend/src/components/mobile/MobilePostTradeView.tsx` を新設
- `app-java/src/main/java/appjava/mobile/MobileRoadmapService.java`
- `frontend/src/offline/mobileRoadmapOffline.ts`

### 優先度 4: risk の厚み

今の risk は入口としては良いが、数字の前提を深く語るにはまだ足りない。

現状:

- [x] mobile risk に scenario library、concentration、liquidity、backtesting preview を追加
- [x] risk 画面に model boundary を追加
- [x] app-java に `risk/deep-dive` API を追加

追加すべき論点:

- historical VaR とその限界
- stress scenario 設計
- concentration
- liquidity risk
- factor exposure
- backtesting
- reservation と margin methodology の違い

repo に入れるべきもの:

- mobile risk に
  - scenario library
  - backtest explanation
  - liquidity assumption
  - concentration narrative
  を追加する
- 単なる数字ではなく
  - 何を省略しているか
  - どう誤るか
  を説明するカードを増やす

この優先度で増やすファイル:

- `frontend/src/components/mobile/MobileRiskView.tsx` を拡張
- `frontend/src/offline/mobileRoadmapOffline.ts`
- `app-java/src/main/java/appjava/mobile/MobileRoadmapService.java`

### 優先度 5: multi-asset 化

この repo は現時点で equities に強い。  
次は「何が共通で、何が asset class ごとに変わるか」を扱う。

現状:

- [x] mobile に asset class 比較画面を追加
- [x] equities / options / FX / rates / credit / futures の比較モデルを追加
- [x] app-java に `asset-classes` API を追加

追加すべき論点:

- futures
- FX spot / forward
- rates
- credit
- option 以外の非線形商品
- valuation / settlement / risk driver の違い

repo に入れるべきもの:

- mobile に `asset class 比較` 画面を追加する
- 同じ order lifecycle の骨格と、商品固有の差分を並べる
- equities の考え方をそのまま持ち込むと壊れる点を明示する

この優先度で増やすファイル:

- `frontend/src/components/mobile/MobileAssetClassView.tsx` を新設
- `frontend/src/offline/mobileRoadmapOffline.ts`
- handbook に asset class 別章を追加

### 優先度 6: 本番運用工学

最後に必要なのは、業務知識と実装知識を production engineering に繋ぐ層である。

現状:

- [x] mobile に運用工学画面を追加
- [x] session monitor、incident drill、schema control、capacity control を追加
- [x] app-java に `operations` API を追加

追加すべき論点:

- FIX session
- drop copy
- market data normalization
- schema evolution
- throttling
- entitlement
- rollout
- incident response
- capacity / latency / failure budget

repo に入れるべきもの:

- mobile に `運用障害` 画面を追加する
- incident scenario drill を作る
- order / ledger だけでなく session / feed / operator action の故障を扱う

この優先度で増やすファイル:

- `frontend/src/components/mobile/MobileOperationsView.tsx`
- `frontend/src/offline/mobileRoadmapOffline.ts`
- `docs/ops` に session / incident handbook を追加
- `scripts/ops` に incident replay drill を追加

## 最短で価値が出る実装順

全部を一気に広げるのではなく、次の順で入れると価値が高い。

1. 市場構造
2. 機関投資家フロー
3. post-trade
4. risk の厚み
5. multi-asset
6. 本番運用工学

この順の理由は単純である。

- 既存の equities front-to-back を一番強くするのが市場構造
- その次に、注文を execution judgement に広げるのが機関投資家フロー
- その後で、注文後の業務を閉じるのが post-trade
- risk と multi-asset は、その骨格の上に積む方が理解しやすい
- 本番運用工学は、全体の骨格が入ってから追加した方が意味が通る

## 何を知っているべきか

最初から知っておくべき項目は次である。

- 注文タイプの業務意図
- 板、スプレッド、厚み、partial fill
- queue priority と cancel-replace の意味
- best execution と slippage
- accepted、working、filled、canceled、expired の違い
- reservation と margin の違い
- gross / net / concentration / kill switch
- cash、position、ledger、realized / unrealized
- aggregateSeq、pending orphan、DLQ、replay、reconcile
- audit trail と operator action
- option payoff、delta、gamma、theta、vega の直感
- asset class ごとに何が増え、何が変わるか

## 「理解している」と言える説明

次を、図なしでも、相手が金融に詳しくなくても、60 秒から 120 秒で言える必要がある。

### 1. 注文フロー

利用者が注文する。Gateway が受理し、OMS が注文状態を管理する。約定が来たら BackOffice が fill 起点で ledger を確定する。利用者には final-out で timeline、fills、reservation release、cash delta、position を束ねて見せる。

### 2. 取消フロー

cancel は残数量に対する操作であり、すでに入った fill は消えない。fill と cancel complete は race するので、aggregateSeq を見て apply 順を守る。最終的に reservation release まで確認して初めて説明が閉じる。

### 3. 障害フロー

event は out-of-order になる前提で設計する。前提待ちは pending orphan、解釈不能は DLQ に分ける。replay は入力再供給、reconcile は truth 差分確認であり、同じではない。不確定状態を勝手に成功・失敗に丸めない。

### 4. リスクフロー

reservation は注文拘束であり、margin とは別物である。risk 数字は必ず前提付きで読み、データ期間、信頼水準、保有期間、省略点を説明する。線形商品の PnL と option の payoff / Greeks は同じ言葉で雑にまとめない。

## 学習のしかた

### 1. まず利用者の言葉で話す

- 何をしたかったか
- 何が見えたか
- 何が不安か

を言えるようにする。

### 2. 次に業務の言葉で話す

- accepted か filled か
- 残数量はあるか
- reservation はどうなったか
- ledger は何を根拠に動いたか

を言えるようにする。

### 3. 最後に実装の言葉で話す

- どの service が持つか
- どの event が根拠か
- どの file に境界があるか

を言えるようにする。

## 進捗チェック

次が自力で説明できれば、この repo の業務理解としてはかなり強い。

- なぜ OMS と BackOffice を分けるのか
- なぜ accepted で ledger を動かさないのか
- partial fill 後の cancel がなぜ難しいのか
- pending orphan と DLQ がなぜ必要か
- replay と reconcile の違いは何か
- final-out はどう読むべきか
- reservation と margin はどう違うか
- hot path を守りつつ業務説明責務を後段に寄せる理由は何か

## この文書の使い方

順番は固定でよい。

1. この文書を読む
2. `/mobile` の注文、台帳、判断カードを見る
3. final-out を 1 件ずつ追う
4. aggregateSeq / pending orphan / DLQ を実装で読む
5. 自分の言葉で 90 秒説明を作る

分からない単語があったら、その単語だけを追うのではなく、

- 利用者に何が見えるか
- 業務で何が確定したか
- 実装でどこが truth か

の順に戻ること。
