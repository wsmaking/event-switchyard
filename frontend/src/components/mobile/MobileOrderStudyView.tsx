import { useState } from 'react';
import {
  useMobileOpsOverview,
  useMobileOrderFinalOut,
  useMobileOrderStream,
  useMobileOrders,
  useRunMobileReplayScenario,
} from '../../hooks/useMobileStudy';
import { OrderSide, OrderType, TimeInForce } from '../../types/trading';
import type { OpsOverview, OrderFinalOut, OrderRequest } from '../../types/trading';
import { formatCurrency, formatDateTime, formatNumber, formatSignedCurrency, statusLabel, statusTone } from './mobileUtils';

interface MobileOrderStudyViewProps {
  focus: 'lifecycle' | 'ledger';
  orderId: string | null;
  onNavigate: (path: string) => void;
}

interface StudyAnchor {
  title: string;
  path: string;
  focus: string;
}

interface StudyBrief {
  intro: string;
  userExpectation: string[];
  systemStory: string[];
  readingOrder: string[];
  chosenDesign: string[];
  rejectedDesign: string[];
  operatorChecks: string[];
  implementationAnchors: StudyAnchor[];
}

const replayScenarios = ['accepted', 'partial-fill', 'filled', 'canceled', 'expired', 'rejected'] as const;

export function MobileOrderStudyView({ focus, orderId, onNavigate }: MobileOrderStudyViewProps) {
  const { data: orders, isLoading, isError, error } = useMobileOrders();
  const runReplayScenario = useRunMobileReplayScenario();
  const [runningScenario, setRunningScenario] = useState<string | null>(null);
  const selectedOrderId = orderId ?? orders?.[0]?.id ?? null;
  const { data: finalOut, isLoading: finalOutLoading } = useMobileOrderFinalOut(selectedOrderId);
  const { data: opsOverview } = useMobileOpsOverview(selectedOrderId);
  const orderStreamState = useMobileOrderStream(selectedOrderId);

  if (isLoading) {
    return <div className="px-4 py-6 text-sm text-[color:var(--mobile-muted)]">注文を読み込み中...</div>;
  }

  if (isError) {
    return (
      <div className="px-4 py-6 text-sm text-rose-200">
        注文一覧を取得できませんでした
        <div className="mt-2 text-xs text-rose-200/80">{error instanceof Error ? error.message : 'unknown_error'}</div>
      </div>
    );
  }

  if (!orders || orders.length === 0) {
    return (
      <div className="space-y-4 px-4 py-5">
        <div className="rounded-[24px] border border-white/10 bg-white/5 p-5">
          <div className="text-lg font-semibold text-white">注文がまだありません</div>
          <div className="mt-2 text-sm leading-6 text-slate-300">
            replay scenario で状態を seed して、注文の物語と台帳影響を学習に使えます。
          </div>
        </div>
        <div className="grid grid-cols-2 gap-3">
          {replayScenarios.map((scenario) => (
            <button
              key={scenario}
              onClick={() => runSeedScenario(scenario, setRunningScenario, runReplayScenario, onNavigate)}
              className="rounded-[22px] border border-white/10 bg-white/5 px-4 py-4 text-left text-sm font-medium text-white"
            >
              {scenario}
            </button>
          ))}
        </div>
        {runningScenario && <div className="text-xs text-slate-400">scenario 起動中: {runningScenario}</div>}
      </div>
    );
  }

  return (
    <div className="space-y-4 px-4 py-5 pb-24">
      <div className="overflow-x-auto pb-1">
        <div className="flex gap-2">
          {orders.slice(0, 8).map((order) => (
            <button
              key={order.id}
              onClick={() => onNavigate(`/mobile/orders/${order.id}`)}
              className={`min-w-[132px] rounded-[22px] border px-4 py-3 text-left ${selectedOrderId === order.id ? 'border-emerald-300/50 bg-emerald-500/10' : 'border-white/10 bg-white/5'}`}
            >
              <div className="text-[11px] text-slate-400">{order.symbol}</div>
              <div className="mt-1 text-sm font-semibold text-white">{statusLabel(order.status)}</div>
              <div className="mt-1 text-[11px] text-slate-400">{formatDateTime(order.submittedAt)}</div>
            </button>
          ))}
        </div>
      </div>

      {finalOutLoading || !finalOut ? (
        <div className="rounded-[24px] border border-white/10 bg-white/5 px-4 py-5 text-sm text-slate-300">
          final-out を読み込み中...
        </div>
      ) : (
        <OrderStudyContent
          focus={focus}
          finalOut={finalOut}
          opsOverview={opsOverview}
          orderStreamState={orderStreamState}
          onNavigate={onNavigate}
        />
      )}
    </div>
  );
}

function OrderStudyContent({
  focus,
  finalOut,
  opsOverview,
  orderStreamState,
  onNavigate,
}: {
  focus: 'lifecycle' | 'ledger';
  finalOut: OrderFinalOut;
  opsOverview: OpsOverview | undefined;
  orderStreamState: string;
  onNavigate: (path: string) => void;
}) {
  const unrealizedPnl = finalOut.positions.reduce((total, position) => total + position.unrealizedPnL, 0);
  const ledgerEntries = opsOverview?.ledgerEntries ?? [];
  const rawEventIndex = new Map(finalOut.rawEvents.map((event) => [event.eventRef, event]));
  const brief = buildOrderStudyBrief(finalOut, opsOverview, focus);

  return (
    <>
      <section className="rounded-[28px] border border-white/10 bg-[linear-gradient(135deg,rgba(15,118,110,0.28),rgba(15,23,42,0.95))] p-5">
        <div className="flex items-start justify-between gap-4">
          <div>
            <div className="text-[11px] uppercase tracking-[0.22em] text-teal-100/70">
              {focus === 'ledger' ? '台帳の流れ' : '注文の流れ'}
            </div>
            <h1 className="mt-2 text-2xl font-semibold text-white">{finalOut.order.symbol}</h1>
            <div className="mt-2 text-sm leading-6 text-slate-200">{brief.intro}</div>
          </div>
          <div className={`rounded-full border px-3 py-1 text-xs font-medium ${statusTone(finalOut.order.status)}`}>
            {statusLabel(finalOut.order.status)}
          </div>
        </div>

        <div className="mt-5 grid grid-cols-3 gap-3">
          <InfoMetric label="数量" value={`${formatNumber(finalOut.order.quantity)} 株`} />
          <InfoMetric label="約定" value={`${formatNumber(finalOut.order.filledQuantity ?? 0)} 株`} />
          <InfoMetric label="配信" value={orderStreamState} />
        </div>
        {orderStreamState === 'offline' && (
          <div className="mt-4 rounded-[18px] border border-teal-300/20 bg-teal-400/10 px-4 py-3 text-xs leading-6 text-teal-50/90">
            on-device pack で再生中。order stream は local storage の state を参照しています。
          </div>
        )}
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-base font-semibold text-white">利用者が見ていること</div>
        <BulletStack items={brief.userExpectation} />
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-base font-semibold text-white">システムで起きていること</div>
        <StepRail steps={brief.systemStory} />
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="flex items-center justify-between">
          <div className="text-base font-semibold text-white">この注文を読む順番</div>
          <button onClick={() => onNavigate(focus === 'ledger' ? '/mobile/orders' : '/mobile/ledger')} className="text-xs font-medium text-emerald-200">
            {focus === 'ledger' ? '注文視点へ' : '台帳視点へ'}
          </button>
        </div>
        <StepRail steps={brief.readingOrder} />
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-base font-semibold text-white">この repo が採った設計</div>
        <BulletStack items={brief.chosenDesign} tone="emerald" />
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-base font-semibold text-white">意図的に取らなかった設計</div>
        <BulletStack items={brief.rejectedDesign} tone="amber" />
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-base font-semibold text-white">運用でまず確認すること</div>
        <BulletStack items={brief.operatorChecks} />
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-base font-semibold text-white">Timeline</div>
        <div className="mt-4 space-y-4">
          {finalOut.timeline.map((entry, index) => (
            <div key={`${entry.eventType}-${entry.eventAt}-${index}`} className="flex gap-3">
              <div className="flex flex-col items-center">
                <div className={`mt-1 h-3 w-3 rounded-full ${index === finalOut.timeline.length - 1 ? 'bg-emerald-300' : 'bg-sky-300'}`} />
                {index < finalOut.timeline.length - 1 && <div className="mt-2 h-full w-px bg-white/10" />}
              </div>
              <div className="flex-1 rounded-[20px] border border-white/8 bg-slate-950/55 px-4 py-3">
                <div className="flex items-start justify-between gap-3">
                  <div className="text-sm font-semibold text-white">{entry.label}</div>
                  <div className="text-[11px] text-slate-500">{formatDateTime(entry.eventAt)}</div>
                </div>
                <div className="mt-2 text-sm leading-6 text-slate-300">{entry.detail}</div>
              </div>
            </div>
          ))}
        </div>
      </section>

      {focus === 'ledger' ? (
        <>
          <LedgerSummary finalOut={finalOut} />
          <PnlSplitCard realizedPnl={finalOut.accountOverview.realizedPnl} unrealizedPnl={unrealizedPnl} />
          <ImpactList
            title="Reservation"
            emptyText="reservation はありません"
            items={finalOut.reservations.map((reservation) => ({
              key: reservation.reservationId,
              title: `${formatNumber(reservation.reservedQuantity)} 株 / ${reservation.status}`,
              body: `拘束 ${formatCurrency(reservation.reservedAmount)} / 解放 ${formatCurrency(reservation.releasedAmount)}`,
            }))}
          />
          <ImpactList
            title="Fills"
            emptyText="fill はありません"
            items={finalOut.fills.map((fill) => ({
              key: fill.fillId,
              title: `${formatNumber(fill.quantity)} 株 @ ${formatCurrency(fill.price)}`,
              body: `notional ${formatCurrency(fill.notional)} / ${formatDateTime(fill.filledAt)}`,
            }))}
          />
          <ImpactList
            title="Positions"
            emptyText="position はありません"
            items={finalOut.positions.map((position) => ({
              key: position.symbol,
              title: `${position.symbol} ${formatNumber(position.quantity)} 株`,
              body: `avg ${formatCurrency(position.avgPrice)} / uPnL ${formatSignedCurrency(position.unrealizedPnL)}`,
            }))}
          />
          <LedgerRail entries={ledgerEntries} rawEventIndex={rawEventIndex} />
        </>
      ) : (
        <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
          <div className="text-base font-semibold text-white">いまの注文で言うべきこと</div>
          <div className="mt-4 grid gap-3">
            <NarrativeCard title="何が起きたか" body={describeLifecycle(finalOut.order.status)} />
            <NarrativeCard title="説明の要点" body={explanationPrompt(finalOut.order.status)} />
            <NarrativeCard
              title="運用観点"
              body={`sequence gap ${opsOverview?.omsStats?.sequenceGaps ?? 0} / pending ${opsOverview?.omsStats?.pendingOrphanCount ?? 0} / dlq ${opsOverview?.omsStats?.deadLetterCount ?? 0}`}
            />
          </div>
        </section>
      )}

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-base font-semibold text-white">実装アンカー</div>
        <AnchorList anchors={brief.implementationAnchors} />
      </section>
    </>
  );
}

function runSeedScenario(
  scenario: string,
  setRunningScenario: (value: string | null) => void,
  runReplayScenario: ReturnType<typeof useRunMobileReplayScenario>,
  onNavigate: (path: string) => void
) {
  const request: OrderRequest = {
    symbol: '7203',
    side: OrderSide.BUY,
    type: OrderType.MARKET,
    quantity: 100,
    price: null,
    timeInForce: TimeInForce.GTC,
    expireAt: null,
  };
  setRunningScenario(scenario);
  runReplayScenario.mutate(
    { scenario, request },
    {
      onSuccess: (order) => {
        setRunningScenario(null);
        onNavigate(`/mobile/orders/${order.id}`);
      },
      onError: () => setRunningScenario(null),
    }
  );
}

function buildOrderStudyBrief(finalOut: OrderFinalOut, opsOverview: OpsOverview | undefined, focus: 'lifecycle' | 'ledger'): StudyBrief {
  const status = finalOut.order.status;
  const gaps = opsOverview?.omsStats?.sequenceGaps ?? 0;
  const pending = (opsOverview?.omsStats?.pendingOrphanCount ?? 0) + (opsOverview?.backOfficeStats?.pendingOrphanCount ?? 0);
  const dlq = (opsOverview?.omsStats?.deadLetterCount ?? 0) + (opsOverview?.backOfficeStats?.deadLetterCount ?? 0);
  const rawEventCount = finalOut.rawEvents.length;

  if (status === 'FILLED') {
    return {
      intro:
        focus === 'ledger'
          ? '注文は全量約定まで進み、reservation の解放と cash / position / realized PnL の確定が final-out に収束している。'
          : '利用者にとっては「買えた」で終わるが、業務では accepted、fill、台帳確定が別々に起きている。',
      userExpectation: [
        'いくらで何株買えたかを知りたい。',
        '残高が減り、保有株数が増えたことを確認したい。',
        '失敗や保留ではなく、本当に終わった注文だと分かりたい。',
      ],
      systemStory: [
        'gateway-rust が注文を受け、hot path で受理境界を守る。',
        'oms-java が accepted / working / filled の注文状態を収束させる。',
        'fill 到着後に backoffice-java が ledger、cash、position、realized PnL を確定する。',
        `app-java が final-out として timeline、fills、balance effect、raw event ${rawEventCount} 件を束ねる。`,
      ],
      readingOrder: [
        'status と timeline で、注文がどう終わったかを見る。',
        'fills で数量と価格を確認する。',
        'reservation の解放と balance effect の符号を確認する。',
        '最後に positions と raw events で説明の裏を取る。',
      ],
      chosenDesign: [
        'accepted と filled を分離し、約定前に会計を確定しない。',
        'OMS と BackOffice を分け、注文状態の収束と台帳確定を別責務にした。',
        'final-out で利用者向けには 1 画面に束ねるが、正本は分けて持つ。',
      ],
      rejectedDesign: [
        'accepted 時点で ledger を動かす設計は取らない。',
        '1 サービスで注文状態と台帳を全部持つ設計は取らない。',
        'raw event なしで UI だけを正本っぽく見せる設計は取らない。',
      ],
      operatorChecks: [
        `OMS gap ${gaps}、pending ${pending}、DLQ ${dlq} を見て、順序破綻や手当待ちがないか確認する。`,
        'timeline の terminal と cash / position の変化が同じ fill を根拠にしているか確認する。',
        'realized と unrealized を混同せず、fill 起点の確定損益だけを realized として読む。',
      ],
      implementationAnchors: [
        anchor('final-out の組み立て', '/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/http/OrderApiHandler.java', 'timeline、fills、balance effect、raw events を 1 枚に束ねる入口'),
        anchor('OMS の順序制御', '/Users/fujii/Desktop/dev/event-switchyard/oms-java/src/main/java/oms/audit/GatewayAuditIntakeService.java', 'accepted / fill / cancel の apply 順を aggregateSeq で守る'),
        anchor('BackOffice の台帳確定', '/Users/fujii/Desktop/dev/event-switchyard/backoffice-java/src/main/java/backofficejava/audit/GatewayAuditIntakeService.java', 'fill 起点で ledger、cash、position を更新する'),
      ],
    };
  }

  if (status === 'PARTIALLY_FILLED') {
    return {
      intro:
        focus === 'ledger'
          ? '一部約定により ledger は先に進んでいるが、注文の残数量はまだ生きている。注文状態と会計状態が違う速度で進む場面。'
          : 'partial fill は terminal ではない。注文はまだ open のまま、経済効果だけが先に一部確定している。',
      userExpectation: [
        '何株だけ約定したのか、残り何株が待機中なのかを知りたい。',
        '指定価格より悪い価格で勝手に全部約定していないことを確認したい。',
        '残数量をこのまま待つか、取り消すか判断したい。',
      ],
      systemStory: [
        'OMS は working order を維持しながら filled quantity と remaining quantity を更新する。',
        'BackOffice は fill 分だけ ledger と cash / position を先に進める。',
        'reservation は残数量に応じて縮小される。',
        'cancel が入る場合、最後の fill と cancel complete の順序競合に備える。',
      ],
      readingOrder: [
        'status を見て、terminal ではなく一部約定であることを確認する。',
        'filled quantity と remaining quantity を並べて読む。',
        'fills と reservation 縮小を確認する。',
        '最後に cancel を入れた場合の race を想像し、sequence を確認する。',
      ],
      chosenDesign: [
        'partial fill を terminal にせず、残数量を持つ OMS を置いた。',
        'ledger は fill 分だけ進め、残数量は注文状態として別に持つ。',
        'aggregateSeq gap は pending orphan に保留し、届いた順 apply を避ける。',
      ],
      rejectedDesign: [
        '一部約定のたびに注文を閉じて新規注文扱いにする設計は取らない。',
        'fill と cancel を arrival order のまま適用する設計は取らない。',
        'reservation をまとめて最後にだけ動かす設計は取らない。',
      ],
      operatorChecks: [
        `pending ${pending} が増えていれば、accepted 未着や cancel race を疑う。`,
        'remaining quantity と reservation の残りが整合しているか確認する。',
        'partial fill 分だけ ledger が進み、未約定分まで cash を確定していないか確認する。',
      ],
      implementationAnchors: [
        anchor('OMS の状態収束', '/Users/fujii/Desktop/dev/event-switchyard/oms-java/src/main/java/oms/audit/GatewayAuditIntakeService.java', 'partial fill と cancel race を含む projection 順序'),
        anchor('Replay scenario', '/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/demo/ReplayScenarioService.java', 'partial-fill scenario の再生入口'),
        anchor('台帳側の fill 反映', '/Users/fujii/Desktop/dev/event-switchyard/backoffice-java/src/main/java/backofficejava/audit/GatewayAuditIntakeService.java', 'fill 分だけ ledger を先に進める'),
      ],
    };
  }

  if (status === 'CANCELED' || status === 'EXPIRED') {
    const terminalLabel = status === 'CANCELED' ? '取消' : '失効';
    return {
      intro:
        focus === 'ledger'
          ? `${terminalLabel} により残数量が止まり、残っていた reservation が release される。fill 済み部分があればそこだけは ledger に残る。`
          : `${terminalLabel} は「存在しなかったことにする」操作ではない。残数量を止め、未使用の拘束を戻す終端である。`,
      userExpectation: [
        '未約定分だけが止まっていることを確認したい。',
        'もうこれ以上勝手に約定しないと分かりたい。',
        '使っていない拘束余力が戻っていることを見たい。',
      ],
      systemStory: [
        'gateway-rust が cancel request もしくは expiry を受け、venue control を走らせる。',
        'OMS が terminal 化し、残数量を close する。',
        'BackOffice は残拘束の release を反映する。',
        'fill 済み部分がある場合は、その分の ledger は消えずに残る。',
      ],
      readingOrder: [
        'timeline で cancel request / cancel complete もしくは expiry を確認する。',
        'filled quantity が 0 かどうかを確認し、既存 fill が残るかを見る。',
        'reservation の releasedAmount と balance effect を確認する。',
        'raw event で cancel race の裏を取る。',
      ],
      chosenDesign: [
        'cancel を残数量への操作として扱う。',
        'venue 側の cancel / cancel-replace を明示し、OMS terminal と台帳 release を分ける。',
        'fill と cancel complete が競合しても sequence で apply 順を守る。',
      ],
      rejectedDesign: [
        'cancel を「注文が最初から無かったことにする」設計は取らない。',
        'fill 済み部分までまとめて巻き戻す設計は取らない。',
        'expiry を UI 上だけの表示変更にする設計は取らない。',
      ],
      operatorChecks: [
        `OMS gap ${gaps}、pending ${pending} を見て cancel race が残っていないか確認する。`,
        'releasedAmount が reservation の残りと整合しているか確認する。',
        'fill 済み部分があるのに会計影響まで消えていないか確認する。',
      ],
      implementationAnchors: [
        anchor('cancel / amend 制御', '/Users/fujii/Desktop/dev/event-switchyard/gateway-rust/src/server/http/orders/classic.rs', 'cancel request、cancel-replace、terminal event の入口'),
        anchor('venue control', '/Users/fujii/Desktop/dev/event-switchyard/gateway-rust/src/exchange/control.rs', '取引所への cancel / amend 伝播'),
        anchor('OMS の終端収束', '/Users/fujii/Desktop/dev/event-switchyard/oms-java/src/main/java/oms/audit/GatewayAuditIntakeService.java', 'cancel complete と fill の順序制御'),
      ],
    };
  }

  if (status === 'REJECTED') {
    return {
      intro:
        focus === 'ledger'
          ? '拒否は会計が動かないことまで含めて正しい。中途半端な reservation や position を残さないのが要件。'
          : '拒否は単なる HTTP 失敗ではない。何を残し、何を残さないかをはっきり決める業務判断である。',
      userExpectation: [
        'なぜ拒否されたかを知りたい。',
        '注文が中途半端に残っていないと分かりたい。',
        '残高や保有が勝手に変わっていないことを確認したい。',
      ],
      systemStory: [
        'gateway-rust もしくは venue が reject reason を返す。',
        'OMS は terminal にするが、working order を残さない。',
        'BackOffice は cash / position / ledger を動かさない。',
        'audit だけは残し、後から説明できるようにする。',
      ],
      readingOrder: [
        'reject reason を確認する。',
        'timeline に working や fill が無いことを確認する。',
        'balance effect が 0 近辺で、台帳影響がないことを確認する。',
        'raw event で reject の根拠を確認する。',
      ],
      chosenDesign: [
        '拒否理由を表に出し、何が起きなかったかまで説明できるようにする。',
        'audit は残すが、OMS / BackOffice の truth は汚さない。',
        '受理系と会計系を分離し、reject を半端な truth にしない。',
      ],
      rejectedDesign: [
        'reject 時に予約や open order を残す設計は取らない。',
        '利用者には generic error だけ返して audit を残さない設計は取らない。',
        'reject と timeout を同じものとして丸める設計は取らない。',
      ],
      operatorChecks: [
        'reject 後に OMS open order や reservation が残っていないか確認する。',
        'BackOffice に ledger entry や cash delta が出ていないか確認する。',
        'audit trail から reject 根拠を辿れるか確認する。',
      ],
      implementationAnchors: [
        anchor('受理と reject の境界', '/Users/fujii/Desktop/dev/event-switchyard/gateway-rust/src/server/http/orders/classic.rs', '受理前 reject と venue reject の入口'),
        anchor('final-out の表示', '/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/http/OrderApiHandler.java', 'reject を会計影響なしで表示する集約'),
        anchor('audit 取り込み', '/Users/fujii/Desktop/dev/event-switchyard/oms-java/src/main/java/oms/audit/GatewayAuditIntakeService.java', 'reject event を順序付きで取り込む'),
      ],
    };
  }

  return {
    intro:
      focus === 'ledger'
        ? 'accepted はまだ約定でも台帳確定でもない。ここで見るのは reservation と order lifecycle の初期段階である。'
        : '注文は受け付けられているが、利用者が本当に知りたい「買えたか」はまだ確定していない。',
    userExpectation: [
      '注文がちゃんと受け付けられたと知りたい。',
      'まだ待機中なのか、すぐ約定する見込みなのか知りたい。',
      '余力が不自然に消えていないか確認したい。',
    ],
    systemStory: [
      'gateway-rust が受理境界を越え、OMS が accepted / working を持つ。',
      'reservation が拘束され、使いすぎを防ぐ。',
      '約定が来るまでは BackOffice の cash / position は原則確定しない。',
      'fill、cancel、expire のいずれかで terminal か継続中かが決まる。',
    ],
    readingOrder: [
      'accepted なのか working なのかを timeline で確認する。',
      'reservation が開いているかを見る。',
      'fills がまだ無ければ、会計影響を過剰に読まない。',
      'その後に fill / cancel / expire のどれへ進むかを想定する。',
    ],
    chosenDesign: [
      '受理と会計確定を分けた。',
      'reservation を margin と呼ばず、注文拘束として分離した。',
      'working order を OMS の責務として保持した。',
    ],
    rejectedDesign: [
      'accepted の時点で cash / position を確定する設計は取らない。',
      '予約拘束を risk model と同じ言葉で説明する設計は取らない。',
      '利用者に raw event の順序を直接押し付ける設計は取らない。',
    ],
    operatorChecks: [
      `pending ${pending}、DLQ ${dlq} を見て受理後の projection 崩れがないか確認する。`,
      'accepted のまま長く止まる場合は venue / stream / replay 状態を確認する。',
      'fills が無いのに台帳が進んでいないか確認する。',
    ],
    implementationAnchors: [
      anchor('注文の受理', '/Users/fujii/Desktop/dev/event-switchyard/gateway-rust/src/server/http/orders/classic.rs', 'classic order path の入口'),
      anchor('reservation と注文状態', '/Users/fujii/Desktop/dev/event-switchyard/oms-java/src/main/java/oms/audit/GatewayAuditIntakeService.java', 'accepted / working と reservation の収束'),
      anchor('学習用 replay', '/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/demo/ReplayScenarioService.java', 'accepted scenario の再現入口'),
    ],
  };
}

function describeLifecycle(status: string) {
  switch (status) {
    case 'PARTIALLY_FILLED':
      return 'accepted 後に partial fill が入り、残数量を維持したまま reservation が縮小する。';
    case 'FILLED':
      return 'accepted 後に full fill が入り、reservation は解放され、position と cash が確定する。';
    case 'REJECTED':
      return '受理前または venue reject により status が terminal になり、会計影響は発生しない。';
    case 'CANCELED':
      return 'cancel request 後に cancel complete へ遷移し、working quantity は消える。';
    default:
      return 'submitted -> accepted -> terminal or working の流れを追う。';
  }
}

function explanationPrompt(status: string) {
  switch (status) {
    case 'FILLED':
      return 'timeline と fills と balance effect を並べて、注文状態と会計状態の収束を分けて話す。';
    case 'PARTIALLY_FILLED':
      return 'working order を維持しながら ledger をどう更新するか、projection 順序を説明する。';
    case 'REJECTED':
      return 'hot path で reject しても BackOffice 正本を壊さない境界を説明する。';
    default:
      return 'OMS と BackOffice の責務差、raw event と UI projection の差を説明する。';
  }
}

function anchor(title: string, path: string, focus: string): StudyAnchor {
  return { title, path, focus };
}

function InfoMetric({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-2xl border border-white/10 bg-black/20 px-3 py-3">
      <div className="text-[11px] uppercase tracking-[0.18em] text-teal-50/60">{label}</div>
      <div className="mt-2 text-sm font-semibold text-white">{value}</div>
    </div>
  );
}

function NarrativeCard({ title, body }: { title: string; body: string }) {
  return (
    <div className="rounded-[20px] border border-white/8 bg-slate-950/55 px-4 py-4">
      <div className="text-sm font-semibold text-white">{title}</div>
      <div className="mt-2 text-sm leading-6 text-slate-300">{body}</div>
    </div>
  );
}

function StepRail({ steps }: { steps: string[] }) {
  return (
    <div className="mt-4 space-y-3">
      {steps.map((step, index) => (
        <div key={`${index}-${step}`} className="flex gap-3">
          <div className="flex flex-col items-center">
            <div className="mt-1 flex h-5 w-5 items-center justify-center rounded-full bg-emerald-400/15 text-[11px] font-semibold text-emerald-200">
              {index + 1}
            </div>
            {index < steps.length - 1 && <div className="mt-2 h-full w-px bg-white/10" />}
          </div>
          <div className="flex-1 rounded-[20px] border border-white/8 bg-slate-950/55 px-4 py-3 text-sm leading-6 text-slate-300">
            {step}
          </div>
        </div>
      ))}
    </div>
  );
}

function BulletStack({
  items,
  tone = 'slate',
}: {
  items: string[];
  tone?: 'slate' | 'emerald' | 'amber';
}) {
  const toneClass =
    tone === 'emerald'
      ? 'border-emerald-300/15 bg-emerald-500/10 text-emerald-50/90'
      : tone === 'amber'
        ? 'border-amber-300/15 bg-amber-500/10 text-amber-50/90'
        : 'border-white/8 bg-slate-950/55 text-slate-300';

  return (
    <div className="mt-4 space-y-3">
      {items.map((item, index) => (
        <div key={`${index}-${item}`} className={`rounded-[20px] border px-4 py-4 text-sm leading-6 ${toneClass}`}>
          {item}
        </div>
      ))}
    </div>
  );
}

function AnchorList({ anchors }: { anchors: StudyAnchor[] }) {
  return (
    <div className="mt-4 space-y-3">
      {anchors.map((item) => (
        <div key={`${item.title}-${item.path}`} className="rounded-[20px] border border-white/8 bg-slate-950/55 px-4 py-4">
          <div className="text-sm font-semibold text-white">{item.title}</div>
          <div className="mt-2 text-sm leading-6 text-slate-300">{item.focus}</div>
          <div className="mt-3 break-all text-[11px] leading-5 text-cyan-200/80">{item.path}</div>
        </div>
      ))}
    </div>
  );
}

function LedgerSummary({ finalOut }: { finalOut: OrderFinalOut }) {
  return (
    <section className="rounded-[24px] border border-white/10 bg-[linear-gradient(135deg,rgba(120,53,15,0.32),rgba(15,23,42,0.95))] p-4">
      <div className="text-base font-semibold text-white">Balance / P&L</div>
      <div className="mt-4 grid grid-cols-2 gap-3">
        <ImpactMetric label="Cash Δ" value={formatSignedCurrency(finalOut.balanceEffect.cashDelta)} />
        <ImpactMetric label="Avail BP Δ" value={formatSignedCurrency(finalOut.balanceEffect.availableBuyingPowerDelta)} />
        <ImpactMetric label="Reserved BP Δ" value={formatSignedCurrency(finalOut.balanceEffect.reservedBuyingPowerDelta)} />
        <ImpactMetric label="Realized PnL Δ" value={formatSignedCurrency(finalOut.balanceEffect.realizedPnlDelta)} />
      </div>
    </section>
  );
}

function PnlSplitCard({
  realizedPnl,
  unrealizedPnl,
}: {
  realizedPnl: number;
  unrealizedPnl: number;
}) {
  const total = Math.max(1, Math.abs(realizedPnl) + Math.abs(unrealizedPnl));
  const realizedWidth = `${(Math.abs(realizedPnl) / total) * 100}%`;
  const unrealizedWidth = `${(Math.abs(unrealizedPnl) / total) * 100}%`;

  return (
    <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
      <div className="text-base font-semibold text-white">Realized / Unrealized</div>
      <div className="mt-4 space-y-3">
        <div>
          <div className="flex items-center justify-between text-xs text-slate-300">
            <span>Realized</span>
            <span>{formatSignedCurrency(realizedPnl)}</span>
          </div>
          <div className="mt-2 h-3 rounded-full bg-slate-900/70">
            <div className="h-3 rounded-full bg-emerald-400/80" style={{ width: realizedWidth }} />
          </div>
        </div>
        <div>
          <div className="flex items-center justify-between text-xs text-slate-300">
            <span>Unrealized</span>
            <span>{formatSignedCurrency(unrealizedPnl)}</span>
          </div>
          <div className="mt-2 h-3 rounded-full bg-slate-900/70">
            <div className="h-3 rounded-full bg-sky-400/80" style={{ width: unrealizedWidth }} />
          </div>
        </div>
        <div className="rounded-[18px] border border-white/8 bg-slate-950/55 px-4 py-3 text-xs leading-6 text-slate-300">
          realized は fills で確定した損益、unrealized は現在価格に依存する評価差。
        </div>
      </div>
    </section>
  );
}

function ImpactMetric({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-2xl border border-white/8 bg-black/20 px-3 py-3">
      <div className="text-[11px] uppercase tracking-[0.18em] text-amber-100/70">{label}</div>
      <div className="mt-2 text-sm font-semibold text-white">{value}</div>
    </div>
  );
}

function ImpactList({
  title,
  emptyText,
  items,
}: {
  title: string;
  emptyText: string;
  items: Array<{ key: string; title: string; body: string }>;
}) {
  return (
    <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
      <div className="text-base font-semibold text-white">{title}</div>
      <div className="mt-4 space-y-3">
        {items.length === 0 ? (
          <div className="rounded-[20px] border border-dashed border-white/10 px-4 py-4 text-sm text-slate-400">{emptyText}</div>
        ) : (
          items.map((item) => (
            <div key={item.key} className="rounded-[20px] border border-white/8 bg-slate-950/55 px-4 py-4">
              <div className="text-sm font-semibold text-white">{item.title}</div>
              <div className="mt-2 text-sm text-slate-300">{item.body}</div>
            </div>
          ))
        )}
      </div>
    </section>
  );
}

function LedgerRail({
  entries,
  rawEventIndex,
}: {
  entries: OpsOverview['ledgerEntries'];
  rawEventIndex: Map<string, OrderFinalOut['rawEvents'][number]>;
}) {
  return (
    <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
      <div className="text-base font-semibold text-white">Ledger Entry Rail</div>
      <div className="mt-4 space-y-4">
        {entries.length === 0 ? (
          <div className="rounded-[20px] border border-dashed border-white/10 px-4 py-4 text-sm text-slate-400">
            ledger entry はまだありません
          </div>
        ) : (
          entries.map((entry, index) => {
            const rawEvent = rawEventIndex.get(entry.eventRef);
            return (
              <div key={entry.entryId} className="flex gap-3">
                <div className="flex flex-col items-center">
                  <div className={`mt-1 h-3 w-3 rounded-full ${index === entries.length - 1 ? 'bg-amber-300' : 'bg-amber-500/70'}`} />
                  {index < entries.length - 1 && <div className="mt-2 h-full w-px bg-white/10" />}
                </div>
                <div className="flex-1 rounded-[20px] border border-white/8 bg-slate-950/55 px-4 py-4">
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <div className="text-sm font-semibold text-white">{entry.eventType}</div>
                      <div className="mt-1 text-xs text-slate-500">{formatDateTime(entry.eventAt)}</div>
                    </div>
                    <div className="rounded-full border border-white/10 px-3 py-1 text-[11px] text-slate-300">
                      {entry.source}
                    </div>
                  </div>
                  <div className="mt-3 grid grid-cols-2 gap-2 text-xs text-slate-300">
                    <div className="rounded-2xl border border-white/8 bg-black/20 px-3 py-3">
                      Cash {formatSignedCurrency(entry.cashDelta)}
                    </div>
                    <div className="rounded-2xl border border-white/8 bg-black/20 px-3 py-3">
                      Reserve {formatSignedCurrency(entry.reservedBuyingPowerDelta)}
                    </div>
                    <div className="rounded-2xl border border-white/8 bg-black/20 px-3 py-3">
                      Qty {entry.quantityDelta > 0 ? '+' : ''}{formatNumber(entry.quantityDelta)}
                    </div>
                    <div className="rounded-2xl border border-white/8 bg-black/20 px-3 py-3">
                      PnL {formatSignedCurrency(entry.realizedPnlDelta)}
                    </div>
                  </div>
                  <div className="mt-3 text-sm leading-6 text-slate-300">{entry.detail}</div>
                  <div className="mt-3 grid gap-2 text-[11px] text-slate-500">
                    <div>eventRef {entry.eventRef}</div>
                    {rawEvent && <div>root {rawEvent.label} / {rawEvent.detail}</div>}
                  </div>
                </div>
              </div>
            );
          })
        )}
      </div>
    </section>
  );
}
