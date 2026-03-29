import { useMemo } from 'react';
import { useMobileMarketStructure, useMobileOrderFinalOut, useMobileOrders } from '../../hooks/useMobileStudy';
import type { BookLevel } from '../../types/trading';
import { formatCurrency, formatNumber, formatPercent, formatSignedCurrency, statusLabel } from './mobileUtils';

interface MobileMarketStructureViewProps {
  symbol: string | null;
  orderId: string | null;
  onNavigate: (path: string) => void;
}

export function MobileMarketStructureView({ symbol, orderId, onNavigate }: MobileMarketStructureViewProps) {
  const { data: orders, isLoading: ordersLoading } = useMobileOrders();
  const activeOrderId = orderId ?? orders?.[0]?.id ?? null;
  const activeSymbol = symbol ?? orders?.find((order) => order.id === activeOrderId)?.symbol ?? orders?.[0]?.symbol ?? '7203';
  const { data: marketStructure, isLoading: marketLoading, isError, error } = useMobileMarketStructure(activeSymbol);
  const { data: finalOut } = useMobileOrderFinalOut(activeOrderId);

  const symbolOptions = useMemo(() => {
    const ordered = new Map<string, string>();
    (orders ?? []).forEach((order) => {
      if (!ordered.has(order.symbol)) {
        ordered.set(order.symbol, order.symbol);
      }
    });
    if (!ordered.has(activeSymbol)) {
      ordered.set(activeSymbol, activeSymbol);
    }
    return [...ordered.keys()].slice(0, 6);
  }, [orders, activeSymbol]);

  if (ordersLoading || marketLoading) {
    return <div className="px-4 py-6 text-sm text-[color:var(--mobile-muted)]">市場構造を読み込み中...</div>;
  }

  if (isError || !marketStructure) {
    return (
      <div className="px-4 py-6 text-sm text-rose-200">
        市場構造を取得できませんでした
        <div className="mt-2 text-xs text-rose-200/80">{error instanceof Error ? error.message : 'unknown_error'}</div>
      </div>
    );
  }

  const executionQuality = finalOut?.executionQuality ?? null;
  const relatedOrder = activeOrderId ? orders?.find((order) => order.id === activeOrderId) ?? null : null;
  const narrative = buildNarrative(marketStructure.venueState, executionQuality?.slippageBps ?? null);

  return (
    <div className="space-y-4 px-4 py-5 pb-24">
      <section className="rounded-[28px] border border-white/10 bg-[linear-gradient(135deg,rgba(8,47,73,0.92),rgba(15,23,42,0.96))] p-5">
        <div className="flex items-start justify-between gap-4">
          <div>
            <div className="text-[11px] uppercase tracking-[0.22em] text-sky-100/70">市場構造</div>
            <h1 className="mt-2 text-2xl font-semibold text-white">{marketStructure.symbol}</h1>
            <div className="mt-1 text-sm text-slate-300">{marketStructure.symbolName}</div>
            <div className="mt-3 text-sm leading-6 text-slate-200">{narrative.intro}</div>
          </div>
          <div className={`rounded-full border px-3 py-1 text-xs font-medium ${venueTone(marketStructure.venueState)}`}>
            {venueLabel(marketStructure.venueState)}
          </div>
        </div>

        <div className="mt-5 grid grid-cols-2 gap-3">
          <Metric label="最終値" value={formatCurrency(marketStructure.lastPrice)} />
          <Metric label="ミッド" value={formatCurrency(marketStructure.midPrice)} />
          <Metric label="スプレッド" value={`${formatCurrency(marketStructure.spread)} / ${formatPercent(marketStructure.spreadBps, 2)}`} />
          <Metric label="気配" value={`${formatNumber(marketStructure.bidQuantity)} x ${formatNumber(marketStructure.askQuantity)}`} />
        </div>
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="flex items-center justify-between gap-3">
          <div className="text-base font-semibold text-white">銘柄を切り替える</div>
          {relatedOrder && (
            <button
              onClick={() => onNavigate(`/mobile/orders/${relatedOrder.id}`)}
              className="text-xs font-medium text-emerald-200"
            >
              注文に戻る
            </button>
          )}
        </div>
        <div className="mt-4 flex gap-2 overflow-x-auto pb-1">
          {symbolOptions.map((candidate) => (
            <button
              key={candidate}
              onClick={() => onNavigate(`/mobile/market/${candidate}`)}
              className={`min-w-[88px] rounded-[20px] border px-4 py-3 text-left ${candidate === marketStructure.symbol ? 'border-sky-300/50 bg-sky-500/10' : 'border-white/10 bg-white/5'}`}
            >
              <div className="text-sm font-semibold text-white">{candidate}</div>
            </button>
          ))}
        </div>
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-base font-semibold text-white">利用者が見ていること</div>
        <BulletStack items={narrative.userView} />
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-base font-semibold text-white">システムで起きていること</div>
        <StepRail steps={narrative.systemView} />
      </section>

      <section className="grid grid-cols-2 gap-4">
        <BookPanel title="買い気配" levels={marketStructure.bids} tone="emerald" />
        <BookPanel title="売り気配" levels={marketStructure.asks} tone="rose" />
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-base font-semibold text-white">この数字をどう読むか</div>
        <StepRail
          steps={[
            `bid ${formatCurrency(marketStructure.bidPrice)} と ask ${formatCurrency(marketStructure.askPrice)} の差が spread。狭いほど今すぐぶつけるコストは低い。`,
            `板の先頭数量は ${formatNumber(marketStructure.bidQuantity)} x ${formatNumber(marketStructure.askQuantity)}。注文数量がこれを超えると、次の段に食い込んで平均約定単価が悪化しやすい。`,
            `市場状態が ${venueLabel(marketStructure.venueState)} のときは、板の表示だけで安心しない。auction や halt 近辺では執行品質の説明が一段難しくなる。`,
          ]}
        />
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-base font-semibold text-white">取った設計</div>
        <BulletStack
          tone="emerald"
          items={[
            '送信時点の到着時基準を保存し、後から現在価格で上書きしない。',
            '市場構造は final-out に直接埋め込まず、独立 endpoint で都度参照できるようにしている。',
            '不利約定は売買方向を考慮した向き付き差分で出し、単純な価格差だけで読まない。',
          ]}
        />
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-base font-semibold text-white">意図的に取らなかった設計</div>
        <BulletStack
          tone="amber"
          items={[
            '注文時の最終値だけを基準にする設計は取らない。bid / ask / mid を残さないと、取りに行く側のコスト説明が曖昧になる。',
            'fill が無い段階で執行を良し悪し判定しきる設計は取らない。未約定なら spread と市場状態の説明に留める。',
            '学習用途でも reservation を market impact と同じ意味で見せる設計は取らない。受注制御と execution quality を分けるため。',
          ]}
        />
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-base font-semibold text-white">執行品質</div>
        {executionQuality ? (
          <div className="mt-4 space-y-3">
            <div className="grid grid-cols-2 gap-3">
              <Metric label="到着時 bid / ask" value={`${formatCurrency(executionQuality.arrivalBidPrice)} / ${formatCurrency(executionQuality.arrivalAskPrice)}`} />
              <Metric label="到着時ミッド" value={formatCurrency(executionQuality.arrivalMidPrice)} />
              <Metric label="到着時 spread" value={formatPercent(executionQuality.arrivalSpreadBps, 2)} />
              <Metric label="約定率" value={formatPercent(executionQuality.fillRatePercent, 1)} />
              <Metric label="平均約定" value={executionQuality.averageExecutionPrice == null ? '未約定' : formatCurrency(executionQuality.averageExecutionPrice)} />
              <Metric label="不利約定" value={executionQuality.slippageBps == null ? '未算出' : `${formatPercent(executionQuality.slippageBps, 2)} / ${formatSignedCurrency(executionQuality.slippageAmount)}`} />
            </div>
            <div className="rounded-[20px] border border-white/8 bg-slate-950/55 px-4 py-4 text-sm leading-6 text-slate-300">
              {executionQuality.note}
            </div>
            {relatedOrder && (
              <div className="rounded-[20px] border border-cyan-300/15 bg-cyan-500/10 px-4 py-4 text-sm leading-6 text-cyan-50/90">
                この注文は {statusLabel(relatedOrder.status)}。到着時基準は送信時点で固定し、後から現在価格で説明を塗り替えない。
              </div>
            )}
          </div>
        ) : (
          <div className="mt-4 rounded-[20px] border border-white/8 bg-slate-950/55 px-4 py-4 text-sm leading-6 text-slate-300">
            執行品質は、注文を起点に読むときに表示されます。まず注文画面で final-out を確認してから戻ると読みやすいです。
          </div>
        )}
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-base font-semibold text-white">実装アンカー</div>
        <AnchorList
          anchors={[
            {
              title: '市場構造 API',
              path: '/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/http/MarketApiHandler.java',
              focus: 'top-of-book と板 depth を返す入口。mobile ではこの API をそのまま読む。',
            },
            {
              title: '教育用 market snapshot',
              path: '/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/market/MarketDataService.java',
              focus: 'bid / ask / spread / venue state をどう組み立てているかを見る。',
            },
            {
              title: 'execution benchmark 保存',
              path: '/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/order/ExecutionBenchmarkStore.java',
              focus: '送信時の到着時基準を orderId 単位で持つ場所。',
            },
            {
              title: '執行品質の集約',
              path: '/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/http/OrderApiHandler.java',
              focus: 'fill と到着時基準から不利約定 / 約定率を final-out に載せている。',
            },
          ]}
        />
      </section>
    </div>
  );
}

function buildNarrative(venueState: string, slippageBps: number | null) {
  const stateLabel = venueLabel(venueState);
  const slippageText =
    slippageBps == null
      ? 'まだ fill が無いため執行品質は spread と板の厚みを中心に見る。'
      : `向き付きの不利約定は ${formatPercent(slippageBps, 2)}。到着時基準に対して、どちら向きにコストが出たかまで読む。`;
  return {
    intro: `いまは ${stateLabel} の想定。板、spread、到着時基準を一緒に見ると、注文時に何を警戒すべきかが分かる。`,
    userView: [
      'いま買うと高いのか、待てばよいのかを知りたい。',
      '成行でぶつけたときに、どこまで価格が滑りそうかを知りたい。',
      slippageText,
    ],
    systemView: [
      'market data から bid / ask / mid / spread / 板数量を作る。',
      '注文送信時には到着時基準を保存し、後から現在価格に引きずられないようにする。',
      'fill が返ったら平均約定価格、約定率、向き付きの不利約定を final-out に載せる。',
    ],
  };
}

function venueLabel(state: string) {
  switch (state) {
    case 'AUCTION_WATCH':
      return '板寄せ監視';
    case 'HALT_WATCH':
      return '売買停止監視';
    default:
      return '連続売買';
  }
}

function venueTone(state: string) {
  switch (state) {
    case 'HALT_WATCH':
      return 'border-rose-400/40 bg-rose-500/15 text-rose-100';
    case 'AUCTION_WATCH':
      return 'border-amber-400/40 bg-amber-500/15 text-amber-100';
    default:
      return 'border-emerald-400/40 bg-emerald-500/15 text-emerald-100';
  }
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-[22px] border border-white/8 bg-slate-950/55 px-4 py-4">
      <div className="text-[11px] uppercase tracking-[0.18em] text-slate-500">{label}</div>
      <div className="mt-2 text-base font-semibold text-white">{value}</div>
    </div>
  );
}

function BookPanel({
  title,
  levels,
  tone,
}: {
  title: string;
  levels: BookLevel[];
  tone: 'emerald' | 'rose';
}) {
  const badgeClass =
    tone === 'emerald'
      ? 'border-emerald-300/20 bg-emerald-500/10 text-emerald-100'
      : 'border-rose-300/20 bg-rose-500/10 text-rose-100';

  return (
    <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
      <div className="flex items-center justify-between">
        <div className="text-base font-semibold text-white">{title}</div>
        <div className={`rounded-full border px-3 py-1 text-[11px] font-medium ${badgeClass}`}>{title}</div>
      </div>
      <div className="mt-4 space-y-3">
        {levels.map((level, index) => (
          <div key={`${title}-${level.price}-${index}`} className="rounded-[18px] border border-white/8 bg-slate-950/55 px-4 py-3">
            <div className="flex items-center justify-between gap-3 text-sm">
              <span className="font-semibold text-white">{formatCurrency(level.price)}</span>
              <span className="text-slate-300">{formatNumber(level.quantity)} 株</span>
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

function StepRail({ steps }: { steps: string[] }) {
  return (
    <div className="mt-4 space-y-3">
      {steps.map((step, index) => (
        <div key={`${index}-${step}`} className="flex gap-3">
          <div className="flex flex-col items-center">
            <div className="mt-1 flex h-5 w-5 items-center justify-center rounded-full bg-cyan-400/15 text-[11px] font-semibold text-cyan-100">
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

function AnchorList({
  anchors,
}: {
  anchors: Array<{ title: string; path: string; focus: string }>;
}) {
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
