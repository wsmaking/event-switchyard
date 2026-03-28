import { useState } from 'react';
import {
  useMobileOpsOverview,
  useMobileOrderFinalOut,
  useMobileOrderStream,
  useMobileOrders,
  useRunMobileReplayScenario,
} from '../../hooks/useMobileStudy';
import { OrderSide, OrderType, TimeInForce } from '../../types/trading';
import type { OrderRequest } from '../../types/trading';
import { formatCurrency, formatDateTime, formatNumber, formatSignedCurrency, statusLabel, statusTone } from './mobileUtils';

interface MobileOrderStudyViewProps {
  focus: 'lifecycle' | 'ledger';
  orderId: string | null;
  onNavigate: (path: string) => void;
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
            replay scenario で状態を seed して、timeline と final-out を学習に使えます。
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
        <>
          {(() => {
            const unrealizedPnl = finalOut.positions.reduce((total, position) => total + position.unrealizedPnL, 0);
            const ledgerEntries = opsOverview?.ledgerEntries ?? [];
            const rawEventIndex = new Map(finalOut.rawEvents.map((event) => [event.eventRef, event]));
            return (
              <>
          <section className="rounded-[28px] border border-white/10 bg-[linear-gradient(135deg,rgba(15,118,110,0.28),rgba(15,23,42,0.95))] p-5">
            <div className="flex items-start justify-between gap-4">
              <div>
                <div className="text-[11px] uppercase tracking-[0.22em] text-teal-100/70">
                  {focus === 'ledger' ? '台帳の流れ' : '注文の流れ'}
                </div>
                <h1 className="mt-2 text-2xl font-semibold text-white">{finalOut.order.symbol}</h1>
                <div className="mt-2 text-sm leading-6 text-slate-200">
                  {explainOrder(finalOut.order.status)}
                </div>
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
            <div className="flex items-center justify-between">
              <div className="text-base font-semibold text-white">Timeline</div>
              <button onClick={() => onNavigate('/mobile/ledger')} className="text-xs font-medium text-emerald-200">
                台帳視点へ
              </button>
            </div>
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
            <section className="space-y-4">
              <LedgerSummary finalOut={finalOut} />
              <PnlSplitCard realizedPnl={finalOut.accountOverview.realizedPnl} unrealizedPnl={unrealizedPnl} />
              <ImpactList
                title="Reservations"
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
              <LedgerRail
                entries={ledgerEntries}
                rawEventIndex={rawEventIndex}
              />
            </section>
          ) : (
            <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
              <div className="text-base font-semibold text-white">説明ポイント</div>
              <div className="mt-4 grid gap-3">
                <NarrativeCard title="何が起きたか" body={describeLifecycle(finalOut.order.status)} />
                <NarrativeCard
                  title="説明の要点"
                  body={explanationPrompt(finalOut.order.status)}
                />
                <NarrativeCard
                  title="運用観点"
                  body={`sequence gap ${opsOverview?.omsStats?.sequenceGaps ?? 0} / pending ${opsOverview?.omsStats?.pendingOrphanCount ?? 0} / dlq ${opsOverview?.omsStats?.deadLetterCount ?? 0}`}
                />
              </div>
            </section>
          )}
              </>
            );
          })()}
        </>
      )}
    </div>
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

function explainOrder(status: string) {
  switch (status) {
    case 'FILLED':
      return '注文は venue で全量約定し、cash / position / raw events が final-out に収束している状態。';
    case 'PARTIALLY_FILLED':
      return '一部約定により reservation が縮小し、残数量が open のまま維持されている状態。';
    case 'REJECTED':
      return 'Gateway または venue で拒否され、台帳影響は最小で止まっている状態。';
    case 'CANCELED':
      return 'cancel が venue まで届き、残拘束が release された状態。';
    case 'EXPIRED':
      return 'GTD 期限切れで order が失効し、残拘束が release された状態。';
    default:
      return 'accepted から final-out までの流れを順に追う。';
  }
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

function LedgerSummary({ finalOut }: { finalOut: NonNullable<ReturnType<typeof useMobileOrderFinalOut>['data']> }) {
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
  entries: NonNullable<ReturnType<typeof useMobileOpsOverview>['data']>['ledgerEntries'];
  rawEventIndex: Map<string, NonNullable<ReturnType<typeof useMobileOrderFinalOut>['data']>['rawEvents'][number]>;
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
