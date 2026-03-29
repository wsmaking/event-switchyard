import { useMobilePostTradeGuide } from '../../hooks/useMobileLearning';
import type { MobilePostTradeGuide } from '../../types/mobile';
import { formatCurrency, formatSignedCurrency } from './mobileUtils';

interface MobilePostTradeViewProps {
  onNavigate: (path: string) => void;
}

export function MobilePostTradeView({ onNavigate }: MobilePostTradeViewProps) {
  const { data, isLoading, isError, error } = useMobilePostTradeGuide();

  if (isLoading) {
    return <div className="px-4 py-6 text-sm text-[color:var(--mobile-muted)]">post-trade を読み込み中...</div>;
  }

  if (isError || !data) {
    return (
      <div className="px-4 py-6 text-sm text-rose-200">
        post-trade を取得できませんでした
        <div className="mt-2 text-xs text-rose-200/80">{error instanceof Error ? error.message : 'unknown_error'}</div>
      </div>
    );
  }

  return (
    <div className="space-y-4 px-4 py-5 pb-24">
      <section className="rounded-[28px] border border-white/10 bg-[linear-gradient(135deg,rgba(89,28,135,0.38),rgba(15,23,42,0.96))] p-5">
        <div className="text-[11px] uppercase tracking-[0.22em] text-fuchsia-100/70">Post-Trade</div>
        <h1 className="mt-2 text-2xl font-semibold text-white">約定のあとに閉じる業務</h1>
        <p className="mt-2 text-sm leading-6 text-slate-300">
          execution、allocation、clearing、settlement、books and records を分けて読む。front 画面の完了と会計上の完了は同じではない。
        </p>
        {data.orderId && (
          <button
            onClick={() => onNavigate(`/mobile/orders/${data.orderId}`)}
            className="mt-4 rounded-full border border-fuchsia-300/30 bg-fuchsia-500/10 px-4 py-2 text-xs font-medium text-fuchsia-100"
          >
            注文の final-out を開く
          </button>
        )}
      </section>

      <section className="space-y-3">
        <div className="text-sm font-semibold text-white">業務ステージ</div>
        {data.stages.map((stage) => (
          <div key={stage.name} className="rounded-[22px] border border-white/10 bg-white/5 p-4">
            <div className="flex items-center justify-between gap-3">
              <div className="text-base font-semibold text-white">{stage.name}</div>
              <div className="rounded-full border border-white/10 bg-black/20 px-3 py-1 text-[11px] text-slate-300">{stage.owner}</div>
            </div>
            <div className="mt-3 text-sm leading-6 text-slate-300">{stage.purpose}</div>
            <div className="mt-3 rounded-2xl border border-white/8 bg-slate-950/55 px-3 py-3 text-sm text-slate-200">{stage.currentView}</div>
            <div className="mt-3 text-xs leading-5 text-slate-400">{stage.whyItMatters}</div>
          </div>
        ))}
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-sm font-semibold text-white">手数料と現金移動</div>
        <div className="mt-4 grid grid-cols-2 gap-3">
          <MetricCard label="gross notional" value={formatCurrency(data.feeBreakdown.grossNotional)} />
          <MetricCard label="net cash" value={formatSignedCurrency(data.feeBreakdown.netCashMovement)} />
          <MetricCard label="commission" value={formatCurrency(data.feeBreakdown.commission)} />
          <MetricCard label="exchange fee" value={formatCurrency(data.feeBreakdown.exchangeFee)} />
          <MetricCard label="taxes" value={formatCurrency(data.feeBreakdown.taxes)} />
        </div>
        <div className="mt-4 space-y-2">
          {data.feeBreakdown.assumptions.map((assumption) => (
            <div key={assumption} className="rounded-2xl border border-white/8 bg-black/20 px-3 py-3 text-sm leading-6 text-slate-300">
              {assumption}
            </div>
          ))}
        </div>
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-sm font-semibold text-white">Statement / Confirm の見え方</div>
        <div className="mt-4 grid grid-cols-2 gap-3">
          <MetricCard label="account" value={data.statementPreview.accountId} />
          <MetricCard label="symbol" value={`${data.statementPreview.symbol} / ${data.statementPreview.symbolName}`} />
          <MetricCard label="settled qty" value={`${data.statementPreview.settledQuantity.toLocaleString('ja-JP')} 株`} />
          <MetricCard label="average price" value={formatCurrency(data.statementPreview.averagePrice)} />
          <MetricCard label="settlement" value={data.statementPreview.settlementDateLabel} />
          <MetricCard label="cash" value={data.statementPreview.netCashMovementLabel} />
        </div>
        <div className="mt-4 space-y-2">
          {data.statementPreview.notes.map((note) => (
            <div key={note} className="rounded-2xl border border-white/8 bg-slate-950/55 px-3 py-3 text-sm leading-6 text-slate-300">
              {note}
            </div>
          ))}
        </div>
      </section>

      <SimpleListSection title="Settlement の確認点" items={data.settlementChecks.map((check) => `${check.title}: ${check.rule} / ${check.currentValue}`)} />
      <SimpleListSection title="Corporate Action 入口" items={data.corporateActionHooks.map((hook) => `${hook.name}: ${hook.businessImpact} / ${hook.systemImpact}`)} />
      <AnchorsSection anchors={data.implementationAnchors} />
    </div>
  );
}

function MetricCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-2xl border border-white/8 bg-black/20 px-3 py-3">
      <div className="text-[11px] uppercase tracking-[0.14em] text-slate-500">{label}</div>
      <div className="mt-2 text-sm font-medium text-white">{value}</div>
    </div>
  );
}

function SimpleListSection({ title, items }: { title: string; items: string[] }) {
  return (
    <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
      <div className="text-sm font-semibold text-white">{title}</div>
      <div className="mt-4 space-y-2">
        {items.map((item) => (
          <div key={item} className="rounded-2xl border border-white/8 bg-slate-950/55 px-3 py-3 text-sm leading-6 text-slate-300">
            {item}
          </div>
        ))}
      </div>
    </section>
  );
}

function AnchorsSection({ anchors }: { anchors: MobilePostTradeGuide['implementationAnchors'] }) {
  return (
    <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
      <div className="text-sm font-semibold text-white">実装アンカー</div>
      <div className="mt-4 space-y-3">
        {anchors.map((anchor) => (
          <div key={`${anchor.path}-${anchor.title}`} className="rounded-[20px] border border-white/8 bg-slate-950/55 px-4 py-4">
            <div className="text-sm font-semibold text-white">{anchor.title}</div>
            <div className="mt-2 text-xs leading-5 text-slate-400">{anchor.focus}</div>
            <div className="mt-3 text-xs text-cyan-200 break-all">{anchor.path}</div>
          </div>
        ))}
      </div>
    </section>
  );
}
