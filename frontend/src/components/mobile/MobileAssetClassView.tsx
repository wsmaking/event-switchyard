import { useMobileAssetClassGuide } from '../../hooks/useMobileLearning';

export function MobileAssetClassView() {
  const { data, isLoading, isError, error } = useMobileAssetClassGuide();

  if (isLoading) {
    return <div className="px-4 py-6 text-sm text-[color:var(--mobile-muted)]">資産別比較を読み込み中...</div>;
  }

  if (isError || !data) {
    return (
      <div className="px-4 py-6 text-sm text-rose-200">
        資産別比較を取得できませんでした
        <div className="mt-2 text-xs text-rose-200/80">{error instanceof Error ? error.message : 'unknown_error'}</div>
      </div>
    );
  }

  return (
    <div className="space-y-4 px-4 py-5 pb-24">
      <section className="rounded-[28px] border border-white/10 bg-[linear-gradient(135deg,rgba(14,116,144,0.35),rgba(15,23,42,0.96))] p-5">
        <div className="text-[11px] uppercase tracking-[0.22em] text-cyan-100/70">Multi-Asset</div>
        <h1 className="mt-2 text-2xl font-semibold text-white">資産ごとに何が共通で、何が変わるか</h1>
        <p className="mt-2 text-sm leading-6 text-slate-300">
          受注、状態遷移、監査の骨格は共通化しつつ、valuation、risk driver、settlement convention は資産ごとに専用化する。
        </p>
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-sm font-semibold text-white">境界の原則</div>
        <div className="mt-4 space-y-2">
          {data.boundaryPrinciples.map((principle) => (
            <div key={principle} className="rounded-2xl border border-white/8 bg-slate-950/55 px-3 py-3 text-sm leading-6 text-slate-300">
              {principle}
            </div>
          ))}
        </div>
      </section>

      <section className="space-y-3">
        {data.assetClasses.map((assetClass) => (
          <div key={assetClass.assetClass} className="rounded-[24px] border border-white/10 bg-white/5 p-4">
            <div className="text-lg font-semibold text-white">{assetClass.assetClass}</div>
            <div className="mt-4 grid grid-cols-1 gap-3 text-sm">
              <Panel label="Trade Initiation" value={assetClass.tradeInitiation} />
              <Panel label="Lifecycle" value={assetClass.lifecycle} />
              <Panel label="Valuation Driver" value={assetClass.valuationDriver} />
              <Panel label="Settlement" value={assetClass.settlementModel} />
              <Panel label="Risk Driver" value={assetClass.riskDriver} />
            </div>
            <TripleList
              leftTitle="運用で見る点"
              leftItems={assetClass.operatorWatchpoints}
              centerTitle="共通化できるもの"
              centerItems={assetClass.whatStaysCommon}
              rightTitle="専用化すべきもの"
              rightItems={assetClass.whatMustSpecialize}
            />
            <div className="mt-4 rounded-[22px] border border-white/10 bg-black/20 p-4">
              <div className="text-xs font-semibold uppercase tracking-[0.14em] text-slate-400">数値例と lifecycle breakpoint</div>
              <div className="mt-3 space-y-3">
                {assetClass.sampleMetrics.map((metric) => (
                  <div key={`${assetClass.assetClass}-${metric.label}`} className="rounded-2xl border border-white/8 bg-slate-950/55 px-3 py-3">
                    <div className="text-[11px] uppercase tracking-[0.14em] text-slate-500">{metric.label}</div>
                    <div className="mt-2 text-sm font-medium text-white">{metric.value}</div>
                    <div className="mt-2 text-xs leading-5 text-slate-400">{metric.note}</div>
                  </div>
                ))}
              </div>
              <div className="mt-4 space-y-2">
                {assetClass.lifecycleBreakpoints.map((item) => (
                  <div key={`${assetClass.assetClass}-${item}`} className="text-sm leading-6 text-slate-300">
                    ・{item}
                  </div>
                ))}
              </div>
            </div>
            <TripleList
              leftTitle="帳票 / 台帳への影響"
              leftItems={assetClass.booksAndRecordsImplications}
              centerTitle="運用事故の起点"
              centerItems={assetClass.failureModes}
              rightTitle="監視ポイント"
              rightItems={assetClass.operatorWatchpoints}
            />
          </div>
        ))}
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-sm font-semibold text-white">実装アンカー</div>
        <div className="mt-4 space-y-3">
          {data.implementationAnchors.map((anchor) => (
            <div key={`${anchor.path}-${anchor.title}`} className="rounded-[20px] border border-white/8 bg-slate-950/55 px-4 py-4">
              <div className="text-sm font-semibold text-white">{anchor.title}</div>
              <div className="mt-2 text-xs leading-5 text-slate-400">{anchor.focus}</div>
              <div className="mt-3 break-all text-xs text-cyan-200">{anchor.path}</div>
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}

function Panel({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-2xl border border-white/8 bg-black/20 px-3 py-3">
      <div className="text-[11px] uppercase tracking-[0.14em] text-slate-500">{label}</div>
      <div className="mt-2 text-sm leading-6 text-slate-200">{value}</div>
    </div>
  );
}

function TripleList({
  leftTitle,
  leftItems,
  centerTitle,
  centerItems,
  rightTitle,
  rightItems,
}: {
  leftTitle: string;
  leftItems: string[];
  centerTitle: string;
  centerItems: string[];
  rightTitle: string;
  rightItems: string[];
}) {
  return (
    <div className="mt-4 grid grid-cols-1 gap-3">
      <ListBox title={leftTitle} items={leftItems} />
      <ListBox title={centerTitle} items={centerItems} />
      <ListBox title={rightTitle} items={rightItems} />
    </div>
  );
}

function ListBox({ title, items }: { title: string; items: string[] }) {
  return (
    <div className="rounded-2xl border border-white/8 bg-slate-950/55 px-3 py-3">
      <div className="text-xs font-semibold uppercase tracking-[0.14em] text-slate-400">{title}</div>
      <div className="mt-3 space-y-2">
        {items.map((item) => (
          <div key={item} className="text-sm leading-6 text-slate-300">
            ・{item}
          </div>
        ))}
      </div>
    </div>
  );
}
