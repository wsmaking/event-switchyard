import { useEffect, useState } from 'react';
import { useEvaluateMobileRisk, useMobileRiskScenarios } from '../../hooks/useMobileLearning';
import { formatCurrency, formatPercent, formatSignedCurrency } from './mobileUtils';

export function MobileRiskView() {
  const { data: scenarios, isLoading, isError, error } = useMobileRiskScenarios();
  const evaluateRisk = useEvaluateMobileRisk();
  const [selectedScenarioId, setSelectedScenarioId] = useState<string | null>(null);
  const [customShock, setCustomShock] = useState(-5);

  useEffect(() => {
    if (!selectedScenarioId && scenarios && scenarios.length > 0) {
      setSelectedScenarioId(scenarios[0].id);
    }
  }, [scenarios, selectedScenarioId]);

  useEffect(() => {
    if (selectedScenarioId) {
      evaluateRisk.mutate({ scenarioId: selectedScenarioId });
    }
  }, [selectedScenarioId]);

  if (isLoading) {
    return <div className="px-4 py-6 text-sm text-[color:var(--mobile-muted)]">risk scenario を読み込み中...</div>;
  }

  if (isError || !scenarios) {
    return (
      <div className="px-4 py-6 text-sm text-rose-200">
        risk scenario を取得できませんでした
        <div className="mt-2 text-xs text-rose-200/80">{error instanceof Error ? error.message : 'unknown_error'}</div>
      </div>
    );
  }

  const evaluation = evaluateRisk.data;

  return (
    <div className="space-y-4 px-4 py-5 pb-24">
      <section className="rounded-[28px] border border-white/10 bg-[linear-gradient(135deg,rgba(127,29,29,0.55),rgba(15,23,42,0.96))] p-5">
        <div className="text-[11px] uppercase tracking-[0.22em] text-rose-100/70">Risk Sandbox</div>
        <h1 className="mt-2 text-2xl font-semibold text-white">教育用 shock 評価</h1>
        <p className="mt-2 text-sm leading-6 text-slate-300">
          ポジションに単純 shock をかけ、数字と前提を同時に確認する。VaR engine ではなく学習用の簡易モデル。
        </p>
      </section>

      <section className="space-y-3">
        {scenarios.map((scenario) => (
          <button
            key={scenario.id}
            onClick={() => setSelectedScenarioId(scenario.id)}
            className={`w-full rounded-[22px] border px-4 py-4 text-left ${selectedScenarioId === scenario.id ? 'border-rose-300/40 bg-rose-500/10' : 'border-white/10 bg-white/5'}`}
          >
            <div className="text-sm font-semibold text-white">{scenario.title}</div>
            <div className="mt-2 text-sm text-slate-300">{scenario.description}</div>
          </button>
        ))}
      </section>

      <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
        <div className="text-sm font-semibold text-white">Custom Shock</div>
        <input
          type="range"
          min={-15}
          max={10}
          step={1}
          value={customShock}
          onChange={(event) => setCustomShock(Number(event.target.value))}
          className="mt-4 w-full accent-rose-400"
        />
        <div className="mt-2 flex items-center justify-between text-xs text-slate-400">
          <span>-15%</span>
          <span>{customShock}%</span>
          <span>+10%</span>
        </div>
        <button
          onClick={() => evaluateRisk.mutate({ customShockPercent: customShock })}
          className="mt-4 w-full rounded-2xl border border-rose-300/30 bg-rose-500/10 px-4 py-3 text-sm font-medium text-rose-100"
        >
          Custom shock を評価
        </button>
      </section>

      {evaluation && (
        <>
          <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
            <div className="text-sm font-semibold text-white">{evaluation.title}</div>
            <div className="mt-2 text-sm leading-6 text-slate-300">{evaluation.description}</div>
            <div className="mt-4 grid grid-cols-2 gap-3">
              <RiskMetric label="Current MV" value={formatCurrency(evaluation.portfolio.currentMarketValue)} />
              <RiskMetric label="Shocked MV" value={formatCurrency(evaluation.portfolio.shockedMarketValue)} />
              <RiskMetric label="PnL Δ" value={formatSignedCurrency(evaluation.portfolio.pnlDelta)} />
              <RiskMetric label="Cash" value={formatCurrency(evaluation.portfolio.cashBalance)} />
            </div>
          </section>

          <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
            <div className="text-sm font-semibold text-white">Educational Historical VaR</div>
            <div className="mt-4 grid grid-cols-2 gap-3">
              <RiskMetric label="Confidence" value={`${evaluation.historicalVar.confidenceLevel}%`} />
              <RiskMetric label="Observations" value={`${evaluation.historicalVar.observationCount}`} />
              <RiskMetric label="VaR Loss" value={formatCurrency(evaluation.historicalVar.varLoss)} />
              <RiskMetric label="Expected SF" value={formatCurrency(evaluation.historicalVar.expectedShortfall)} />
            </div>
            <div className="mt-4 rounded-[18px] border border-white/8 bg-slate-950/55 px-4 py-4 text-sm leading-6 text-slate-300">
              {evaluation.historicalVar.methodology} / holding period {evaluation.historicalVar.holdingPeriod}
            </div>
          </section>

          <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
            <div className="text-sm font-semibold text-white">Simple Hedge Comparison</div>
            <div className="mt-4 grid grid-cols-2 gap-3">
              <RiskMetric label="Hedge Symbol" value={evaluation.hedgeComparison.hedgeSymbol ?? 'none'} />
              <RiskMetric label="Hedge Ratio" value={formatPercent(evaluation.hedgeComparison.hedgeRatio * 100, 0)} />
              <RiskMetric label="Unhedged Δ" value={formatSignedCurrency(evaluation.hedgeComparison.unhedgedPnlDelta)} />
              <RiskMetric label="Hedged Δ" value={formatSignedCurrency(evaluation.hedgeComparison.hedgedPnlDelta)} />
            </div>
            <div className="mt-4 rounded-[18px] border border-emerald-300/20 bg-emerald-500/10 px-4 py-4 text-sm leading-6 text-emerald-100">
              Protection {formatCurrency(evaluation.hedgeComparison.protectionAmount)} / {evaluation.hedgeComparison.note}
            </div>
          </section>

          <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
            <div className="text-sm font-semibold text-white">Position Impact</div>
            <div className="mt-4 space-y-3">
              {evaluation.positions.length === 0 ? (
                <div className="rounded-[20px] border border-dashed border-white/10 px-4 py-4 text-sm text-slate-400">
                  ポジションがありません。filled scenario を先に作ると学習しやすいです。
                </div>
              ) : (
                evaluation.positions.map((position) => (
                  <div key={position.symbol} className="rounded-[20px] border border-white/8 bg-slate-950/55 px-4 py-4">
                    <div className="flex items-start justify-between gap-3">
                      <div>
                        <div className="text-sm font-semibold text-white">{position.symbolName}</div>
                        <div className="mt-1 text-xs text-slate-400">{position.symbol} / {position.netQty} 株</div>
                      </div>
                      <div className="rounded-full border border-rose-300/30 bg-rose-500/10 px-3 py-1 text-[11px] text-rose-100">
                        {position.shockPercent > 0 ? '+' : ''}{position.shockPercent}%
                      </div>
                    </div>
                    <div className="mt-3 grid grid-cols-2 gap-3 text-sm">
                      <div className="rounded-2xl border border-white/8 bg-black/20 px-3 py-3 text-slate-300">
                        現値 {formatCurrency(position.currentPrice)}
                      </div>
                      <div className="rounded-2xl border border-white/8 bg-black/20 px-3 py-3 text-slate-300">
                        stress 後 {formatCurrency(position.shockedPrice)}
                      </div>
                      <div className="rounded-2xl border border-white/8 bg-black/20 px-3 py-3 text-slate-300">
                        MV {formatCurrency(position.currentValue)}
                      </div>
                      <div className="rounded-2xl border border-white/8 bg-black/20 px-3 py-3 text-slate-300">
                        Δ {formatSignedCurrency(position.pnlDelta)}
                      </div>
                    </div>
                  </div>
                ))
              )}
            </div>
          </section>

          <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
            <div className="text-sm font-semibold text-white">Assumptions</div>
            <div className="mt-4 space-y-3">
              {evaluation.assumptions.map((assumption) => (
                <div key={assumption} className="rounded-[18px] border border-white/8 bg-slate-950/55 px-4 py-4 text-sm leading-6 text-slate-300">
                  {assumption}
                </div>
              ))}
            </div>
          </section>
        </>
      )}
    </div>
  );
}

function RiskMetric({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-2xl border border-white/8 bg-slate-950/55 px-3 py-3">
      <div className="text-[11px] uppercase tracking-[0.18em] text-rose-100/60">{label}</div>
      <div className="mt-2 text-sm font-semibold text-white">{value}</div>
    </div>
  );
}
