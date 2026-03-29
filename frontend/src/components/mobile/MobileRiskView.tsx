import { useEffect, useMemo, useState } from 'react';
import { useEvaluateMobileOption, useEvaluateMobileRisk, useMobileRiskDeepDive, useMobileRiskScenarios } from '../../hooks/useMobileLearning';
import { formatCurrency, formatPercent, formatSignedCurrency } from './mobileUtils';

export function MobileRiskView() {
  const { data: scenarios, isLoading, isError, error } = useMobileRiskScenarios();
  const { data: deepDive } = useMobileRiskDeepDive();
  const evaluateRisk = useEvaluateMobileRisk();
  const evaluateOption = useEvaluateMobileOption();
  const [selectedScenarioId, setSelectedScenarioId] = useState<string | null>(null);
  const [customShock, setCustomShock] = useState(-5);
  const [optionType, setOptionType] = useState<'CALL' | 'PUT'>('CALL');
  const [symbol, setSymbol] = useState('7203');
  const [strikePrice, setStrikePrice] = useState('');
  const [volatilityPercent, setVolatilityPercent] = useState('24');
  const [ratePercent, setRatePercent] = useState('0.5');
  const [maturityDays, setMaturityDays] = useState('30');
  const [contracts, setContracts] = useState('1');

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

  useEffect(() => {
    if (!evaluateOption.data && !evaluateOption.isPending) {
      evaluateOption.mutate({
        symbol,
        optionType,
        strikePrice: null,
        volatilityPercent: parseOptionalNumber(volatilityPercent),
        ratePercent: parseOptionalNumber(ratePercent),
        maturityDays: parseOptionalNumber(maturityDays),
        contracts: parseOptionalNumber(contracts),
      });
    }
  }, []);

  const optionMetrics = useMemo(() => {
    if (!evaluateOption.data) {
      return null;
    }
    const deltaShares = evaluateOption.data.greeks.delta * evaluateOption.data.contracts;
    const gammaMessage =
      Math.abs(evaluateOption.data.greeks.gamma) >= 0.02 ? 'spot が動くほど delta が速く変わる' : 'delta の変化は比較的なだらか';
    const thetaMessage =
      evaluateOption.data.greeks.theta < 0 ? '時間経過で premium が削れやすい' : '時間経過の負担は小さめ';
    return {
      deltaShares,
      gammaMessage,
      thetaMessage,
    };
  }, [evaluateOption.data]);

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
  const option = evaluateOption.data;

  return (
    <div className="space-y-4 px-4 py-5 pb-24">
      <section className="rounded-[28px] border border-white/10 bg-[linear-gradient(135deg,rgba(127,29,29,0.55),rgba(15,23,42,0.96))] p-5">
        <div className="text-[11px] uppercase tracking-[0.22em] text-rose-100/70">リスク演習</div>
        <h1 className="mt-2 text-2xl font-semibold text-white">教育用ショックとオプション演習</h1>
        <p className="mt-2 text-sm leading-6 text-slate-300">
          現物の shock、historical VaR、simple hedge comparison、option payoff と Greeks を同じ画面でつなげて見る。
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
        <div className="text-sm font-semibold text-white">任意ショック</div>
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
          任意ショックを評価
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
            <div className="text-sm font-semibold text-white">教育用ヒストリカル VaR</div>
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
            <div className="text-sm font-semibold text-white">簡易ヘッジ比較</div>
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
            <div className="text-sm font-semibold text-white">建玉への影響</div>
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
                        <div className="mt-1 text-xs text-slate-400">
                          {position.symbol} / {position.netQty} 株
                        </div>
                      </div>
                      <div className="rounded-full border border-rose-300/30 bg-rose-500/10 px-3 py-1 text-[11px] text-rose-100">
                        {position.shockPercent > 0 ? '+' : ''}
                        {position.shockPercent}%
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

          {deepDive && (
            <>
              <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
                <div className="text-sm font-semibold text-white">集中度と流動性</div>
                <div className="mt-4 grid grid-cols-2 gap-3">
                  <RiskMetric label="Market Value" value={formatCurrency(deepDive.marketValue)} />
                  <RiskMetric label="Cash" value={formatCurrency(deepDive.cashBalance)} />
                </div>
                <div className="mt-4 space-y-3">
                  {deepDive.concentration.map((item) => (
                    <div key={item.symbol} className="rounded-[20px] border border-white/8 bg-slate-950/55 px-4 py-4">
                      <div className="flex items-start justify-between gap-3">
                        <div>
                          <div className="text-sm font-semibold text-white">{item.symbolName}</div>
                          <div className="mt-1 text-xs text-slate-400">{item.symbol}</div>
                        </div>
                        <div className="rounded-full border border-amber-300/30 bg-amber-500/10 px-3 py-1 text-[11px] text-amber-100">
                          {formatPercent(item.weightPercent, 1)}
                        </div>
                      </div>
                      <div className="mt-3 text-sm text-slate-300">Exposure {formatCurrency(item.exposure)}</div>
                      <div className="mt-2 text-xs leading-5 text-slate-400">{item.note}</div>
                    </div>
                  ))}
                </div>
                <div className="mt-4 space-y-3">
                  {deepDive.liquidity.map((item) => (
                    <div key={`${item.symbol}-liq`} className="rounded-[20px] border border-white/8 bg-black/20 px-4 py-4">
                      <div className="flex items-center justify-between gap-3">
                        <div className="text-sm font-semibold text-white">{item.symbolName}</div>
                        <div className="rounded-full border border-cyan-300/30 bg-cyan-500/10 px-3 py-1 text-[11px] text-cyan-100">
                          {formatPercent(item.participationPercent, 1)}
                        </div>
                      </div>
                      <div className="mt-3 grid grid-cols-2 gap-3">
                        <RiskMetric label="Position" value={`${item.positionQuantity}`} />
                        <RiskMetric label="Top of Book" value={`${item.visibleTopOfBookQuantity}`} />
                        <RiskMetric label="Days to Exit" value={`${item.estimatedDaysToExit.toFixed(1)}d`} />
                        <RiskMetric label="Participation" value={formatPercent(item.participationPercent, 1)} />
                      </div>
                      <div className="mt-3 text-xs leading-5 text-slate-400">{item.note}</div>
                    </div>
                  ))}
                </div>
              </section>

              <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
                <div className="text-sm font-semibold text-white">Scenario Library と Backtesting</div>
                <div className="mt-4 space-y-3">
                  {deepDive.scenarioLibrary.map((item) => (
                    <div key={item.id} className="rounded-[20px] border border-white/8 bg-slate-950/55 px-4 py-4">
                      <div className="flex items-center justify-between gap-3">
                        <div className="text-sm font-semibold text-white">{item.title}</div>
                        <div className="rounded-full border border-rose-300/30 bg-rose-500/10 px-3 py-1 text-[11px] text-rose-100">{item.shock}</div>
                      </div>
                      <div className="mt-2 text-xs uppercase tracking-[0.14em] text-slate-500">{item.category}</div>
                      <div className="mt-2 text-sm leading-6 text-slate-300">{item.rationale}</div>
                      <div className="mt-2 text-xs leading-5 text-slate-400">{item.focus}</div>
                    </div>
                  ))}
                </div>
                <div className="mt-4 rounded-[20px] border border-white/8 bg-black/20 px-4 py-4">
                  <div className="text-sm font-semibold text-white">Backtesting preview</div>
                  <div className="mt-3 grid grid-cols-2 gap-3">
                    <RiskMetric label="Obs" value={`${deepDive.backtesting.observationCount}`} />
                    <RiskMetric label="Breach" value={formatPercent(deepDive.backtesting.breachRatePercent, 1)} />
                    <RiskMetric label="Avg Tail" value={formatCurrency(deepDive.backtesting.averageTailLoss)} />
                  </div>
                  <div className="mt-3 text-sm leading-6 text-slate-300">{deepDive.backtesting.note}</div>
                  <div className="mt-3 space-y-2">
                    {deepDive.backtesting.samples.map((sample) => (
                      <div key={sample.label} className="flex items-center justify-between rounded-2xl border border-white/8 bg-slate-950/55 px-3 py-3 text-sm">
                        <div className="text-slate-300">{sample.label}</div>
                        <div className={sample.breached ? 'text-rose-200' : 'text-emerald-100'}>
                          {formatSignedCurrency(sample.pnl)}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              </section>

              <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
                <div className="text-sm font-semibold text-white">モデル境界</div>
                <div className="mt-4 space-y-3">
                  {deepDive.modelBoundaries.map((boundary) => (
                    <div key={boundary.title} className="rounded-[20px] border border-white/8 bg-slate-950/55 px-4 py-4">
                      <div className="text-sm font-semibold text-white">{boundary.title}</div>
                      <div className="mt-2 text-sm leading-6 text-slate-300">{boundary.whyItMatters}</div>
                      <div className="mt-3 text-xs leading-5 text-emerald-100">含めるもの: {boundary.whatIncluded}</div>
                      <div className="mt-1 text-xs leading-5 text-slate-400">省略しているもの: {boundary.whatExcluded}</div>
                    </div>
                  ))}
                </div>
              </section>

              {deepDive.marginProjection && (
                <section className="rounded-[24px] border border-amber-300/20 bg-amber-500/10 p-4">
                  <div className="text-sm font-semibold text-amber-50">Margin Utilization</div>
                  <div className="mt-4 grid grid-cols-2 gap-3">
                    <RiskMetric label="Method" value={deepDive.marginProjection.methodology} />
                    <RiskMetric label="Status" value={deepDive.marginProjection.breachStatus} />
                    <RiskMetric label="Limit" value={formatCurrency(deepDive.marginProjection.marginLimit)} />
                    <RiskMetric label="Used" value={formatCurrency(deepDive.marginProjection.marginUsed)} />
                    <RiskMetric label="Utilization" value={formatPercent(deepDive.marginProjection.utilizationPercent, 1)} />
                  </div>
                  <div className="mt-4 space-y-2">
                    {deepDive.marginProjection.breachedLimits.map((item) => (
                      <div key={item} className="rounded-2xl border border-amber-200/20 bg-black/20 px-3 py-3 text-sm leading-6 text-amber-50/90">
                        {item}
                      </div>
                    ))}
                    {deepDive.marginProjection.requiredActions.map((item) => (
                      <div key={item} className="rounded-2xl border border-white/8 bg-slate-950/55 px-3 py-3 text-sm leading-6 text-slate-300">
                        {item}
                      </div>
                    ))}
                    {deepDive.marginProjection.modelNotes.map((item) => (
                      <div key={item} className="rounded-2xl border border-white/8 bg-black/20 px-3 py-3 text-sm leading-6 text-slate-400">
                        {item}
                      </div>
                    ))}
                  </div>
                </section>
              )}

              {deepDive.scenarioEvaluationHistory && (
                <section className="rounded-[24px] border border-cyan-300/20 bg-cyan-500/10 p-4">
                  <div className="text-sm font-semibold text-cyan-50">Scenario Evaluation History</div>
                  <div className="mt-2 text-xs leading-5 text-cyan-100/80">{deepDive.scenarioEvaluationHistory.lastEvaluatedAtLabel}</div>
                  <div className="mt-4 space-y-3">
                    {deepDive.scenarioEvaluationHistory.evaluations.map((item) => (
                      <div key={`${item.title}-${item.shock}`} className="rounded-[20px] border border-cyan-200/20 bg-black/20 px-4 py-4">
                        <div className="flex items-center justify-between gap-3">
                          <div className="text-sm font-semibold text-white">{item.title}</div>
                          <div className="rounded-full border border-cyan-200/20 bg-cyan-400/10 px-3 py-1 text-[11px] text-cyan-50">{item.shock}</div>
                        </div>
                        <div className="mt-3 text-sm text-cyan-50">{formatSignedCurrency(item.pnlDelta)}</div>
                        <div className="mt-2 text-xs leading-5 text-slate-300">{item.note}</div>
                      </div>
                    ))}
                  </div>
                </section>
              )}

              {deepDive.backtestHistory && (
                <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
                  <div className="text-sm font-semibold text-white">Backtest Timeline</div>
                  <div className="mt-2 text-xs leading-5 text-slate-400">
                    {deepDive.backtestHistory.windowLabel} / breach {formatPercent(deepDive.backtestHistory.breachRatePercent, 1)}
                  </div>
                  <div className="mt-4 space-y-2">
                    {deepDive.backtestHistory.history.map((point) => (
                      <div key={`${point.label}-${point.pnl}`} className="rounded-2xl border border-white/8 bg-slate-950/55 px-3 py-3">
                        <div className="flex items-center justify-between gap-3 text-sm">
                          <div className="text-slate-300">{point.label}</div>
                          <div className={point.breached ? 'text-rose-200' : 'text-emerald-100'}>
                            {formatSignedCurrency(point.pnl)}
                          </div>
                        </div>
                        <div className="mt-2 text-xs leading-5 text-slate-400">{point.note}</div>
                      </div>
                    ))}
                  </div>
                </section>
              )}
            </>
          )}
        </>
      )}

      <section className="rounded-[24px] border border-white/10 bg-[linear-gradient(135deg,rgba(30,64,175,0.22),rgba(15,23,42,0.96))] p-4">
        <div className="flex items-center justify-between gap-3">
          <div>
            <div className="text-[11px] uppercase tracking-[0.2em] text-sky-100/70">オプション演習</div>
            <div className="mt-2 text-lg font-semibold text-white">ペイオフと Greeks の直感</div>
          </div>
          <div className="rounded-full border border-sky-300/30 bg-sky-500/10 px-3 py-1 text-[11px] text-sky-100">
            Black-Scholes
          </div>
        </div>
        <div className="mt-4 grid grid-cols-2 gap-3">
          <label className="text-xs text-slate-300">
            Symbol
            <input
              value={symbol}
              onChange={(event) => setSymbol(event.target.value.toUpperCase())}
              className="mt-2 w-full rounded-2xl border border-white/10 bg-slate-950/60 px-3 py-3 text-sm text-white outline-none"
            />
          </label>
          <label className="text-xs text-slate-300">
            Type
            <select
              value={optionType}
              onChange={(event) => setOptionType(event.target.value as 'CALL' | 'PUT')}
              className="mt-2 w-full rounded-2xl border border-white/10 bg-slate-950/60 px-3 py-3 text-sm text-white outline-none"
            >
              <option value="CALL">CALL</option>
              <option value="PUT">PUT</option>
            </select>
          </label>
          <label className="text-xs text-slate-300">
            Strike
            <input
              value={strikePrice}
              onChange={(event) => setStrikePrice(event.target.value)}
              placeholder="spot default"
              className="mt-2 w-full rounded-2xl border border-white/10 bg-slate-950/60 px-3 py-3 text-sm text-white outline-none placeholder:text-slate-500"
            />
          </label>
          <label className="text-xs text-slate-300">
            Vol %
            <input
              value={volatilityPercent}
              onChange={(event) => setVolatilityPercent(event.target.value)}
              className="mt-2 w-full rounded-2xl border border-white/10 bg-slate-950/60 px-3 py-3 text-sm text-white outline-none"
            />
          </label>
          <label className="text-xs text-slate-300">
            Rate %
            <input
              value={ratePercent}
              onChange={(event) => setRatePercent(event.target.value)}
              className="mt-2 w-full rounded-2xl border border-white/10 bg-slate-950/60 px-3 py-3 text-sm text-white outline-none"
            />
          </label>
          <label className="text-xs text-slate-300">
            Days
            <input
              value={maturityDays}
              onChange={(event) => setMaturityDays(event.target.value)}
              className="mt-2 w-full rounded-2xl border border-white/10 bg-slate-950/60 px-3 py-3 text-sm text-white outline-none"
            />
          </label>
          <label className="col-span-2 text-xs text-slate-300">
            Contracts
            <input
              value={contracts}
              onChange={(event) => setContracts(event.target.value)}
              className="mt-2 w-full rounded-2xl border border-white/10 bg-slate-950/60 px-3 py-3 text-sm text-white outline-none"
            />
          </label>
        </div>
        <button
          onClick={() =>
            evaluateOption.mutate({
              symbol,
              optionType,
              strikePrice: parseOptionalNumber(strikePrice),
              volatilityPercent: parseOptionalNumber(volatilityPercent),
              ratePercent: parseOptionalNumber(ratePercent),
              maturityDays: parseOptionalNumber(maturityDays),
              contracts: parseOptionalNumber(contracts),
            })
          }
          className="mt-4 w-full rounded-2xl border border-sky-300/30 bg-sky-500/10 px-4 py-3 text-sm font-medium text-sky-100"
        >
          option を評価
        </button>
      </section>

      {option && (
        <>
          <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
            <div className="flex items-start justify-between gap-3">
              <div>
                <div className="text-sm font-semibold text-white">{option.symbolName}</div>
                <div className="mt-2 text-sm leading-6 text-slate-300">
                  {option.optionType} / strike {formatCurrency(option.strikePrice)} / spot {formatCurrency(option.spotPrice)}
                </div>
              </div>
              <div className="rounded-full border border-sky-300/30 bg-sky-500/10 px-3 py-1 text-[11px] text-sky-100">
                {option.maturityDays}d
              </div>
            </div>
            <div className="mt-4 grid grid-cols-2 gap-3">
              <RiskMetric label="Option Price" value={formatCurrency(option.optionPrice)} />
              <RiskMetric label="Total Premium" value={formatCurrency(option.totalPremium)} />
              <RiskMetric label="Vol" value={formatPercent(option.volatilityPercent, 1)} />
              <RiskMetric label="Rate" value={formatPercent(option.ratePercent, 2)} />
            </div>
          </section>

          <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
            <div className="text-sm font-semibold text-white">Greeks</div>
            <div className="mt-4 grid grid-cols-2 gap-3">
              <RiskMetric label="Delta" value={option.greeks.delta.toFixed(4)} />
              <RiskMetric label="Gamma" value={option.greeks.gamma.toFixed(4)} />
              <RiskMetric label="Vega" value={option.greeks.vega.toFixed(4)} />
              <RiskMetric label="Theta" value={option.greeks.theta.toFixed(4)} />
            </div>
          </section>

          <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
            <div className="text-sm font-semibold text-white">Payoff Curve</div>
            <div className="mt-4 space-y-3">
              {option.payoffCurve.map((point) => (
                <PayoffRow key={`${point.underlyingPrice}-${point.payoff}`} point={point} />
              ))}
            </div>
          </section>

          {optionMetrics && (
            <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
              <div className="text-sm font-semibold text-white">Hedge Intuition</div>
              <div className="mt-4 grid gap-3">
                <HintCard body={`方向感は現物 ${optionMetrics.deltaShares.toFixed(2)} 株分の delta に近い。`} />
                <HintCard body={optionMetrics.gammaMessage} />
                <HintCard body={`vega ${option.greeks.vega.toFixed(4)} は implied vol 変化への感度。`} />
                <HintCard body={optionMetrics.thetaMessage} />
              </div>
            </section>
          )}

          <section className="rounded-[24px] border border-white/10 bg-white/5 p-4">
            <div className="text-sm font-semibold text-white">Assumptions</div>
            <div className="mt-4 space-y-3">
              {option.assumptions.map((assumption) => (
                <div key={assumption} className="rounded-[18px] border border-white/8 bg-slate-950/55 px-4 py-4 text-sm leading-6 text-slate-300">
                  {assumption}
                </div>
              ))}
            </div>
          </section>
        </>
      )}

      {evaluation && (
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

function HintCard({ body }: { body: string }) {
  return (
    <div className="rounded-[18px] border border-white/8 bg-slate-950/55 px-4 py-4 text-sm leading-6 text-slate-300">
      {body}
    </div>
  );
}

function PayoffRow({
  point,
}: {
  point: {
    underlyingPrice: number;
    payoff: number;
  };
}) {
  const max = 400;
  const width = `${Math.min(100, (Math.abs(point.payoff) / max) * 100)}%`;
  const positive = point.payoff >= 0;
  return (
    <div className="grid grid-cols-[96px_1fr_72px] items-center gap-3">
      <div className="text-xs text-slate-400">{formatCurrency(point.underlyingPrice)}</div>
      <div className="relative h-3 rounded-full bg-slate-900/70">
        <div className="absolute left-1/2 top-0 h-3 w-px -translate-x-1/2 bg-white/20" />
        <div
          className={`absolute top-0 h-3 rounded-full ${positive ? 'bg-emerald-400/80' : 'bg-rose-400/80'}`}
          style={positive ? { left: '50%', width } : { right: '50%', width }}
        />
      </div>
      <div className={`text-right text-xs ${positive ? 'text-emerald-200' : 'text-rose-200'}`}>
        {formatSignedCurrency(point.payoff)}
      </div>
    </div>
  );
}

function parseOptionalNumber(value: string): number | null {
  if (!value.trim()) {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}
