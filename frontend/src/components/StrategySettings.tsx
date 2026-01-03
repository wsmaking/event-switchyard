import { useEffect, useState } from 'react';
import { useStrategyConfig, useUpdateStrategyConfig } from '../hooks/useStrategyConfig';
import type { StrategyConfigUpdate } from '../types/trading';

export function StrategySettings() {
  const { data, isLoading, isError, error } = useStrategyConfig();
  const updateConfig = useUpdateStrategyConfig();
  const [enabled, setEnabled] = useState(false);
  const [symbolsInput, setSymbolsInput] = useState('7203,6758,9984');
  const [tickMs, setTickMs] = useState(1000);
  const [maxOrdersPerMin, setMaxOrdersPerMin] = useState(0);
  const [cooldownMs, setCooldownMs] = useState(0);

  useEffect(() => {
    if (!data) return;
    setEnabled(data.enabled);
    setSymbolsInput(data.symbols.join(','));
    setTickMs(data.tickMs);
    setMaxOrdersPerMin(data.maxOrdersPerMin);
    setCooldownMs(data.cooldownMs);
  }, [data]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const symbols = symbolsInput
      .split(',')
      .map((s) => s.trim())
      .filter((s) => s.length > 0);
    const payload: StrategyConfigUpdate = {
      enabled,
      symbols,
      tickMs,
      maxOrdersPerMin,
      cooldownMs,
    };
    updateConfig.mutate(payload);
  };

  const isSaving = updateConfig.isPending;
  const updateError =
    updateConfig.error instanceof Error ? updateConfig.error.message : 'Failed to save strategy config.';
  const lastUpdated =
    data?.updatedAtMs ? new Date(data.updatedAtMs).toLocaleString() : '---';
  const storageStatus = data
    ? `${data.storage.toUpperCase()}${data.storageHealthy ? '' : ' (degraded)'}`
    : '---';

  return (
    <div className="mx-auto w-full max-w-6xl px-4 pb-16 pt-8 sm:px-6 lg:px-8">
      <div className="mb-6">
        <h2 className="text-2xl font-semibold text-slate-100">Strategy Settings</h2>
        <p className="mt-1 text-sm text-slate-400">
          自動戦略のON/OFFや発注間隔、安全柵を管理します。
        </p>
      </div>

        <div className="grid gap-6 lg:grid-cols-[2fr_1fr]">
          <form
            onSubmit={handleSubmit}
            className="rounded-2xl border border-slate-800/70 bg-slate-900/60 p-6 shadow-lg shadow-slate-950/40"
          >
          <div className="mb-6 flex items-center justify-between">
            <div>
              <div className="text-sm font-semibold text-slate-100">Auto Strategy</div>
              <div className="text-xs text-slate-500">配信中の戦略設定</div>
            </div>
            <label className="inline-flex items-center gap-2 text-sm text-slate-300">
              <input
                type="checkbox"
                checked={enabled}
                onChange={(e) => setEnabled(e.target.checked)}
                className="h-4 w-4 rounded border-slate-600 bg-slate-800 text-blue-400 focus:ring-blue-400"
              />
              Enabled
            </label>
          </div>

          <div className="grid gap-4 md:grid-cols-2">
            <label className="text-sm text-slate-300">
              Symbols (CSV)
              <input
                type="text"
                value={symbolsInput}
                onChange={(e) => setSymbolsInput(e.target.value)}
                className="mt-2 w-full rounded-lg border border-slate-700 bg-slate-950/70 px-3 py-2 text-sm text-slate-100 focus:border-blue-400 focus:outline-none"
                placeholder="7203,6758,9984"
              />
            </label>

            <label className="text-sm text-slate-300">
              Tick Interval (ms)
              <input
                type="number"
                min={100}
                value={tickMs}
                onChange={(e) => setTickMs(Number(e.target.value))}
                className="mt-2 w-full rounded-lg border border-slate-700 bg-slate-950/70 px-3 py-2 text-sm text-slate-100 focus:border-blue-400 focus:outline-none"
              />
            </label>

            <label className="text-sm text-slate-300">
              Max Orders / Min
              <input
                type="number"
                min={0}
                value={maxOrdersPerMin}
                onChange={(e) => setMaxOrdersPerMin(Number(e.target.value))}
                className="mt-2 w-full rounded-lg border border-slate-700 bg-slate-950/70 px-3 py-2 text-sm text-slate-100 focus:border-blue-400 focus:outline-none"
              />
            </label>

            <label className="text-sm text-slate-300">
              Cooldown (ms)
              <input
                type="number"
                min={0}
                value={cooldownMs}
                onChange={(e) => setCooldownMs(Number(e.target.value))}
                className="mt-2 w-full rounded-lg border border-slate-700 bg-slate-950/70 px-3 py-2 text-sm text-slate-100 focus:border-blue-400 focus:outline-none"
              />
            </label>
          </div>

          <div className="mt-6 flex items-center justify-between">
            <div className="text-xs text-slate-500">
              Last updated: {lastUpdated} · Storage: {storageStatus}
            </div>
            <button
              type="submit"
              disabled={isSaving}
              className="rounded-lg bg-blue-500/90 px-4 py-2 text-sm font-semibold text-slate-950 transition hover:bg-blue-400 disabled:cursor-not-allowed disabled:bg-slate-700 disabled:text-slate-300"
            >
              {isSaving ? 'Saving...' : 'Save Settings'}
            </button>
          </div>

          {isError && (
            <div className="mt-4 rounded-lg border border-red-500/40 bg-red-500/10 px-3 py-2 text-xs text-red-300">
              {error instanceof Error ? error.message : 'Failed to load strategy config.'}
            </div>
          )}
          {updateConfig.isError && (
            <div className="mt-3 rounded-lg border border-red-500/40 bg-red-500/10 px-3 py-2 text-xs text-red-300">
              {updateError}
            </div>
          )}
        </form>

        <div className="rounded-2xl border border-slate-800/70 bg-slate-900/40 p-6 text-sm text-slate-300">
          <div className="mb-3 text-xs font-semibold uppercase tracking-wide text-slate-500">
            Notes
          </div>
          <ul className="space-y-3 text-sm text-slate-400">
            <li>EnabledをOFFにすると自動発注は停止します。</li>
            <li>Max Orders / Min は0で制限なし。</li>
            <li>Cooldownは同一銘柄への連続発注を抑制します。</li>
            <li>設定はAPI経由で更新され、数秒以内に反映されます。</li>
            <li>DBが落ちた場合は自動戦略が無効化されます。</li>
          </ul>
        </div>
      </div>

      {isLoading && (
        <div className="mt-4 text-sm text-slate-400">Loading strategy config...</div>
      )}
    </div>
  );
}
