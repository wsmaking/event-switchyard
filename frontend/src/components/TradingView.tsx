import { useState } from 'react';
import {
  useOrderFinalOut,
  useOpsOverview,
  useReplayGatewayAudit,
  useOrders,
  useResetDemo,
  useRunReplayScenario,
  useSubmitOrder,
} from '../hooks/useOrders';
import { useStockInfo, usePositions, usePriceHistory } from '../hooks/useMarketData';
import { OrderSide, OrderType, OrderStatus, TimeInForce } from '../types/trading';
import type { OrderRequest, PricePoint } from '../types/trading';

export function TradingView() {
  const {
    data: orders,
    isLoading: ordersLoading,
    isError: ordersError,
    error: ordersErrorObj,
  } = useOrders();
  const {
    data: positions,
    isLoading: positionsLoading,
    isError: positionsError,
    error: positionsErrorObj,
  } = usePositions();
  const submitOrder = useSubmitOrder();
  const resetDemo = useResetDemo();
  const runReplayScenario = useRunReplayScenario();
  const replayGatewayAudit = useReplayGatewayAudit();
  const [selectedOrderId, setSelectedOrderId] = useState<string | null>(null);

  // フォーム状態
  const [symbol, setSymbol] = useState('7203'); // トヨタ自動車
  const [side, setSide] = useState<OrderSide>(OrderSide.BUY);
  const [orderType, setOrderType] = useState<OrderType>(OrderType.MARKET);
  const [quantity, setQuantity] = useState(100);
  const [price, setPrice] = useState<number | null>(null);
  const [timeInForce, setTimeInForce] = useState<TimeInForce>(TimeInForce.GTC);
  const [expireAtInput, setExpireAtInput] = useState('');

  // 現在の銘柄情報を取得
  const { data: stockInfo } = useStockInfo(symbol);
  const {
    data: priceHistory,
    isLoading: historyLoading,
    isError: historyError,
  } = usePriceHistory(symbol);

  const buildOrderRequest = (): OrderRequest => {
    const parsedExpireAt = expireAtInput ? Date.parse(expireAtInput) : Number.NaN;
    const expireAtMs =
      timeInForce === TimeInForce.GTD && !Number.isNaN(parsedExpireAt) ? parsedExpireAt : null;

    return {
      symbol,
      side,
      type: orderType,
      quantity,
      price: orderType === OrderType.LIMIT ? price : null,
      timeInForce,
      expireAt: expireAtMs,
    };
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const request = buildOrderRequest();

    submitOrder.mutate(request);

    // フォームリセット（銘柄・数量は保持）
    setOrderType(OrderType.MARKET);
    setPrice(null);
    setTimeInForce(TimeInForce.GTC);
    setExpireAtInput('');
  };

  const handleDemoOrder = () => {
    const demoRequest: OrderRequest = {
      symbol: symbol || '7203',
      side: OrderSide.BUY,
      type: OrderType.MARKET,
      quantity: 100,
      price: null,
      timeInForce: TimeInForce.GTC,
      expireAt: null,
    };
    submitOrder.mutate(demoRequest);
  };

  const handleReplayScenario = (scenario: string) => {
    runReplayScenario.mutate(
      {
        scenario,
        request: buildOrderRequest(),
      },
      {
        onSuccess: (order) => {
          setSelectedOrderId(order.id);
        },
      }
    );
  };

  const apiErrors = [ordersErrorObj, positionsErrorObj]
    .filter(Boolean)
    .map((err) => (err instanceof Error ? err.message : String(err)))
    .join(' / ');

  const showEmptyOrders = !ordersLoading && !ordersError && (!orders || orders.length === 0);
  const showEmptyPositions = !positionsLoading && !positionsError && (!positions || positions.length === 0);
  const historyPoints = priceHistory ?? [];
  const replayScenarios = [
    { id: 'accepted', label: '受付済', tone: 'border-amber-500/30 text-amber-200' },
    { id: 'partial-fill', label: '一部約定', tone: 'border-blue-500/30 text-blue-200' },
    { id: 'filled', label: '全量約定', tone: 'border-emerald-500/30 text-emerald-200' },
    { id: 'canceled', label: '取消完了', tone: 'border-sky-500/30 text-sky-200' },
    { id: 'expired', label: '失効', tone: 'border-rose-500/30 text-rose-200' },
    { id: 'rejected', label: '拒否', tone: 'border-rose-500/30 text-rose-200' },
  ] as const;

  const buildSparklinePoints = (points: PricePoint[]) => {
    if (points.length < 2) return '';
    const prices = points.map((p) => p.price);
    const min = Math.min(...prices);
    const max = Math.max(...prices);
    const range = max - min || 1;
    const step = 100 / (points.length - 1);
    return points
      .map((p, idx) => {
        const x = idx * step;
        const y = 40 - ((p.price - min) / range) * 40;
        return `${x.toFixed(2)},${y.toFixed(2)}`;
      })
      .join(' ');
  };

  const summarizeHistory = (points: PricePoint[]) => {
    if (points.length === 0) return null;
    const prices = points.map((p) => p.price);
    const min = Math.min(...prices);
    const max = Math.max(...prices);
    const spanSec =
      points.length > 1
        ? Math.max(0, Math.round((points[points.length - 1].timestamp - points[0].timestamp) / 1000))
        : 0;
    return { min, max, spanSec };
  };

  const sparkline = buildSparklinePoints(historyPoints);
  const historyStats = summarizeHistory(historyPoints);
  const effectiveSelectedOrderId = selectedOrderId ?? orders?.[0]?.id ?? null;
  const { data: finalOut, isLoading: finalOutLoading } = useOrderFinalOut(effectiveSelectedOrderId);
  const { data: opsOverview, isLoading: opsOverviewLoading } = useOpsOverview(effectiveSelectedOrderId);

  const statusTone = (status: OrderStatus) => {
    switch (status) {
      case OrderStatus.FILLED:
        return 'bg-emerald-500/20 text-emerald-200';
      case OrderStatus.REJECTED:
      case OrderStatus.CANCELED:
      case OrderStatus.EXPIRED:
        return 'bg-rose-500/20 text-rose-200';
      case OrderStatus.PARTIALLY_FILLED:
        return 'bg-blue-500/20 text-blue-200';
      case OrderStatus.CANCEL_PENDING:
      case OrderStatus.AMEND_PENDING:
        return 'bg-sky-500/20 text-sky-200';
      default:
        return 'bg-amber-500/20 text-amber-200';
    }
  };

  const statusLabel = (status: OrderStatus) => {
    switch (status) {
      case OrderStatus.PENDING_ACCEPT:
        return '受付中';
      case OrderStatus.ACCEPTED:
        return '受付済';
      case OrderStatus.PARTIALLY_FILLED:
        return '一部約定';
      case OrderStatus.FILLED:
        return '約定済';
      case OrderStatus.CANCEL_PENDING:
        return '取消中';
      case OrderStatus.CANCELED:
        return '取消済';
      case OrderStatus.EXPIRED:
        return '失効';
      case OrderStatus.REJECTED:
        return '失敗';
      case OrderStatus.AMEND_PENDING:
        return '訂正中';
      default:
        return status;
    }
  };

  const marketSymbols = [
    { symbol: '7203', label: 'トヨタ自動車' },
    { symbol: '6758', label: 'ソニーグループ' },
    { symbol: '9984', label: 'ソフトバンクG' },
    { symbol: '6861', label: 'キーエンス' },
    { symbol: '8306', label: '三菱UFJ' },
  ];

  const panelClass =
    'rounded-2xl border border-slate-800/70 bg-[color:var(--panel)] shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur';
  const panelStrongClass =
    'rounded-2xl border border-slate-800/70 bg-[color:var(--panel-strong)] shadow-[0_20px_60px_rgba(0,0,0,0.5)] backdrop-blur';
  const inputClass =
    'w-full rounded-md border border-slate-700/60 bg-slate-900/70 px-3 py-2 text-sm text-slate-100 placeholder:text-slate-500 focus:outline-none focus:ring-2 focus:ring-blue-500/40';

  const formatEventTime = (value: number | null | undefined) =>
    value ? new Date(value).toLocaleString('ja-JP') : '-';

  const MarketCard = ({ symbol: cardSymbol, label }: { symbol: string; label: string }) => {
    const { data: info } = useStockInfo(cardSymbol);
    const { data: history, isLoading, isError } = usePriceHistory(cardSymbol);
    const points = history ?? [];
    const stats = summarizeHistory(points);
    const sparklinePoints = buildSparklinePoints(points);
    const latest = info?.currentPrice;
    const change = info?.change ?? 0;
    const changePercent = info?.changePercent ?? 0;

    return (
      <div className="rounded-xl border border-slate-800/60 bg-slate-950/70 p-4 shadow-[0_16px_32px_rgba(0,0,0,0.4)] transition duration-200 hover:-translate-y-0.5 hover:border-slate-700/70">
        <div className="flex items-start justify-between">
          <div>
            <div className="text-xs text-slate-500">{cardSymbol}</div>
            <div className="text-sm font-semibold text-slate-100">{label}</div>
          </div>
          <div className="text-right">
            <div className="text-sm font-bold text-slate-100">
              {latest ? `¥${latest.toLocaleString()}` : '—'}
            </div>
            <div className={`text-xs font-medium ${change >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
              {latest ? `${change >= 0 ? '+' : ''}${change.toFixed(2)} (${changePercent.toFixed(2)}%)` : '更新待ち'}
            </div>
          </div>
        </div>

        <div className="mt-3 min-h-[70px] rounded-md bg-slate-900/60 px-2 py-2">
          <div className="flex items-center justify-between text-[10px] text-slate-400">
            <span>価格(円)</span>
            <span>{stats ? `過去${stats.spanSec}s` : '時間'}</span>
          </div>
          {isLoading ? (
            <div className="text-xs text-slate-500">読み込み中...</div>
          ) : isError ? (
            <div className="text-xs text-rose-300">履歴を取得できませんでした</div>
          ) : points.length < 2 ? (
            <div className="text-xs text-slate-500">履歴がありません</div>
          ) : (
            <svg viewBox="0 0 100 40" className="h-12 w-full text-blue-200">
              <polyline fill="none" stroke="currentColor" strokeWidth="1.5" points={sparklinePoints} />
            </svg>
          )}
          <div className="mt-1 flex items-center justify-between text-[10px] text-slate-400">
            <span>{stats ? `安 ¥${stats.min.toFixed(2)}` : '安値'}</span>
            <span>{stats ? `高 ¥${stats.max.toFixed(2)}` : '高値'}</span>
          </div>
        </div>
      </div>
    );
  };

  return (
    <div className="relative min-h-screen overflow-hidden">
      <div className="pointer-events-none absolute inset-0">
        <div className="absolute -top-24 right-[-10%] h-72 w-72 rounded-full bg-blue-500/10 blur-3xl" />
        <div className="absolute top-1/3 left-[-10%] h-72 w-72 rounded-full bg-emerald-500/10 blur-3xl" />
        <div className="absolute bottom-0 right-1/3 h-64 w-64 rounded-full bg-amber-500/10 blur-3xl" />
      </div>
      <div className="relative mx-auto max-w-7xl px-6 py-8">
        <div className="mb-8 flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between reveal">
          <div>
            <p className="text-xs uppercase tracking-[0.3em] text-slate-500">Trading Console</p>
            <h1 className="text-3xl font-semibold text-slate-100 sm:text-4xl">注文 & 市場ビュー</h1>
            <p className="text-sm text-slate-400">
              Fast Pathの注文処理と主要銘柄の動きを同じ画面で監視します
            </p>
          </div>
          <div className="flex items-center gap-3 rounded-full border border-slate-800/70 bg-slate-950/60 px-4 py-2 text-xs text-slate-400">
            <span className="inline-flex h-2 w-2 rounded-full bg-emerald-400 shadow-[0_0_12px_rgba(52,211,153,0.8)]" />
            Gateway SSE / Market Polling
          </div>
        </div>

        {(ordersError || positionsError) && (
          <div className="mb-6 rounded-xl border border-rose-500/40 bg-rose-500/10 p-4 text-sm text-rose-100 reveal reveal-delay-1">
            APIに接続できません。App/Gateway/BackOffice が起動しているか確認してください。
            {apiErrors && <div className="mt-1 text-xs text-rose-200">詳細: {apiErrors}</div>}
          </div>
        )}

        <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
          <div className="lg:col-span-2 space-y-6">
            <div className={`${panelClass} p-6 reveal reveal-delay-1`}>
              <div className="flex items-center justify-between">
                <h2 className="text-lg font-semibold text-slate-100">選択中の銘柄</h2>
                <span className="rounded-full border border-slate-700/70 bg-slate-900/70 px-3 py-1 text-xs text-slate-400">
                  Symbol: {symbol}
                </span>
              </div>
              {stockInfo ? (
                <>
                  <div className="mt-3 text-sm text-slate-400">{stockInfo.name}</div>
                  <div className="mt-1 flex items-baseline space-x-2">
                    <span className="text-2xl font-bold text-slate-100">
                      ¥{stockInfo.currentPrice.toLocaleString()}
                    </span>
                    <span
                      className={`text-sm font-medium ${
                        stockInfo.change >= 0 ? 'text-emerald-400' : 'text-rose-400'
                      }`}
                    >
                      {stockInfo.change >= 0 ? '+' : ''}
                      {stockInfo.change.toFixed(2)} ({stockInfo.changePercent.toFixed(2)}%)
                    </span>
                  </div>
                  <div className="mt-1 text-xs text-slate-400">
                    高値: ¥{stockInfo.high.toLocaleString()} / 安値: ¥{stockInfo.low.toLocaleString()}
                  </div>

                  <div className="mt-4 rounded-md bg-slate-900/60 px-3 py-3">
                    <div className="flex items-center justify-between text-xs text-slate-400">
                      <span>価格(円)</span>
                      <span>{historyStats ? `過去${historyStats.spanSec}s` : '時間'}</span>
                    </div>
                    {historyLoading ? (
                      <div className="mt-2 text-xs text-slate-500">読み込み中...</div>
                    ) : historyError ? (
                      <div className="mt-2 text-xs text-rose-300">履歴を取得できませんでした</div>
                    ) : historyPoints.length < 2 ? (
                      <div className="mt-2 text-xs text-slate-500">履歴がありません</div>
                    ) : (
                      <svg viewBox="0 0 100 40" className="mt-1 h-24 w-full text-blue-200">
                        <polyline
                          fill="none"
                          stroke="currentColor"
                          strokeWidth="1.5"
                          points={sparkline}
                        />
                      </svg>
                    )}
                    <div className="mt-1 flex items-center justify-between text-[11px] text-slate-400">
                      <span>{historyStats ? `安 ¥${historyStats.min.toFixed(2)}` : '安値'}</span>
                      <span>{historyStats ? `高 ¥${historyStats.max.toFixed(2)}` : '高値'}</span>
                    </div>
                  </div>
                </>
              ) : (
                <div className="text-sm text-slate-500">読み込み中...</div>
              )}
            </div>

            <div className={`${panelStrongClass} p-6 reveal reveal-delay-2`}>
              <div className="flex items-center justify-between">
                <h2 className="text-lg font-semibold text-slate-100">主要銘柄の動き</h2>
                <span className="text-xs text-slate-500">5秒/10秒更新</span>
              </div>
              <div className="mt-4 grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-3">
                {marketSymbols.map((item) => (
                  <MarketCard key={item.symbol} symbol={item.symbol} label={item.label} />
                ))}
              </div>
            </div>
          </div>

          <div className="lg:col-span-1">
            <div className={`${panelStrongClass} p-6 reveal reveal-delay-2`}>
              <div className="flex items-center justify-between">
                <h2 className="text-lg font-semibold text-slate-100">新規注文</h2>
                <span className="text-xs text-slate-500">Pre-Trade Risk</span>
              </div>

              <form onSubmit={handleSubmit} className="space-y-4">
                {/* 銘柄コード */}
                <div>
                  <label className="mb-1 block text-sm font-medium text-slate-300">
                    銘柄コード
                  </label>
                  <input
                    type="text"
                    value={symbol}
                    onChange={(e) => setSymbol(e.target.value)}
                    className={inputClass}
                    placeholder="例: 7203"
                    required
                  />
                </div>

                {/* 売買区分 */}
                <div>
                  <label className="mb-1 block text-sm font-medium text-slate-300">
                    売買区分
                  </label>
                  <div className="grid grid-cols-2 gap-2">
                    <button
                      type="button"
                      onClick={() => setSide(OrderSide.BUY)}
                      className={`px-4 py-2 rounded-md font-medium ${
                        side === OrderSide.BUY
                          ? 'bg-rose-500 text-slate-900 shadow-[0_0_14px_rgba(244,63,94,0.45)]'
                          : 'bg-slate-900/70 text-slate-300 hover:bg-slate-800/80'
                      }`}
                    >
                      買い
                    </button>
                    <button
                      type="button"
                      onClick={() => setSide(OrderSide.SELL)}
                      className={`px-4 py-2 rounded-md font-medium ${
                        side === OrderSide.SELL
                          ? 'bg-blue-500/70 text-slate-100 shadow-[0_0_14px_rgba(96,125,196,0.45)]'
                          : 'bg-slate-900/70 text-slate-300 hover:bg-slate-800/80'
                      }`}
                    >
                      売り
                    </button>
                  </div>
                </div>

                {/* 注文種別 */}
                <div>
                  <label className="mb-1 block text-sm font-medium text-slate-300">
                    注文種別
                  </label>
                  <select
                    value={orderType}
                    onChange={(e) => setOrderType(e.target.value as OrderType)}
                    className={inputClass}
                  >
                    <option value={OrderType.MARKET}>成行（即座に約定）</option>
                    <option value={OrderType.LIMIT}>指値（価格指定）</option>
                  </select>
                  <p className="mt-1 text-xs text-slate-500">
                    {orderType === OrderType.MARKET
                      ? '現在価格で即座に約定します（実行時間: 0.5-2.5ms）'
                      : '指定価格に達したら約定します（実行時間: 5-15ms）'}
                  </p>
                </div>

                {/* 有効期限 */}
                <div>
                  <label className="mb-1 block text-sm font-medium text-slate-300">
                    有効期限（Time in Force）
                  </label>
                  <select
                    value={timeInForce}
                    onChange={(e) => {
                      const next = e.target.value as TimeInForce;
                      setTimeInForce(next);
                      if (next !== TimeInForce.GTD) {
                        setExpireAtInput('');
                      }
                    }}
                    className={inputClass}
                  >
                    <option value={TimeInForce.GTC}>GTC（期限なし）</option>
                    <option value={TimeInForce.GTD}>GTD（期限指定）</option>
                  </select>
                  {timeInForce === TimeInForce.GTD && (
                    <input
                      type="datetime-local"
                      value={expireAtInput}
                      onChange={(e) => setExpireAtInput(e.target.value)}
                      className={`${inputClass} mt-2`}
                      required
                    />
                  )}
                </div>

                {/* 数量 */}
                <div>
                  <label className="mb-1 block text-sm font-medium text-slate-300">
                    数量（株）
                  </label>
                  <input
                    type="number"
                    value={quantity}
                    onChange={(e) => setQuantity(Number(e.target.value))}
                    className={inputClass}
                    min="1"
                    step="1"
                    required
                  />
                </div>

                {/* 指値価格 */}
                {orderType === OrderType.LIMIT && (
                  <div>
                    <label className="mb-1 block text-sm font-medium text-slate-300">
                      指値価格（円）
                    </label>
                    <input
                      type="number"
                      value={price || ''}
                      onChange={(e) => setPrice(Number(e.target.value))}
                      className={inputClass}
                      step="0.01"
                      required
                    />
                  </div>
                )}

                {/* 送信ボタン */}
                <button
                  type="submit"
                  disabled={submitOrder.isPending || runReplayScenario.isPending}
                  className="w-full rounded-md bg-blue-500/80 py-3 font-semibold text-slate-100 transition hover:bg-blue-400/90 disabled:cursor-not-allowed disabled:bg-slate-700"
                >
                  {submitOrder.isPending ? '送信中...' : '注文を送信'}
                </button>

                <button
                  type="button"
                  onClick={handleDemoOrder}
                  disabled={submitOrder.isPending || runReplayScenario.isPending}
                  className="w-full rounded-md border border-slate-700/70 py-2 font-semibold text-slate-300 transition hover:bg-slate-900/60 disabled:bg-slate-900 disabled:text-slate-500"
                >
                  デモ注文を1件作成
                </button>

                <div className="rounded-xl border border-slate-800/60 bg-slate-950/40 p-3">
                  <div className="flex items-center justify-between">
                    <div>
                      <div className="text-sm font-semibold text-slate-100">業務再現シナリオ</div>
                      <div className="mt-1 text-xs text-slate-500">
                        1クリックで OMS / BackOffice を再構成し、最終Outを即確認します
                      </div>
                    </div>
                    <span className="text-[11px] text-slate-500">状態は上書きされます</span>
                  </div>
                  <div className="mt-3 grid grid-cols-2 gap-2">
                    {replayScenarios.map((scenario) => (
                      <button
                        key={scenario.id}
                        type="button"
                        onClick={() => handleReplayScenario(scenario.id)}
                        disabled={runReplayScenario.isPending || submitOrder.isPending}
                        className={`rounded-md border bg-slate-900/70 px-3 py-2 text-sm font-semibold transition hover:bg-slate-900 disabled:cursor-not-allowed disabled:border-slate-800 disabled:text-slate-500 ${scenario.tone}`}
                      >
                        {scenario.label}
                      </button>
                    ))}
                  </div>
                </div>

                <button
                  type="button"
                  onClick={() => {
                    resetDemo.mutate();
                    setSelectedOrderId(null);
                  }}
                  disabled={resetDemo.isPending || runReplayScenario.isPending}
                  className="w-full rounded-md border border-slate-700/70 py-2 font-semibold text-slate-300 transition hover:bg-slate-900/60 disabled:bg-slate-900 disabled:text-slate-500"
                >
                  {resetDemo.isPending ? 'リセット中...' : 'デモをリセット'}
                </button>

                {submitOrder.isError && (
                  <div className="text-sm text-rose-300">
                    注文に失敗しました: {submitOrder.error.message}
                  </div>
                )}
                {resetDemo.isError && (
                  <div className="text-sm text-rose-300">
                    リセットに失敗しました: {resetDemo.error.message}
                  </div>
                )}
                {runReplayScenario.isError && (
                  <div className="text-sm text-rose-300">
                    シナリオ再生に失敗しました: {runReplayScenario.error.message}
                  </div>
                )}
              </form>
            </div>
          </div>
        </div>

        <div className="mt-6 grid grid-cols-1 gap-6 lg:grid-cols-2">
          {/* 保有銘柄 */}
          <div className={`${panelClass} p-6 reveal reveal-delay-3`}>
            <h2 className="mb-4 text-lg font-semibold text-slate-100">保有銘柄</h2>

              {positionsLoading && !positionsError ? (
                <div className="text-sm text-slate-500">読み込み中...</div>
              ) : positions && positions.length > 0 ? (
                <div className="overflow-x-auto rounded-xl border border-slate-800/60">
                  <table className="min-w-full divide-y divide-slate-800/60">
                    <thead className="bg-slate-900/70">
                      <tr>
                        <th className="px-4 py-3 text-left text-xs font-medium uppercase text-slate-400">
                          銘柄
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium uppercase text-slate-400">
                          数量
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium uppercase text-slate-400">
                          平均取得価格
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium uppercase text-slate-400">
                          現在価格
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium uppercase text-slate-400">
                          評価損益
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium uppercase text-slate-400">
                          損益率
                        </th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-800/60 bg-slate-950/40">
                      {positions.map((position) => (
                        <tr key={position.symbol} className="hover:bg-slate-900/40">
                          <td className="px-4 py-3 whitespace-nowrap text-sm font-medium text-slate-100">
                            {position.symbol}
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm text-slate-300">
                            {position.quantity.toLocaleString()}株
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm text-slate-300">
                            ¥{position.avgPrice.toLocaleString()}
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm text-slate-300">
                            ¥{position.currentPrice.toLocaleString()}
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm">
                            <span
                              className={`font-medium ${
                                position.unrealizedPnL >= 0 ? 'text-emerald-400' : 'text-rose-400'
                              }`}
                            >
                              {position.unrealizedPnL >= 0 ? '+' : ''}
                              ¥{position.unrealizedPnL.toLocaleString()}
                            </span>
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm">
                            <span
                              className={`font-medium ${
                                position.unrealizedPnLPercent >= 0 ? 'text-emerald-400' : 'text-rose-400'
                              }`}
                            >
                              {position.unrealizedPnLPercent >= 0 ? '+' : ''}
                              {position.unrealizedPnLPercent.toFixed(2)}%
                            </span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : showEmptyPositions ? (
                <div className="text-sm text-slate-500">
                  保有銘柄がありません。注文を出すか、自動発注を有効にしてください。
                </div>
              ) : (
                <div className="text-sm text-slate-500">保有銘柄を取得できませんでした</div>
              )}
          </div>

          {/* 注文履歴 */}
          <div className={`${panelClass} p-6 reveal reveal-delay-3`}>
            <h2 className="mb-4 text-lg font-semibold text-slate-100">注文履歴</h2>

              {ordersLoading && !ordersError ? (
                <div className="text-slate-500">読み込み中...</div>
              ) : orders && orders.length > 0 ? (
                <div className="overflow-x-auto rounded-xl border border-slate-800/60">
                  <table className="min-w-full divide-y divide-slate-800/60">
                    <thead className="bg-slate-900/70">
                      <tr>
                        <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-400">
                          時刻
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-400">
                          銘柄
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-400">
                          売買
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-400">
                          種別
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-400">
                          TIF
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-400">
                          期限
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-400">
                          数量
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-400">
                          価格
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-400">
                          ステータス
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-400">
                          約定時間
                        </th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-800/60 bg-slate-950/40">
                      {orders.map((order) => (
                        <tr
                          key={order.id}
                          className={`cursor-pointer hover:bg-slate-900/40 ${
                            effectiveSelectedOrderId === order.id ? 'bg-slate-900/50' : ''
                          }`}
                          onClick={() => setSelectedOrderId(order.id)}
                        >
                          <td className="px-4 py-3 whitespace-nowrap text-sm text-slate-100">
                            {new Date(order.submittedAt).toLocaleTimeString('ja-JP')}
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm font-medium text-slate-100">
                            {order.symbol}
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm">
                            <span
                              className={`px-2 py-1 rounded-md font-medium ${
                                order.side === OrderSide.BUY
                                  ? 'bg-rose-500/20 text-rose-200'
                                  : 'bg-blue-500/20 text-blue-200'
                              }`}
                            >
                              {order.side === OrderSide.BUY ? '買' : '売'}
                            </span>
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm text-slate-300">
                            {order.type === OrderType.MARKET ? '成行' : '指値'}
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm text-slate-300">
                            {order.timeInForce}
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm text-slate-300">
                            {order.expireAt ? new Date(order.expireAt).toLocaleString('ja-JP') : '-'}
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm text-slate-300">
                            {order.quantity.toLocaleString()}
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm text-slate-300">
                            {order.price ? `¥${order.price.toLocaleString()}` : '-'}
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm">
                            <span
                              className={`px-2 py-1 rounded-md font-medium ${statusTone(order.status)}`}
                            >
                              {statusLabel(order.status)}
                            </span>
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm text-slate-300">
                            {order.executionTimeMs ? `${order.executionTimeMs.toFixed(2)}ms` : '-'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : showEmptyOrders ? (
                <div className="text-slate-500">
                  注文履歴がありません。右のフォームから注文を作成してください。
                </div>
              ) : (
                <div className="text-slate-500">注文履歴を取得できませんでした</div>
              )}
          </div>
        </div>

        <div className={`mt-6 ${panelClass} p-6 reveal reveal-delay-3`}>
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h2 className="text-lg font-semibold text-slate-100">最終Out</h2>
              <span className="text-xs text-slate-500">
                {effectiveSelectedOrderId ? `Order: ${effectiveSelectedOrderId}` : '注文未選択'}
              </span>
            </div>
            <button
              onClick={() => replayGatewayAudit.mutate()}
              disabled={replayGatewayAudit.isPending}
              className="rounded-md border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs font-medium text-amber-100 transition hover:bg-amber-500/20 disabled:cursor-not-allowed disabled:opacity-50"
            >
              {replayGatewayAudit.isPending ? 'Gateway Audit Replay中...' : 'Gateway Audit Replay'}
            </button>
          </div>

          {finalOutLoading ? (
            <div className="mt-4 text-sm text-slate-500">読み込み中...</div>
          ) : finalOut ? (
            <div className="mt-4 grid grid-cols-1 gap-4 xl:grid-cols-2 2xl:grid-cols-3">
              <div className="rounded-xl border border-slate-800/60 bg-slate-950/50 p-4">
                <div className="text-xs uppercase tracking-[0.2em] text-slate-500">Order</div>
                <div className="mt-3 flex items-center justify-between">
                  <div>
                    <div className="text-sm font-semibold text-slate-100">{finalOut.order.symbol}</div>
                    <div className="text-xs text-slate-400">
                      {finalOut.order.side} / {finalOut.order.type} / {finalOut.order.quantity.toLocaleString()}株
                    </div>
                  </div>
                  <span className={`rounded-md px-2 py-1 text-xs font-medium ${statusTone(finalOut.order.status)}`}>
                    {statusLabel(finalOut.order.status)}
                  </span>
                </div>
                <div className="mt-4 space-y-2 text-sm text-slate-300">
                  <div>受付時刻: {new Date(finalOut.order.submittedAt).toLocaleString('ja-JP')}</div>
                  <div>価格: {finalOut.order.price ? `¥${finalOut.order.price.toLocaleString()}` : '成行'}</div>
                  <div>約定数量: {(finalOut.order.filledQuantity ?? 0).toLocaleString()}株</div>
                  <div>残数量: {(finalOut.order.remainingQuantity ?? finalOut.order.quantity).toLocaleString()}株</div>
                  <div>理由: {finalOut.order.statusReason ?? '-'}</div>
                </div>
              </div>

              <div className="rounded-xl border border-slate-800/60 bg-slate-950/50 p-4">
                <div className="text-xs uppercase tracking-[0.2em] text-slate-500">Account</div>
                <div className="mt-3 space-y-3 text-sm text-slate-300">
                  <div>口座: {finalOut.accountOverview.accountId}</div>
                  <div>現金残高: ¥{finalOut.accountOverview.cashBalance.toLocaleString()}</div>
                  <div>利用可能余力: ¥{finalOut.accountOverview.availableBuyingPower.toLocaleString()}</div>
                  <div>拘束余力: ¥{finalOut.accountOverview.reservedBuyingPower.toLocaleString()}</div>
                  <div>建玉数: {finalOut.accountOverview.positionCount.toLocaleString()}</div>
                  <div>実現損益: ¥{finalOut.accountOverview.realizedPnl.toLocaleString()}</div>
                </div>
                <div className="mt-4 rounded-lg border border-slate-800/60 bg-slate-900/60 p-3">
                  <div className="text-[11px] uppercase tracking-[0.2em] text-slate-500">Balance Delta</div>
                  <div className="mt-2 space-y-2 text-xs text-slate-300">
                    <div>現金差分: {finalOut.balanceEffect.cashDelta >= 0 ? '+' : ''}¥{finalOut.balanceEffect.cashDelta.toLocaleString()}</div>
                    <div>利用可能余力差分: {finalOut.balanceEffect.availableBuyingPowerDelta >= 0 ? '+' : ''}¥{finalOut.balanceEffect.availableBuyingPowerDelta.toLocaleString()}</div>
                    <div>拘束余力差分: {finalOut.balanceEffect.reservedBuyingPowerDelta >= 0 ? '+' : ''}¥{finalOut.balanceEffect.reservedBuyingPowerDelta.toLocaleString()}</div>
                    <div>実現損益差分: {finalOut.balanceEffect.realizedPnlDelta >= 0 ? '+' : ''}¥{finalOut.balanceEffect.realizedPnlDelta.toLocaleString()}</div>
                  </div>
                </div>
              </div>

              <div className="rounded-xl border border-slate-800/60 bg-slate-950/50 p-4">
                <div className="text-xs uppercase tracking-[0.2em] text-slate-500">Reservations</div>
                {finalOut.reservations.length > 0 ? (
                  <div className="mt-3 space-y-3">
                    {finalOut.reservations.map((reservation) => (
                      <div key={reservation.reservationId} className="rounded-lg border border-slate-800/60 bg-slate-900/60 p-3">
                        <div className="flex items-center justify-between">
                          <div className="text-sm font-semibold text-slate-100">{reservation.symbol}</div>
                          <div className="text-xs text-slate-400">{reservation.status}</div>
                        </div>
                        <div className="mt-2 text-xs text-slate-400">
                          {reservation.reservedQuantity.toLocaleString()}株 / 拘束 ¥{reservation.reservedAmount.toLocaleString()}
                        </div>
                        <div className="mt-1 text-xs text-slate-500">
                          解放 ¥{reservation.releasedAmount.toLocaleString()} / 更新 {new Date(reservation.updatedAt).toLocaleTimeString('ja-JP')}
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="mt-3 text-sm text-slate-500">reservation はありません。</div>
                )}
              </div>

              <div className="rounded-xl border border-slate-800/60 bg-slate-950/50 p-4">
                <div className="text-xs uppercase tracking-[0.2em] text-slate-500">Fills</div>
                {finalOut.fills.length > 0 ? (
                  <div className="mt-3 space-y-3">
                    {finalOut.fills.map((fill) => (
                      <div key={fill.fillId} className="rounded-lg border border-slate-800/60 bg-slate-900/60 p-3">
                        <div className="flex items-center justify-between gap-3">
                          <div className="text-sm font-semibold text-slate-100">{fill.quantity.toLocaleString()}株</div>
                          <div className="text-[11px] text-slate-500">
                            {new Date(fill.filledAt).toLocaleTimeString('ja-JP')}
                          </div>
                        </div>
                        <div className="mt-1 text-xs text-slate-400">
                          {fill.side} / ¥{fill.price.toLocaleString()} / 約定代金 ¥{fill.notional.toLocaleString()}
                        </div>
                        <div className="mt-1 text-xs text-slate-500">Liquidity: {fill.liquidity}</div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="mt-3 text-sm text-slate-500">fill はありません。</div>
                )}
              </div>

              <div className="rounded-xl border border-slate-800/60 bg-slate-950/50 p-4">
                <div className="text-xs uppercase tracking-[0.2em] text-slate-500">Positions</div>
                {finalOut.positions.length > 0 ? (
                  <div className="mt-3 space-y-3">
                    {finalOut.positions.map((position) => (
                      <div key={position.symbol} className="rounded-lg border border-slate-800/60 bg-slate-900/60 p-3">
                        <div className="flex items-center justify-between">
                          <div className="text-sm font-semibold text-slate-100">{position.symbol}</div>
                          <div className="text-xs text-slate-400">{position.quantity.toLocaleString()}株</div>
                        </div>
                        <div className="mt-2 text-xs text-slate-400">
                          平均 ¥{position.avgPrice.toLocaleString()} / 現在 ¥{position.currentPrice.toLocaleString()}
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="mt-3 text-sm text-slate-500">ポジションはありません。</div>
                )}
              </div>

              <div className="rounded-xl border border-slate-800/60 bg-slate-950/50 p-4">
                <div className="text-xs uppercase tracking-[0.2em] text-slate-500">Timeline</div>
                {finalOut.timeline.length > 0 ? (
                  <div className="mt-3 space-y-3">
                    {finalOut.timeline.map((entry) => (
                      <div key={`${entry.eventType}-${entry.eventAt}`} className="rounded-lg border border-slate-800/60 bg-slate-900/60 p-3">
                        <div className="flex items-center justify-between gap-3">
                          <div className="text-sm font-semibold text-slate-100">{entry.label}</div>
                          <div className="text-[11px] text-slate-500">
                            {new Date(entry.eventAt).toLocaleTimeString('ja-JP')}
                          </div>
                        </div>
                        <div className="mt-1 text-xs text-slate-400">{entry.detail}</div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="mt-3 text-sm text-slate-500">イベントはまだありません。</div>
                )}
              </div>

              <div className="rounded-xl border border-slate-800/60 bg-slate-950/50 p-4">
                <div className="text-xs uppercase tracking-[0.2em] text-slate-500">Raw Event Ref</div>
                {finalOut.rawEvents.length > 0 ? (
                  <div className="mt-3 space-y-2">
                    {finalOut.rawEvents.map((event) => (
                      <div key={event.eventRef} className="rounded-lg border border-slate-800/60 bg-slate-900/60 p-3">
                        <div className="flex items-center justify-between gap-3">
                          <div className="text-sm font-semibold text-slate-100">{event.eventType}</div>
                          <div className="text-[11px] text-slate-500">{event.source}</div>
                        </div>
                        <div className="mt-1 text-xs text-slate-400">{event.eventRef}</div>
                        <div className="mt-1 text-[11px] text-slate-500">{event.detail}</div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="mt-3 text-sm text-slate-500">raw event はありません。</div>
                )}
              </div>
            </div>
          ) : (
            <div className="mt-4 text-sm text-slate-500">
              注文を作成すると、その注文の最終Outをここで確認できます。
            </div>
          )}

          <div className="mt-6 border-t border-slate-800/60 pt-6">
            <div className="flex items-center justify-between">
              <h3 className="text-base font-semibold text-slate-100">Ops</h3>
              <span className="text-xs text-slate-500">
                intake / reconcile / ledger
              </span>
            </div>

            {opsOverviewLoading ? (
              <div className="mt-4 text-sm text-slate-500">Ops情報を読み込み中...</div>
            ) : opsOverview ? (
              <div className="mt-4 grid grid-cols-1 gap-4 xl:grid-cols-2 2xl:grid-cols-3">
                <div className="rounded-xl border border-slate-800/60 bg-slate-950/50 p-4">
                  <div className="text-xs uppercase tracking-[0.2em] text-slate-500">OMS Intake</div>
                  {opsOverview.omsStats ? (
                    <div className="mt-3 space-y-2 text-sm text-slate-300">
                      <div>状態: {opsOverview.omsStats.state}</div>
                      <div>処理件数: {opsOverview.omsStats.processed.toLocaleString()}</div>
                      <div>重複: {opsOverview.omsStats.duplicates.toLocaleString()}</div>
                      <div>孤児: {opsOverview.omsStats.orphans.toLocaleString()}</div>
                      <div>Replay回数: {opsOverview.omsStats.replays.toLocaleString()}</div>
                      <div>最終イベント: {formatEventTime(opsOverview.omsStats.lastEventAt)}</div>
                    </div>
                  ) : (
                    <div className="mt-3 text-sm text-slate-500">OMS stats はありません。</div>
                  )}
                  {opsOverview.omsReconcile && (
                    <div className="mt-4 rounded-lg border border-slate-800/60 bg-slate-900/60 p-3 text-xs text-slate-300">
                      <div>open orders: {opsOverview.omsReconcile.openOrders.toLocaleString()}</div>
                      <div>expected reserved: ¥{opsOverview.omsReconcile.expectedReservedAmount.toLocaleString()}</div>
                      <div>actual reserved: ¥{opsOverview.omsReconcile.actualReservedAmount.toLocaleString()}</div>
                      <div>gap: ¥{opsOverview.omsReconcile.reservedGapAmount.toLocaleString()}</div>
                    </div>
                  )}
                </div>

                <div className="rounded-xl border border-slate-800/60 bg-slate-950/50 p-4">
                  <div className="text-xs uppercase tracking-[0.2em] text-slate-500">BackOffice Intake</div>
                  {opsOverview.backOfficeStats ? (
                    <div className="mt-3 space-y-2 text-sm text-slate-300">
                      <div>状態: {opsOverview.backOfficeStats.state}</div>
                      <div>処理件数: {opsOverview.backOfficeStats.processed.toLocaleString()}</div>
                      <div>Ledger件数: {opsOverview.backOfficeStats.ledgerEntryCount.toLocaleString()}</div>
                      <div>重複: {opsOverview.backOfficeStats.duplicates.toLocaleString()}</div>
                      <div>孤児: {opsOverview.backOfficeStats.orphans.toLocaleString()}</div>
                      <div>最終イベント: {formatEventTime(opsOverview.backOfficeStats.lastEventAt)}</div>
                    </div>
                  ) : (
                    <div className="mt-3 text-sm text-slate-500">BackOffice stats はありません。</div>
                  )}
                  {opsOverview.backOfficeReconcile && (
                    <div className="mt-4 rounded-lg border border-slate-800/60 bg-slate-900/60 p-3 text-xs text-slate-300">
                      <div>cash: ¥{opsOverview.backOfficeReconcile.cashBalance.toLocaleString()}</div>
                      <div>expected cash: ¥{opsOverview.backOfficeReconcile.expectedCashBalance.toLocaleString()}</div>
                      <div>reserved: ¥{opsOverview.backOfficeReconcile.reservedBuyingPower.toLocaleString()}</div>
                      <div>expected reserved: ¥{opsOverview.backOfficeReconcile.expectedReservedBuyingPower.toLocaleString()}</div>
                    </div>
                  )}
                </div>

                <div className="rounded-xl border border-slate-800/60 bg-slate-950/50 p-4">
                  <div className="text-xs uppercase tracking-[0.2em] text-slate-500">Issues</div>
                  <div className="mt-3 space-y-2">
                    {[
                      ...(opsOverview.omsReconcile?.issues ?? []),
                      ...(opsOverview.backOfficeReconcile?.issues ?? []),
                    ].length > 0 ? (
                      [
                        ...(opsOverview.omsReconcile?.issues ?? []),
                        ...(opsOverview.backOfficeReconcile?.issues ?? []),
                      ].map((issue) => (
                        <div key={issue} className="rounded-lg border border-rose-500/20 bg-rose-500/10 p-3 text-xs text-rose-100">
                          {issue}
                        </div>
                      ))
                    ) : (
                      <div className="rounded-lg border border-emerald-500/20 bg-emerald-500/10 p-3 text-xs text-emerald-100">
                        issue はありません。
                      </div>
                    )}
                  </div>
                  {replayGatewayAudit.isSuccess && replayGatewayAudit.data && (
                    <div className="mt-4 rounded-lg border border-amber-500/20 bg-amber-500/10 p-3 text-xs text-amber-100">
                      OMS replay: {replayGatewayAudit.data.oms?.status ?? 'N/A'} / BackOffice replay: {replayGatewayAudit.data.backOffice?.status ?? 'N/A'}
                    </div>
                  )}
                </div>

                <div className="rounded-xl border border-slate-800/60 bg-slate-950/50 p-4 xl:col-span-2 2xl:col-span-3">
                  <div className="text-xs uppercase tracking-[0.2em] text-slate-500">Ledger</div>
                  {opsOverview.ledgerEntries.length > 0 ? (
                    <div className="mt-3 space-y-3">
                      {opsOverview.ledgerEntries.map((entry) => (
                        <div key={entry.entryId} className="rounded-lg border border-slate-800/60 bg-slate-900/60 p-3">
                          <div className="flex items-center justify-between gap-3">
                            <div className="text-sm font-semibold text-slate-100">{entry.eventType}</div>
                            <div className="text-[11px] text-slate-500">
                              {new Date(entry.eventAt).toLocaleTimeString('ja-JP')}
                            </div>
                          </div>
                          <div className="mt-1 text-xs text-slate-400">{entry.detail}</div>
                          <div className="mt-2 grid grid-cols-2 gap-2 text-[11px] text-slate-500 xl:grid-cols-4">
                            <div>qty delta: {entry.quantityDelta.toLocaleString()}</div>
                            <div>cash delta: ¥{entry.cashDelta.toLocaleString()}</div>
                            <div>reserve delta: ¥{entry.reservedBuyingPowerDelta.toLocaleString()}</div>
                            <div>realized pnl delta: ¥{entry.realizedPnlDelta.toLocaleString()}</div>
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div className="mt-3 text-sm text-slate-500">ledger はまだありません。</div>
                  )}
                </div>
              </div>
            ) : (
              <div className="mt-4 text-sm text-slate-500">Ops情報を取得できませんでした。</div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
