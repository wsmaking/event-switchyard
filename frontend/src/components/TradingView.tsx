import { useState } from 'react';
import { useOrders, useSubmitOrder } from '../hooks/useOrders';
import { useStockInfo, usePositions, usePriceHistory } from '../hooks/useMarketData';
import { OrderSide, OrderType, OrderStatus } from '../types/trading';
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

  // フォーム状態
  const [symbol, setSymbol] = useState('7203'); // トヨタ自動車
  const [side, setSide] = useState<OrderSide>(OrderSide.BUY);
  const [orderType, setOrderType] = useState<OrderType>(OrderType.MARKET);
  const [quantity, setQuantity] = useState(100);
  const [price, setPrice] = useState<number | null>(null);

  // 現在の銘柄情報を取得
  const { data: stockInfo } = useStockInfo(symbol);
  const {
    data: priceHistory,
    isLoading: historyLoading,
    isError: historyError,
  } = usePriceHistory(symbol);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();

    const request: OrderRequest = {
      symbol,
      side,
      type: orderType,
      quantity,
      price: orderType === OrderType.LIMIT ? price : null,
    };

    submitOrder.mutate(request);

    // フォームリセット（銘柄・数量は保持）
    setOrderType(OrderType.MARKET);
    setPrice(null);
  };

  const handleDemoOrder = () => {
    const demoRequest: OrderRequest = {
      symbol: symbol || '7203',
      side: OrderSide.BUY,
      type: OrderType.MARKET,
      quantity: 100,
      price: null,
    };
    submitOrder.mutate(demoRequest);
  };

  const apiErrors = [ordersErrorObj, positionsErrorObj]
    .filter(Boolean)
    .map((err) => (err instanceof Error ? err.message : String(err)))
    .join(' / ');

  const showEmptyOrders = !ordersLoading && !ordersError && (!orders || orders.length === 0);
  const showEmptyPositions = !positionsLoading && !positionsError && (!positions || positions.length === 0);
  const historyPoints = priceHistory ?? [];

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
                  disabled={submitOrder.isPending}
                  className="w-full rounded-md bg-blue-500/80 py-3 font-semibold text-slate-100 transition hover:bg-blue-400/90 disabled:cursor-not-allowed disabled:bg-slate-700"
                >
                  {submitOrder.isPending ? '送信中...' : '注文を送信'}
                </button>

                <button
                  type="button"
                  onClick={handleDemoOrder}
                  disabled={submitOrder.isPending}
                  className="w-full rounded-md border border-slate-700/70 py-2 font-semibold text-slate-300 transition hover:bg-slate-900/60 disabled:bg-slate-900 disabled:text-slate-500"
                >
                  デモ注文を1件作成
                </button>

                {submitOrder.isError && (
                  <div className="text-sm text-rose-300">
                    注文に失敗しました: {submitOrder.error.message}
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
                        <tr key={order.id} className="hover:bg-slate-900/40">
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
                            {order.quantity.toLocaleString()}
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm text-slate-300">
                            {order.price ? `¥${order.price.toLocaleString()}` : '-'}
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm">
                            <span
                              className={`px-2 py-1 rounded-md font-medium ${
                                order.status === OrderStatus.FILLED
                                  ? 'bg-emerald-500/20 text-emerald-200'
                                  : order.status === OrderStatus.REJECTED
                                  ? 'bg-rose-500/20 text-rose-200'
                                  : 'bg-amber-500/20 text-amber-200'
                              }`}
                            >
                              {order.status === OrderStatus.FILLED
                                ? '約定済'
                                : order.status === OrderStatus.REJECTED
                                ? '失敗'
                                : '待機中'}
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
      </div>
    </div>
  );
}
