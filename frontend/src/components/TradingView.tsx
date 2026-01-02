import { useState } from 'react';
import { useOrders, useSubmitOrder } from '../hooks/useOrders';
import { useStockInfo, usePositions } from '../hooks/useMarketData';
import { OrderSide, OrderType, OrderStatus } from '../types/trading';
import type { OrderRequest } from '../types/trading';

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

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      <div className="max-w-7xl mx-auto">
        <h1 className="text-3xl font-bold text-gray-900 mb-8">証券取引システム</h1>

        {(ordersError || positionsError) && (
          <div className="mb-6 rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700">
            APIに接続できません。App/Gateway/BackOffice が起動しているか確認してください。
            {apiErrors && <div className="mt-1 text-xs text-red-600">詳細: {apiErrors}</div>}
          </div>
        )}

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* 注文入力フォーム */}
          <div className="lg:col-span-1">
            <div className="bg-white rounded-lg shadow p-6">
              <h2 className="text-xl font-semibold text-gray-900 mb-4">新規注文</h2>

              {/* 銘柄情報表示 */}
              {stockInfo && (
                <div className="mb-4 p-3 bg-blue-50 rounded-md">
                  <div className="text-sm text-gray-600">{stockInfo.name}</div>
                  <div className="flex items-baseline space-x-2">
                    <span className="text-2xl font-bold text-gray-900">
                      ¥{stockInfo.currentPrice.toLocaleString()}
                    </span>
                    <span
                      className={`text-sm font-medium ${
                        stockInfo.change >= 0 ? 'text-green-600' : 'text-red-600'
                      }`}
                    >
                      {stockInfo.change >= 0 ? '+' : ''}
                      {stockInfo.change.toFixed(2)} ({stockInfo.changePercent.toFixed(2)}%)
                    </span>
                  </div>
                  <div className="text-xs text-gray-500 mt-1">
                    高値: ¥{stockInfo.high.toLocaleString()} / 安値: ¥{stockInfo.low.toLocaleString()}
                  </div>
                </div>
              )}

              <form onSubmit={handleSubmit} className="space-y-4">
                {/* 銘柄コード */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    銘柄コード
                  </label>
                  <input
                    type="text"
                    value={symbol}
                    onChange={(e) => setSymbol(e.target.value)}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                    placeholder="例: 7203"
                    required
                  />
                </div>

                {/* 売買区分 */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    売買区分
                  </label>
                  <div className="grid grid-cols-2 gap-2">
                    <button
                      type="button"
                      onClick={() => setSide(OrderSide.BUY)}
                      className={`px-4 py-2 rounded-md font-medium ${
                        side === OrderSide.BUY
                          ? 'bg-red-500 text-white'
                          : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                      }`}
                    >
                      買い
                    </button>
                    <button
                      type="button"
                      onClick={() => setSide(OrderSide.SELL)}
                      className={`px-4 py-2 rounded-md font-medium ${
                        side === OrderSide.SELL
                          ? 'bg-blue-500 text-white'
                          : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                      }`}
                    >
                      売り
                    </button>
                  </div>
                </div>

                {/* 注文種別 */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    注文種別
                  </label>
                  <select
                    value={orderType}
                    onChange={(e) => setOrderType(e.target.value as OrderType)}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                  >
                    <option value={OrderType.MARKET}>成行（即座に約定）</option>
                    <option value={OrderType.LIMIT}>指値（価格指定）</option>
                  </select>
                  <p className="text-xs text-gray-500 mt-1">
                    {orderType === OrderType.MARKET
                      ? '現在価格で即座に約定します（実行時間: 0.5-2.5ms）'
                      : '指定価格に達したら約定します（実行時間: 5-15ms）'}
                  </p>
                </div>

                {/* 数量 */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    数量（株）
                  </label>
                  <input
                    type="number"
                    value={quantity}
                    onChange={(e) => setQuantity(Number(e.target.value))}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                    min="1"
                    step="1"
                    required
                  />
                </div>

                {/* 指値価格 */}
                {orderType === OrderType.LIMIT && (
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      指値価格（円）
                    </label>
                    <input
                      type="number"
                      value={price || ''}
                      onChange={(e) => setPrice(Number(e.target.value))}
                      className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                      step="0.01"
                      required
                    />
                  </div>
                )}

                {/* 送信ボタン */}
                <button
                  type="submit"
                  disabled={submitOrder.isPending}
                  className="w-full bg-blue-600 text-white py-3 rounded-md font-semibold hover:bg-blue-700 disabled:bg-gray-300 disabled:cursor-not-allowed transition"
                >
                  {submitOrder.isPending ? '送信中...' : '注文を送信'}
                </button>

                <button
                  type="button"
                  onClick={handleDemoOrder}
                  disabled={submitOrder.isPending}
                  className="w-full border border-gray-300 text-gray-700 py-2 rounded-md font-semibold hover:bg-gray-50 disabled:bg-gray-100 disabled:text-gray-400 transition"
                >
                  デモ注文を1件作成
                </button>

                {submitOrder.isError && (
                  <div className="text-red-600 text-sm">
                    注文に失敗しました: {submitOrder.error.message}
                  </div>
                )}
              </form>
            </div>
          </div>

          {/* 右側: 注文履歴と保有銘柄 */}
          <div className="lg:col-span-2 space-y-6">
            {/* 保有銘柄 */}
            <div className="bg-white rounded-lg shadow p-6">
              <h2 className="text-xl font-semibold text-gray-900 mb-4">保有銘柄</h2>

              {positionsLoading && !positionsError ? (
                <div className="text-gray-500 text-sm">読み込み中...</div>
              ) : positions && positions.length > 0 ? (
                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                          銘柄
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                          数量
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                          平均取得価格
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                          現在価格
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                          評価損益
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                          損益率
                        </th>
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {positions.map((position) => (
                        <tr key={position.symbol} className="hover:bg-gray-50">
                          <td className="px-4 py-3 whitespace-nowrap text-sm font-medium text-gray-900">
                            {position.symbol}
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-700">
                            {position.quantity.toLocaleString()}株
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-700">
                            ¥{position.avgPrice.toLocaleString()}
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-700">
                            ¥{position.currentPrice.toLocaleString()}
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm">
                            <span
                              className={`font-medium ${
                                position.unrealizedPnL >= 0 ? 'text-green-600' : 'text-red-600'
                              }`}
                            >
                              {position.unrealizedPnL >= 0 ? '+' : ''}
                              ¥{position.unrealizedPnL.toLocaleString()}
                            </span>
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm">
                            <span
                              className={`font-medium ${
                                position.unrealizedPnLPercent >= 0 ? 'text-green-600' : 'text-red-600'
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
                <div className="text-gray-500 text-sm">
                  保有銘柄がありません。注文を出すか、自動発注を有効にしてください。
                </div>
              ) : (
                <div className="text-gray-500 text-sm">保有銘柄を取得できませんでした</div>
              )}
            </div>

            {/* 注文履歴 */}
            <div className="bg-white rounded-lg shadow p-6">
              <h2 className="text-xl font-semibold text-gray-900 mb-4">注文履歴</h2>

              {ordersLoading && !ordersError ? (
                <div className="text-gray-500">読み込み中...</div>
              ) : orders && orders.length > 0 ? (
                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          時刻
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          銘柄
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          売買
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          種別
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          数量
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          価格
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          ステータス
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          約定時間
                        </th>
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {orders.map((order) => (
                        <tr key={order.id} className="hover:bg-gray-50">
                          <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-900">
                            {new Date(order.submittedAt).toLocaleTimeString('ja-JP')}
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm font-medium text-gray-900">
                            {order.symbol}
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm">
                            <span
                              className={`px-2 py-1 rounded-md font-medium ${
                                order.side === OrderSide.BUY
                                  ? 'bg-red-100 text-red-700'
                                  : 'bg-blue-100 text-blue-700'
                              }`}
                            >
                              {order.side === OrderSide.BUY ? '買' : '売'}
                            </span>
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-700">
                            {order.type === OrderType.MARKET ? '成行' : '指値'}
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-700">
                            {order.quantity.toLocaleString()}
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-700">
                            {order.price ? `¥${order.price.toLocaleString()}` : '-'}
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm">
                            <span
                              className={`px-2 py-1 rounded-md font-medium ${
                                order.status === OrderStatus.FILLED
                                  ? 'bg-green-100 text-green-700'
                                  : order.status === OrderStatus.REJECTED
                                  ? 'bg-red-100 text-red-700'
                                  : 'bg-yellow-100 text-yellow-700'
                              }`}
                            >
                              {order.status === OrderStatus.FILLED
                                ? '約定済'
                                : order.status === OrderStatus.REJECTED
                                ? '失敗'
                                : '待機中'}
                            </span>
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-700">
                            {order.executionTimeMs ? `${order.executionTimeMs.toFixed(2)}ms` : '-'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : showEmptyOrders ? (
                <div className="text-gray-500">
                  注文履歴がありません。右のフォームから注文を作成してください。
                </div>
              ) : (
                <div className="text-gray-500">注文履歴を取得できませんでした</div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
