package app.fast

import org.agrona.collections.Int2ObjectHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * 簡易オーダーブックエンジン (HFT性能検証用プロトタイプ)
 *
 * 目的:
 * - Phase 4 (ベンチマーク) での現実的な負荷生成
 * - Fast Path性能測定の業務ロジック
 *
 * 設計方針:
 * - ロックフリー (単一スレッドアクセス前提)
 * - 価格レベルごとにオーダーキュー管理
 * - 価格優先・時間優先のマッチング
 * - Immutable data class使用（JVM最適化を最大限活用）
 */
class OrderBook(
    private val symbolId: Int
) {
    // 価格レベル -> オーダーリスト
    private val buyOrders = Int2ObjectHashMap<MutableList<Order>>()
    private val sellOrders = Int2ObjectHashMap<MutableList<Order>>()

    // メトリクス
    private val orderCount = AtomicLong(0)
    private val matchCount = AtomicLong(0)

    /**
     * 買い注文を処理
     * @return マッチした場合はExecution、なければnull
     */
    fun processBuyOrder(price: Int, quantity: Int, timestamp: Long): Execution? {
        orderCount.incrementAndGet()

        // 最良売り注文とマッチング試行
        val bestSell = findBestSell(price)
        if (bestSell != null) {
            matchCount.incrementAndGet()
            return Execution(
                symbolId = symbolId,
                price = bestSell.price,
                quantity = minOf(quantity, bestSell.quantity),
                buyTimestamp = timestamp,
                sellTimestamp = bestSell.timestamp,
                executionTimestamp = System.nanoTime()
            )
        }

        // マッチしない場合はオーダーブックに追加
        addBuyOrder(price, quantity, timestamp)
        return null
    }

    /**
     * 売り注文を処理
     * @return マッチした場合はExecution、なければnull
     */
    fun processSellOrder(price: Int, quantity: Int, timestamp: Long): Execution? {
        orderCount.incrementAndGet()

        // 最良買い注文とマッチング試行
        val bestBuy = findBestBuy(price)
        if (bestBuy != null) {
            matchCount.incrementAndGet()
            return Execution(
                symbolId = symbolId,
                price = bestBuy.price,
                quantity = minOf(quantity, bestBuy.quantity),
                buyTimestamp = bestBuy.timestamp,
                sellTimestamp = timestamp,
                executionTimestamp = System.nanoTime()
            )
        }

        // マッチしない場合はオーダーブックに追加
        addSellOrder(price, quantity, timestamp)
        return null
    }

    private fun findBestSell(maxPrice: Int): Order? {
        // 売り注文から最低価格を探す (価格優先)
        var bestPrice = Int.MAX_VALUE
        var bestOrder: Order? = null

        val keys = sellOrders.keys
        keys.forEach { priceObj ->
            val price = priceObj as Int
            if (price <= maxPrice && price < bestPrice) {
                val orders = sellOrders[price]
                if (orders != null && orders.isNotEmpty()) {
                    bestPrice = price
                    bestOrder = orders.removeAt(0)  // FIFO (時間優先)
                    if (orders.isEmpty()) {
                        sellOrders.remove(price)
                    }
                }
            }
        }

        return bestOrder
    }

    private fun findBestBuy(minPrice: Int): Order? {
        // 買い注文から最高価格を探す (価格優先)
        var bestPrice = Int.MIN_VALUE
        var bestOrder: Order? = null

        val keys = buyOrders.keys
        keys.forEach { priceObj ->
            val price = priceObj as Int
            if (price >= minPrice && price > bestPrice) {
                val orders = buyOrders[price]
                if (orders != null && orders.isNotEmpty()) {
                    bestPrice = price
                    bestOrder = orders.removeAt(0)  // FIFO (時間優先)
                    if (orders.isEmpty()) {
                        buyOrders.remove(price)
                    }
                }
            }
        }

        return bestOrder
    }

    private fun addBuyOrder(price: Int, quantity: Int, timestamp: Long) {
        val order = Order(price, quantity, timestamp)
        buyOrders.computeIfAbsent(price) { mutableListOf() }.add(order)
    }

    private fun addSellOrder(price: Int, quantity: Int, timestamp: Long) {
        val order = Order(price, quantity, timestamp)
        sellOrders.computeIfAbsent(price) { mutableListOf() }.add(order)
    }

    fun getStats(): OrderBookStats {
        return OrderBookStats(
            symbolId = symbolId,
            orderCount = orderCount.get(),
            matchCount = matchCount.get(),
            buyLevels = buyOrders.size,
            sellLevels = sellOrders.size
        )
    }
}

/**
 * 注文 (Immutable data class - JVM最適化を最大限活用)
 */
data class Order(
    val price: Int,
    val quantity: Int,
    val timestamp: Long
)

/**
 * 約定
 */
data class Execution(
    val symbolId: Int,
    val price: Int,
    val quantity: Int,
    val buyTimestamp: Long,
    val sellTimestamp: Long,
    val executionTimestamp: Long
) {
    fun latencyNs() = executionTimestamp - minOf(buyTimestamp, sellTimestamp)
}

/**
 * オーダーブック統計
 */
data class OrderBookStats(
    val symbolId: Int,
    val orderCount: Long,
    val matchCount: Long,
    val buyLevels: Int,
    val sellLevels: Int
) {
    fun matchRate() = if (orderCount > 0) matchCount.toDouble() / orderCount else 0.0
}
