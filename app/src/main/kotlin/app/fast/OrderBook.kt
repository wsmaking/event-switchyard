package app.fast

import org.agrona.collections.Int2ObjectHashMap
import org.agrona.collections.IntArrayList
import java.util.concurrent.atomic.AtomicLong

/**
 * 簡易オーダーブックエンジン (HFT性能検証用プロトタイプ)
 *
 * 目的:
 * - Phase 2 (Zero-GC最適化) の対象特定
 * - Phase 4 (ベンチマーク) での現実的な負荷生成
 *
 * 設計方針:
 * - ロックフリー (単一スレッドアクセス前提)
 * - 価格レベルごとにオーダーキュー管理
 * - 価格優先・時間優先のマッチング
 * - Phase 2最適化: Object Poolでallocation削減
 */
class OrderBook(
    private val symbolId: Int,
    private val orderPool: OrderPool = OrderPool(),
    private val enablePooling: Boolean = System.getenv("ORDER_POOL_ENABLE") == "1"
) {
    // 価格レベル -> オーダーリスト
    // Phase 2最適化: MutableList<Order> -> ArrayList<MutableOrder>
    private val buyOrders = Int2ObjectHashMap<ArrayList<MutableOrder>>()
    private val sellOrders = Int2ObjectHashMap<ArrayList<MutableOrder>>()

    // メトリクス
    private val orderCount = AtomicLong(0)
    private val matchCount = AtomicLong(0)
    private val poolMissCount = AtomicLong(0)

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
            val execution = Execution(
                symbolId = symbolId,
                price = bestSell.price,
                quantity = minOf(quantity, bestSell.quantity),
                buyTimestamp = timestamp,
                sellTimestamp = bestSell.timestamp,
                executionTimestamp = System.nanoTime()
            )
            // マッチしたオーダーをプールに返却
            if (enablePooling) {
                orderPool.release(bestSell)
            }
            return execution
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
            val execution = Execution(
                symbolId = symbolId,
                price = bestBuy.price,
                quantity = minOf(quantity, bestBuy.quantity),
                buyTimestamp = bestBuy.timestamp,
                sellTimestamp = timestamp,
                executionTimestamp = System.nanoTime()
            )
            // マッチしたオーダーをプールに返却
            if (enablePooling) {
                orderPool.release(bestBuy)
            }
            return execution
        }

        // マッチしない場合はオーダーブックに追加
        addSellOrder(price, quantity, timestamp)
        return null
    }

    private fun findBestSell(maxPrice: Int): MutableOrder? {
        // 売り注文から最低価格を探す (価格優先)
        var bestPrice = Int.MAX_VALUE
        var bestOrder: MutableOrder? = null

        val keys = sellOrders.keys
        keys.forEach { priceObj ->
            val price = priceObj as Int
            if (price <= maxPrice && price < bestPrice) {
                val orders = sellOrders[price]
                if (orders != null && orders.isNotEmpty()) {
                    bestPrice = price
                    bestOrder = orders.removeAt(0)  // 時間優先 (FIFO)
                    if (orders.isEmpty()) {
                        sellOrders.remove(price)
                    }
                }
            }
        }

        return bestOrder
    }

    private fun findBestBuy(minPrice: Int): MutableOrder? {
        // 買い注文から最高価格を探す (価格優先)
        var bestPrice = Int.MIN_VALUE
        var bestOrder: MutableOrder? = null

        val keys = buyOrders.keys
        keys.forEach { priceObj ->
            val price = priceObj as Int
            if (price >= minPrice && price > bestPrice) {
                val orders = buyOrders[price]
                if (orders != null && orders.isNotEmpty()) {
                    bestPrice = price
                    bestOrder = orders.removeAt(0)  // 時間優先 (FIFO)
                    if (orders.isEmpty()) {
                        buyOrders.remove(price)
                    }
                }
            }
        }

        return bestOrder
    }

    private fun addBuyOrder(price: Int, quantity: Int, timestamp: Long) {
        val order = if (enablePooling) {
            orderPool.tryAcquire()?.also { it.set(price, quantity, timestamp) }
                ?: run {
                    poolMissCount.incrementAndGet()
                    MutableOrder(price, quantity, timestamp)
                }
        } else {
            MutableOrder(price, quantity, timestamp)
        }
        buyOrders.computeIfAbsent(price) { ArrayList() }.add(order)
    }

    private fun addSellOrder(price: Int, quantity: Int, timestamp: Long) {
        val order = if (enablePooling) {
            orderPool.tryAcquire()?.also { it.set(price, quantity, timestamp) }
                ?: run {
                    poolMissCount.incrementAndGet()
                    MutableOrder(price, quantity, timestamp)
                }
        } else {
            MutableOrder(price, quantity, timestamp)
        }
        sellOrders.computeIfAbsent(price) { ArrayList() }.add(order)
    }

    fun getStats(): OrderBookStats {
        return OrderBookStats(
            symbolId = symbolId,
            orderCount = orderCount.get(),
            matchCount = matchCount.get(),
            buyLevels = buyOrders.size,
            sellLevels = sellOrders.size,
            poolMissCount = poolMissCount.get(),
            poolAvailable = if (enablePooling) orderPool.available() else 0,
            poolUsageRatio = if (enablePooling) orderPool.usageRatio() else 0.0
        )
    }
}

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
    val sellLevels: Int,
    val poolMissCount: Long,
    val poolAvailable: Int,
    val poolUsageRatio: Double
) {
    fun matchRate() = if (orderCount > 0) matchCount.toDouble() / orderCount else 0.0
}
