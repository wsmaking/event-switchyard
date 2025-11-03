package app.fast

import java.util.concurrent.atomic.AtomicInteger

/**
 * Object Pool for Order Reuse (Phase 2: Zero-GC Optimization)
 *
 * 目的:
 * - Order allocation GCを削減
 * - マッチング後のOrderオブジェクトを再利用
 *
 * 設計方針:
 * - Fixed-size pool (preallocate)
 * - Lock-free (単一スレッド前提、indexカウンターのみatomic)
 * - シンプルな配列ベース実装
 */
class OrderPool(
    private val capacity: Int = 10_000
) {
    private val pool = Array(capacity) { MutableOrder() }
    private val index = AtomicInteger(capacity)

    /**
     * プールからOrderを取得 (ノンブロッキング)
     * @return 取得成功でMutableOrder、プール枯渇でnull
     */
    fun tryAcquire(): MutableOrder? {
        val idx = index.decrementAndGet()
        return if (idx >= 0) pool[idx] else run {
            index.incrementAndGet()  // Restore
            null
        }
    }

    /**
     * Orderをプールに返却
     */
    fun release(order: MutableOrder) {
        order.clear()
        val idx = index.getAndIncrement()
        if (idx < capacity) {
            pool[idx] = order
        }
        // idx >= capacity の場合は無視 (プールが満杯)
    }

    /**
     * 現在のプール内のOrder数
     */
    fun available(): Int = index.get().coerceIn(0, capacity)

    /**
     * プール使用率 (0.0 ~ 1.0)
     */
    fun usageRatio(): Double = 1.0 - (available().toDouble() / capacity)
}

/**
 * Mutable Order (Object Pool用)
 *
 * data classではなくmutable classを使用してGC allocを回避
 */
class MutableOrder(
    var price: Int = 0,
    var quantity: Int = 0,
    var timestamp: Long = 0
) {
    fun set(price: Int, quantity: Int, timestamp: Long) {
        this.price = price
        this.quantity = quantity
        this.timestamp = timestamp
    }

    fun clear() {
        this.price = 0
        this.quantity = 0
        this.timestamp = 0
    }

    fun copyTo(dest: MutableOrder) {
        dest.price = this.price
        dest.quantity = this.quantity
        dest.timestamp = this.timestamp
    }

    override fun toString(): String =
        "MutableOrder(price=$price, quantity=$quantity, timestamp=$timestamp)"
}

/**
 * Execution Pool (約定結果の再利用)
 */
class ExecutionPool(
    private val capacity: Int = 5_000
) {
    private val pool = Array(capacity) { MutableExecution() }
    private val index = AtomicInteger(capacity)

    fun tryAcquire(): MutableExecution? {
        val idx = index.decrementAndGet()
        return if (idx >= 0) pool[idx] else run {
            index.incrementAndGet()
            null
        }
    }

    fun release(execution: MutableExecution) {
        execution.clear()
        val idx = index.getAndIncrement()
        if (idx < capacity) {
            pool[idx] = execution
        }
    }

    fun available(): Int = index.get().coerceIn(0, capacity)

    fun usageRatio(): Double = 1.0 - (available().toDouble() / capacity)
}

/**
 * Mutable Execution (Object Pool用)
 */
class MutableExecution(
    var symbolId: Int = 0,
    var price: Int = 0,
    var quantity: Int = 0,
    var buyTimestamp: Long = 0,
    var sellTimestamp: Long = 0,
    var executionTimestamp: Long = 0
) {
    fun set(
        symbolId: Int,
        price: Int,
        quantity: Int,
        buyTimestamp: Long,
        sellTimestamp: Long,
        executionTimestamp: Long
    ) {
        this.symbolId = symbolId
        this.price = price
        this.quantity = quantity
        this.buyTimestamp = buyTimestamp
        this.sellTimestamp = sellTimestamp
        this.executionTimestamp = executionTimestamp
    }

    fun clear() {
        this.symbolId = 0
        this.price = 0
        this.quantity = 0
        this.buyTimestamp = 0
        this.sellTimestamp = 0
        this.executionTimestamp = 0
    }

    fun latencyNs() = executionTimestamp - minOf(buyTimestamp, sellTimestamp)

    override fun toString(): String =
        "MutableExecution(symbolId=$symbolId, price=$price, quantity=$quantity, " +
        "buyTimestamp=$buyTimestamp, sellTimestamp=$sellTimestamp, executionTimestamp=$executionTimestamp)"
}
