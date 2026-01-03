package app.fast.handlers

import app.events.OrderEvent
import app.order.OrderExecutionService
import app.strategy.StrategyConfig
import app.strategy.StrategyConfigService
import com.lmax.disruptor.EventHandler

// Fast Pathのイベントを「注文を送る処理」に渡すための橋渡し。
// 「注文を決める」処理と「注文を送る」処理を切り分け、自動戦略の安全柵をここに集約する。
class OrderSubmissionHandler(
    private val executionService: OrderExecutionService,
    private val configService: StrategyConfigService
) : EventHandler<RingBufferEvent> {
    private val recentOrderTimesMs = ArrayDeque<Long>()
    private val lastOrderAtBySymbol = HashMap<String, Long>()

    override fun onEvent(event: RingBufferEvent, sequence: Long, endOfBatch: Boolean) {
        if (event.eventType != "Order") return
        val order = event.payload as? OrderEvent ?: return

        val config = configService.current()
        if (!config.enabled) {
            event.eventType = "OrderFiltered"
            return
        }
        if (isCoolingDown(order.symbol, config)) {
            event.eventType = "OrderFiltered"
            return
        }
        if (isRateLimited(config)) {
            event.eventType = "OrderFiltered"
            return
        }

        val now = System.currentTimeMillis()
        recentOrderTimesMs.addLast(now)
        lastOrderAtBySymbol[order.symbol] = now

        val result = executionService.submitOrder(order)
        event.eventType = if (result.accepted) "OrderSubmitted" else "OrderRejected"
    }

    private fun isRateLimited(config: StrategyConfig): Boolean {
        val maxPerMin = config.maxOrdersPerMin
        if (maxPerMin <= 0) return false
        val now = System.currentTimeMillis()
        val cutoff = now - 60_000
        while (recentOrderTimesMs.isNotEmpty() && recentOrderTimesMs.first() < cutoff) {
            recentOrderTimesMs.removeFirst()
        }
        return recentOrderTimesMs.size >= maxPerMin
    }

    private fun isCoolingDown(symbol: String, config: StrategyConfig): Boolean {
        val cooldownMs = config.cooldownMs
        if (cooldownMs <= 0) return false
        val last = lastOrderAtBySymbol[symbol] ?: return false
        return System.currentTimeMillis() - last < cooldownMs
    }
}
