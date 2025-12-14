package gateway.order

import gateway.exchange.ExecutionReport
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

class InMemoryOrderStore {
    private val byId = ConcurrentHashMap<String, OrderSnapshot>()
    private val idempotencyIndex = ConcurrentHashMap<String, String>() // accountId+key -> orderId

    fun findById(orderId: String): OrderSnapshot? = byId[orderId]

    fun findByIdempotencyKey(accountId: String, key: String): OrderSnapshot? {
        val orderId = idempotencyIndex[idempotencyIndexKey(accountId, key)] ?: return null
        return byId[orderId]
    }

    fun put(order: OrderSnapshot, idempotencyKey: String?) {
        byId[order.orderId] = order
        if (idempotencyKey != null) {
            idempotencyIndex.putIfAbsent(idempotencyIndexKey(order.accountId, idempotencyKey), order.orderId)
        }
    }

    fun remove(orderId: String, idempotencyKey: String?) {
        val order = byId.remove(orderId)
        if (idempotencyKey != null) {
            val accountId = order?.accountId ?: return
            idempotencyIndex.remove(idempotencyIndexKey(accountId, idempotencyKey), orderId)
        }
    }

    fun update(orderId: String, fn: (OrderSnapshot) -> OrderSnapshot): OrderSnapshot? {
        return byId.computeIfPresent(orderId) { _, prev -> fn(prev) }
    }

    fun applyExecutionReport(report: ExecutionReport): OrderSnapshot? {
        return byId.computeIfPresent(report.orderId) { _, prev ->
            val now: Instant = report.at
            val nextFilled = maxOf(prev.filledQty, report.filledQtyTotal)
            val isTerminal = prev.status == OrderStatus.FILLED || prev.status == OrderStatus.CANCELED || prev.status == OrderStatus.REJECTED
            if (isTerminal) {
                return@computeIfPresent prev.copy(lastUpdateAt = now)
            }

            val nextStatus = when (report.status) {
                OrderStatus.PARTIALLY_FILLED ->
                    if (nextFilled >= prev.qty) OrderStatus.FILLED else OrderStatus.PARTIALLY_FILLED
                OrderStatus.FILLED -> OrderStatus.FILLED
                OrderStatus.CANCELED -> OrderStatus.CANCELED
                OrderStatus.REJECTED -> {
                    // Guard: if we already observed any fills, don't downgrade the order into REJECTED.
                    if (nextFilled > 0) prev.status else OrderStatus.REJECTED
                }
                else -> report.status
            }

            prev.copy(status = nextStatus, filledQty = nextFilled, lastUpdateAt = now)
        }
    }

    private fun idempotencyIndexKey(accountId: String, idempotencyKey: String): String = "$accountId::$idempotencyKey"
}
