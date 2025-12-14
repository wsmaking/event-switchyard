package gateway.order

import gateway.exchange.ExecutionReport
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

class InMemoryOrderStore {
    private val byId = ConcurrentHashMap<String, OrderSnapshot>()
    private val idempotencyIndex = ConcurrentHashMap<String, String>() // idempotencyKey -> orderId

    fun findById(orderId: String): OrderSnapshot? = byId[orderId]

    fun findByIdempotencyKey(key: String): OrderSnapshot? {
        val orderId = idempotencyIndex[key] ?: return null
        return byId[orderId]
    }

    fun put(order: OrderSnapshot, idempotencyKey: String?) {
        byId[order.orderId] = order
        if (idempotencyKey != null) {
            idempotencyIndex.putIfAbsent(idempotencyKey, order.orderId)
        }
    }

    fun remove(orderId: String, idempotencyKey: String?) {
        byId.remove(orderId)
        if (idempotencyKey != null) {
            idempotencyIndex.remove(idempotencyKey, orderId)
        }
    }

    fun update(orderId: String, fn: (OrderSnapshot) -> OrderSnapshot): OrderSnapshot? {
        return byId.computeIfPresent(orderId) { _, prev -> fn(prev) }
    }

    fun applyExecutionReport(report: ExecutionReport): OrderSnapshot? {
        return byId.computeIfPresent(report.orderId) { _, prev ->
            val now: Instant = report.at
            val nextStatus = report.status
            val nextFilled = maxOf(prev.filledQty, report.filledQtyTotal)
            prev.copy(
                status = nextStatus,
                filledQty = nextFilled,
                lastUpdateAt = now
            )
        }
    }
}
