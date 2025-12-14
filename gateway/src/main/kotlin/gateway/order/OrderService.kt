package gateway.order

import gateway.audit.AuditEvent
import gateway.audit.AuditLog
import gateway.risk.PreTradeRisk
import gateway.queue.FastPathQueue
import gateway.queue.OrderCommand
import java.time.Instant
import java.util.UUID

sealed interface AcceptOrderResult {
    val httpStatus: Int

    data class Accepted(val orderId: String) : AcceptOrderResult {
        override val httpStatus: Int = 202
    }

    data class Rejected(val reason: String, override val httpStatus: Int) : AcceptOrderResult
}

class OrderService(
    private val orderStore: InMemoryOrderStore,
    private val auditLog: AuditLog,
    private val risk: PreTradeRisk,
    private val fastPathQueue: FastPathQueue
) {
    fun acceptOrder(req: CreateOrderRequest, idempotencyKey: String?): AcceptOrderResult {
        if (idempotencyKey != null) {
            val existing = orderStore.findByIdempotencyKey(idempotencyKey)
            if (existing != null) return AcceptOrderResult.Accepted(existing.orderId)
        }

        val validationError = validate(req)
        if (validationError != null) return AcceptOrderResult.Rejected(validationError, 422)

        val riskResult = risk.validate(req)
        if (!riskResult.ok) {
            return AcceptOrderResult.Rejected(riskResult.reason ?: "RISK_REJECT", 422)
        }

        val now = Instant.now()
        val orderId = "ord_${UUID.randomUUID()}"
        val snapshot = OrderSnapshot(
            orderId = orderId,
            clientOrderId = req.clientOrderId,
            symbol = req.symbol,
            side = req.side,
            type = req.type,
            qty = req.qty,
            price = req.price,
            status = OrderStatus.ACCEPTED,
            acceptedAt = now,
            lastUpdateAt = now,
            filledQty = 0
        )
        orderStore.put(snapshot, idempotencyKey)

        val enqueue = fastPathQueue.tryEnqueue(OrderCommand(snapshot))
        if (!enqueue.ok) {
            orderStore.remove(orderId, idempotencyKey)
            return AcceptOrderResult.Rejected(enqueue.reason ?: "QUEUE_REJECT", 503)
        }

        auditLog.append(
            AuditEvent(
                type = "OrderAccepted",
                at = now,
                orderId = orderId,
                data = mapOf(
                    "symbol" to req.symbol,
                    "side" to req.side.name,
                    "type" to req.type.name,
                    "qty" to req.qty,
                    "price" to req.price,
                    "clientOrderId" to req.clientOrderId,
                    "idempotencyKey" to idempotencyKey
                )
            )
        )

        return AcceptOrderResult.Accepted(orderId)
    }

    fun getOrder(orderId: String): OrderSnapshot? = orderStore.findById(orderId)

    private fun validate(req: CreateOrderRequest): String? {
        if (req.symbol.isBlank()) return "INVALID_SYMBOL"
        if (req.qty <= 0) return "INVALID_QTY"
        return when (req.type) {
            OrderType.MARKET -> null
            OrderType.LIMIT -> {
                val p = req.price ?: return "MISSING_PRICE"
                if (p <= 0) "INVALID_PRICE" else null
            }
        }
    }
}
