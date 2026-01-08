package gateway.order

import gateway.audit.AuditEvent
import gateway.audit.AuditLog
import gateway.auth.Principal
import gateway.bus.BusEvent
import gateway.bus.EventPublisher
import gateway.metrics.GatewayMetrics
import gateway.risk.PreTradeRisk
import gateway.queue.FastPathQueue
import gateway.queue.NewOrderCommand
import gateway.queue.CancelOrderCommand
import gateway.rate.RateLimiter
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
    private val eventPublisher: EventPublisher,
    private val metrics: GatewayMetrics,
    private val risk: PreTradeRisk,
    private val fastPathQueue: FastPathQueue,
    private val rateLimiter: RateLimiter? = null
) {
    fun acceptOrder(principal: Principal, req: CreateOrderRequest, idempotencyKey: String?): AcceptOrderResult {
        if (idempotencyKey != null) {
            val existing = orderStore.findByIdempotencyKey(principal.accountId, idempotencyKey)
            if (existing != null) return AcceptOrderResult.Accepted(existing.orderId)
        }

        if (rateLimiter != null && !rateLimiter.allow()) {
            metrics.onOrderRejected()
            return AcceptOrderResult.Rejected("RATE_LIMIT", 429)
        }

        val validationError = validate(req)
        if (validationError != null) return AcceptOrderResult.Rejected(validationError, 422)

        val riskResult = risk.validate(principal.accountId, req)
        if (!riskResult.ok) {
            metrics.onOrderRejected()
            return AcceptOrderResult.Rejected(riskResult.reason ?: "RISK_REJECT", 422)
        }

        val now = Instant.now()
        val orderId = "ord_${UUID.randomUUID()}"
        val snapshot = OrderSnapshot(
            orderId = orderId,
            accountId = principal.accountId,
            clientOrderId = req.clientOrderId,
            symbol = req.symbol,
            side = req.side,
            type = req.type,
            qty = req.qty,
            price = req.price,
            timeInForce = req.timeInForce,
            expireAt = req.expireAt,
            status = OrderStatus.ACCEPTED,
            acceptedAt = now,
            lastUpdateAt = now,
            filledQty = 0
        )
        orderStore.put(snapshot, idempotencyKey)

        val enqueue = fastPathQueue.tryEnqueue(NewOrderCommand(orderId))
        metrics.onFastPathEnqueue(enqueue.ok)
        if (!enqueue.ok) {
            orderStore.remove(orderId, idempotencyKey)
            metrics.onOrderRejected()
            return AcceptOrderResult.Rejected(enqueue.reason ?: "QUEUE_REJECT", 503)
        }

        auditLog.append(
            AuditEvent(
                type = "OrderAccepted",
                at = now,
                accountId = principal.accountId,
                orderId = orderId,
                data = mapOf(
                    "accountId" to principal.accountId,
                    "symbol" to req.symbol,
                    "side" to req.side.name,
                    "type" to req.type.name,
                    "qty" to req.qty,
                    "price" to req.price,
                    "timeInForce" to req.timeInForce.name,
                    "expireAt" to req.expireAt,
                    "clientOrderId" to req.clientOrderId,
                    "idempotencyKey" to idempotencyKey
                )
            )
        )
        eventPublisher.publish(
            BusEvent(
                type = "OrderAccepted",
                at = now,
                accountId = principal.accountId,
                orderId = orderId,
                data = mapOf(
                    "symbol" to req.symbol,
                    "side" to req.side.name,
                    "type" to req.type.name,
                    "qty" to req.qty,
                    "price" to req.price,
                    "timeInForce" to req.timeInForce.name,
                    "expireAt" to req.expireAt,
                    "clientOrderId" to req.clientOrderId
                )
            )
        )

        metrics.onOrderAccepted()
        return AcceptOrderResult.Accepted(orderId)
    }

    fun getOrder(orderId: String): OrderSnapshot? = orderStore.findById(orderId)

    fun requestCancel(principal: Principal, orderId: String): CancelOrderResult {
        val now = Instant.now()
        val order = orderStore.findById(orderId) ?: return CancelOrderResult.Rejected("NOT_FOUND", 404)
        if (order.accountId != principal.accountId) return CancelOrderResult.Rejected("NOT_FOUND", 404)

        if (order.status == OrderStatus.CANCELED || order.status == OrderStatus.FILLED || order.status == OrderStatus.REJECTED) {
            return CancelOrderResult.Rejected("ORDER_FINAL", 409)
        }

        if (order.status == OrderStatus.CANCEL_REQUESTED) {
            return CancelOrderResult.Accepted(orderId)
        }

        orderStore.update(orderId) { it.copy(status = OrderStatus.CANCEL_REQUESTED, lastUpdateAt = now) }

        val enqueue = fastPathQueue.tryEnqueue(CancelOrderCommand(orderId))
        metrics.onFastPathEnqueue(enqueue.ok)
        if (!enqueue.ok) {
            orderStore.update(orderId) { it.copy(status = order.status, lastUpdateAt = now) }
            return CancelOrderResult.Rejected(enqueue.reason ?: "QUEUE_REJECT", 503)
        }

        auditLog.append(AuditEvent(type = "CancelRequested", at = now, accountId = principal.accountId, orderId = orderId))
        eventPublisher.publish(
            BusEvent(
                type = "CancelRequested",
                at = now,
                accountId = principal.accountId,
                orderId = orderId
            )
        )
        metrics.onCancelRequested()
        return CancelOrderResult.Accepted(orderId)
    }

    private fun validate(req: CreateOrderRequest): String? {
        if (req.symbol.isBlank()) return "INVALID_SYMBOL"
        if (req.qty <= 0) return "INVALID_QTY"
        if (req.timeInForce == TimeInForce.GTD) {
            val expireAt = req.expireAt ?: return "MISSING_EXPIRE_AT"
            if (expireAt <= Instant.now().toEpochMilli()) return "EXPIRE_AT_IN_PAST"
        }
        return when (req.type) {
            OrderType.MARKET -> null
            OrderType.LIMIT -> {
                val p = req.price ?: return "MISSING_PRICE"
                if (p <= 0) "INVALID_PRICE" else null
            }
        }
    }
}

sealed interface CancelOrderResult {
    val httpStatus: Int

    data class Accepted(val orderId: String) : CancelOrderResult {
        override val httpStatus: Int = 202
    }

    data class Rejected(val reason: String, override val httpStatus: Int) : CancelOrderResult
}
