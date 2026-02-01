package gateway.order

import gateway.audit.AuditEvent
import gateway.audit.AuditLog
import gateway.auth.Principal
import gateway.bus.NoopEventPublisher
import gateway.http.SseHub
import gateway.metrics.GatewayMetrics
import gateway.queue.BlockingFastPathQueue
import gateway.queue.NewOrderCommand
import gateway.risk.SimplePreTradeRisk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Instant

class OrderServiceBackpressureTest {
    @Test
    fun `queue full returns 429 for new orders`() {
        val queue = BlockingFastPathQueue(1)
        val sseHub = SseHub()
        val metrics = GatewayMetrics(queue = queue, sseHub = sseHub, kafka = null)
        val auditLog = object : AuditLog {
            override fun append(event: AuditEvent) {}
        }
        val service = OrderService(
            orderStore = InMemoryOrderStore(),
            auditLog = auditLog,
            eventPublisher = NoopEventPublisher,
            metrics = metrics,
            risk = SimplePreTradeRisk(),
            fastPathQueue = queue
        )

        val principal = Principal(accountId = "acct-1")
        val req = CreateOrderRequest(
            symbol = "BTC",
            side = OrderSide.BUY,
            type = OrderType.LIMIT,
            qty = 1L,
            price = 100L,
            timeInForce = TimeInForce.GTC,
            expireAt = null,
            clientOrderId = "c-1"
        )

        val first = service.acceptOrder(principal, req, idempotencyKey = "k-1")
        val second = service.acceptOrder(principal, req.copy(clientOrderId = "c-2"), idempotencyKey = "k-2")

        assertTrue(first is AcceptOrderResult.Accepted)
        assertTrue(second is AcceptOrderResult.Rejected)
        assertEquals(429, (second as AcceptOrderResult.Rejected).httpStatus)
        sseHub.close()
    }

    @Test
    fun `queue full returns 429 for cancels`() {
        val queue = BlockingFastPathQueue(1)
        queue.tryEnqueue(NewOrderCommand("ord-block"))
        val sseHub = SseHub()
        val metrics = GatewayMetrics(queue = queue, sseHub = sseHub, kafka = null)
        val auditLog = object : AuditLog {
            override fun append(event: AuditEvent) {}
        }
        val store = InMemoryOrderStore()
        val now = Instant.now()
        val order = OrderSnapshot(
            orderId = "ord-1",
            accountId = "acct-1",
            clientOrderId = "c-1",
            symbol = "BTC",
            side = OrderSide.BUY,
            type = OrderType.LIMIT,
            qty = 1L,
            price = 100L,
            timeInForce = TimeInForce.GTC,
            expireAt = null,
            status = OrderStatus.ACCEPTED,
            acceptedAt = now,
            lastUpdateAt = now,
            filledQty = 0
        )
        store.put(order, idempotencyKey = null)

        val service = OrderService(
            orderStore = store,
            auditLog = auditLog,
            eventPublisher = NoopEventPublisher,
            metrics = metrics,
            risk = SimplePreTradeRisk(),
            fastPathQueue = queue
        )

        val result = service.requestCancel(Principal(accountId = "acct-1"), order.orderId)
        assertTrue(result is CancelOrderResult.Rejected)
        assertEquals(429, (result as CancelOrderResult.Rejected).httpStatus)
        sseHub.close()
    }
}
