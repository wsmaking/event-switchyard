package gateway.order

import gateway.audit.FileAuditLog
import gateway.bus.NoopEventPublisher
import gateway.metrics.GatewayMetrics
import gateway.queue.BlockingFastPathQueue
import gateway.risk.SimplePreTradeRisk
import gateway.http.SseHub
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.file.Files

class OrderServiceCanaryTest {
    @Test
    fun `accepts a small canary batch`() {
        val auditPath = Files.createTempFile("audit", ".log")
        val auditLog = FileAuditLog(auditPath)
        val queue = BlockingFastPathQueue(1024)
        val sseHub = SseHub()
        val metrics = GatewayMetrics(queue = queue, sseHub = sseHub, kafka = null)
        val service = OrderService(
            orderStore = InMemoryOrderStore(),
            auditLog = auditLog,
            eventPublisher = NoopEventPublisher,
            metrics = metrics,
            risk = SimplePreTradeRisk(),
            fastPathQueue = queue
        )

        val principal = gateway.auth.Principal(accountId = "acct-1")
        val accepted = (1..500).count { i ->
            val req = CreateOrderRequest(
                symbol = "BTC",
                side = OrderSide.BUY,
                type = OrderType.LIMIT,
                qty = 1,
                price = 100 + i,
                timeInForce = TimeInForce.GTC,
                expireAt = null,
                clientOrderId = "c-$i"
            )
            service.acceptOrder(principal, req, idempotencyKey = "k-$i") is AcceptOrderResult.Accepted
        }

        assertEquals(500, accepted)
        assertTrue(queue.depth() >= 500)
        sseHub.close()
        auditLog.close()
    }
}
