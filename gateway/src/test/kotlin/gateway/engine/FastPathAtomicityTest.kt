package gateway.engine

import gateway.audit.AuditEvent
import gateway.audit.AuditLog
import gateway.auth.Principal
import gateway.bus.NoopEventPublisher
import gateway.exchange.ExchangeClient
import gateway.exchange.ExecutionReport
import gateway.http.SseHub
import gateway.metrics.GatewayMetrics
import gateway.order.CreateOrderRequest
import gateway.order.InMemoryOrderStore
import gateway.order.OrderService
import gateway.order.OrderSide
import gateway.order.OrderSnapshot
import gateway.order.OrderType
import gateway.order.TimeInForce
import gateway.queue.BlockingFastPathQueue
import gateway.risk.SimplePreTradeRisk
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

class FastPathAtomicityTest {
    @Test
    fun `fast path waits for accepted audit before send`() {
        val auditGate = CountDownLatch(1)
        val auditEvents = CopyOnWriteArrayList<AuditEvent>()
        val auditLog = object : AuditLog {
            override fun append(event: AuditEvent) {
                if (event.type == "OrderAccepted") {
                    auditGate.await()
                }
                auditEvents.add(event)
            }
        }

        val queue = BlockingFastPathQueue(8)
        val sseHub = SseHub()
        val metrics = GatewayMetrics(queue = queue, sseHub = sseHub, kafka = null)
        val store = InMemoryOrderStore()

        val sentBeforeAudit = AtomicBoolean(false)
        val sentLatch = CountDownLatch(1)
        val exchange = object : ExchangeClient {
            override fun sendNewOrder(order: OrderSnapshot, onReport: (ExecutionReport) -> Unit) {
                val hasAudit = auditEvents.any { it.type == "OrderAccepted" && it.orderId == order.orderId }
                sentBeforeAudit.set(!hasAudit)
                sentLatch.countDown()
            }

            override fun sendCancel(orderId: String, onReport: (ExecutionReport) -> Unit) {}
            override fun close() {}
        }

        val engine = FastPathEngine(queue, store, auditLog, NoopEventPublisher, metrics, exchange, sseHub)

        val service = OrderService(
            orderStore = store,
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

        val acceptThread = thread(start = true) {
            service.acceptOrder(principal, req, idempotencyKey = "k-1")
        }

        val enqueueSeen = waitForQueueDepth(queue, 1)
        assertTrue(enqueueSeen)
        engine.start()
        assertFalse(sentLatch.await(100, TimeUnit.MILLISECONDS))

        auditGate.countDown()

        assertTrue(sentLatch.await(1, TimeUnit.SECONDS))
        assertFalse(sentBeforeAudit.get())

        acceptThread.join(1000)
        engine.close()
        sseHub.close()
    }

    private fun waitForQueueDepth(queue: BlockingFastPathQueue, depth: Int): Boolean {
        val timeoutMs = 200L
        val start = System.currentTimeMillis()
        while (System.currentTimeMillis() - start < timeoutMs) {
            if (queue.depth() >= depth) return true
            Thread.sleep(5)
        }
        return false
    }
}
