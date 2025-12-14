package gateway.engine

import gateway.audit.AuditEvent
import gateway.audit.AuditLog
import gateway.exchange.ExchangeClient
import gateway.exchange.ExecutionReport
import gateway.http.SseHub
import gateway.order.InMemoryOrderStore
import gateway.order.OrderSnapshot
import gateway.order.OrderStatus
import gateway.queue.FastPathQueue
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

class FastPathEngine(
    private val queue: FastPathQueue,
    private val orderStore: InMemoryOrderStore,
    private val auditLog: AuditLog,
    private val exchange: ExchangeClient,
    private val sseHub: SseHub
) : AutoCloseable {
    private val running = AtomicBoolean(false)
    private var worker: Thread? = null

    fun start() {
        if (!running.compareAndSet(false, true)) return
        worker = thread(name = "gateway-fastpath", isDaemon = false, start = true) {
            while (running.get()) {
                try {
                    val cmd = queue.poll()
                    if (cmd == null) continue
                    handle(cmd.order)
                } catch (_: InterruptedException) {
                    break
                } catch (_: Throwable) {
                }
            }
        }
    }

    private fun handle(order: OrderSnapshot) {
        val now = Instant.now()
        val updated = orderStore.update(order.orderId) { it.copy(status = OrderStatus.SENT, lastUpdateAt = now) }
        if (updated != null) {
            auditLog.append(AuditEvent(type = "OrderSent", at = now, orderId = updated.orderId))
            sseHub.publish(updated.orderId, event = "order_update", data = updated)
        }

        exchange.sendNewOrder(order) { report ->
            onExecutionReport(report)
        }
    }

    private fun onExecutionReport(report: ExecutionReport) {
        val now = report.at
        auditLog.append(
            AuditEvent(
                type = "ExecutionReport",
                at = now,
                orderId = report.orderId,
                data = mapOf(
                    "status" to report.status.name,
                    "filledQtyDelta" to report.filledQtyDelta,
                    "filledQtyTotal" to report.filledQtyTotal,
                    "price" to report.price
                )
            )
        )

        val updated = orderStore.applyExecutionReport(report)
        if (updated != null) {
            auditLog.append(
                AuditEvent(
                    type = "OrderUpdated",
                    at = now,
                    orderId = updated.orderId,
                    data = mapOf(
                        "status" to updated.status.name,
                        "filledQty" to updated.filledQty
                    )
                )
            )
            sseHub.publish(updated.orderId, event = "order_update", data = updated)
        }
    }

    override fun close() {
        running.set(false)
        worker?.interrupt()
        try {
            worker?.join(1000)
        } catch (_: InterruptedException) {
        }
    }
}

