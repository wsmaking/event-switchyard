package gateway.engine

import gateway.audit.AuditEvent
import gateway.audit.AuditLog
import gateway.bus.BusEvent
import gateway.bus.EventPublisher
import gateway.exchange.ExchangeClient
import gateway.exchange.ExecutionReport
import gateway.http.SseHub
import gateway.metrics.GatewayMetrics
import gateway.order.InMemoryOrderStore
import gateway.order.OrderSnapshot
import gateway.order.OrderStatus
import gateway.queue.FastPathQueue
import gateway.queue.NewOrderCommand
import gateway.queue.CancelOrderCommand
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

class FastPathEngine(
    private val queue: FastPathQueue,
    private val orderStore: InMemoryOrderStore,
    private val auditLog: AuditLog,
    private val eventPublisher: EventPublisher,
    private val metrics: GatewayMetrics,
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
                    when (cmd) {
                        is NewOrderCommand -> handleNewOrder(cmd.orderId)
                        is CancelOrderCommand -> handleCancel(cmd.orderId)
                    }
                } catch (_: InterruptedException) {
                    break
                } catch (_: Throwable) {
                }
            }
        }
    }

    private fun handleNewOrder(orderId: String) {
        val order = orderStore.findById(orderId) ?: return
        val now = Instant.now()
        val updated = orderStore.update(order.orderId) { it.copy(status = OrderStatus.SENT, lastUpdateAt = now) }
        if (updated != null) {
            auditLog.append(AuditEvent(type = "OrderSent", at = now, accountId = updated.accountId, orderId = updated.orderId))
            eventPublisher.publish(
                BusEvent(
                    type = "OrderSent",
                    at = now,
                    accountId = updated.accountId,
                    orderId = updated.orderId
                )
            )
            metrics.onOrderSent()
            sseHub.publish(updated.orderId, event = "order_update", data = updated)
            sseHub.publishAccount(updated.accountId, event = "order_update", data = updated)
        }

        exchange.sendNewOrder(order) { report ->
            onExecutionReport(report)
        }
    }

    private fun handleCancel(orderId: String) {
        val now = Instant.now()
        val order = orderStore.findById(orderId)
        auditLog.append(AuditEvent(type = "CancelSent", at = now, accountId = order?.accountId, orderId = orderId))
        if (order?.accountId != null) {
            eventPublisher.publish(
                BusEvent(
                    type = "CancelSent",
                    at = now,
                    accountId = order.accountId,
                    orderId = orderId
                )
            )
        }
        metrics.onCancelSent()
        exchange.sendCancel(orderId) { report ->
            onExecutionReport(report)
        }
    }

    private fun onExecutionReport(report: ExecutionReport) {
        val now = report.at
        val accountId = orderStore.findById(report.orderId)?.accountId ?: return
        metrics.onExecutionReport()
        auditLog.append(
            AuditEvent(
                type = "ExecutionReport",
                at = now,
                accountId = accountId,
                orderId = report.orderId,
                data = mapOf(
                    "status" to report.status.name,
                    "filledQtyDelta" to report.filledQtyDelta,
                    "filledQtyTotal" to report.filledQtyTotal,
                    "price" to report.price
                )
            )
        )
        eventPublisher.publish(
            BusEvent(
                type = "ExecutionReport",
                at = now,
                accountId = accountId,
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
            metrics.onOrderUpdated()
            auditLog.append(
                AuditEvent(
                    type = "OrderUpdated",
                    at = now,
                    accountId = updated.accountId,
                    orderId = updated.orderId,
                    data = mapOf(
                        "status" to updated.status.name,
                        "filledQty" to updated.filledQty
                    )
                )
            )
            eventPublisher.publish(
                BusEvent(
                    type = "OrderUpdated",
                    at = now,
                    accountId = updated.accountId,
                    orderId = updated.orderId,
                    data = mapOf(
                        "status" to updated.status.name,
                        "filledQty" to updated.filledQty
                    )
                )
            )
            sseHub.publish(updated.orderId, event = "order_update", data = updated)
            sseHub.publishAccount(updated.accountId, event = "order_update", data = updated)
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
