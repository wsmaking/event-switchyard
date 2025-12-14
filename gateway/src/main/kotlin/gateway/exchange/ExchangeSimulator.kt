package gateway.exchange

import gateway.order.OrderSnapshot
import gateway.order.OrderStatus
import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class ExchangeSimulator(
    private val delayMs: Long = (System.getenv("EXCHANGE_SIM_DELAY_MS") ?: "50").toLong()
) : ExchangeClient {
    private val running = AtomicBoolean(true)
    private val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(1) { r ->
        Thread(r, "exchange-sim").apply { isDaemon = false }
    }

    override fun sendNewOrder(order: OrderSnapshot, onReport: (ExecutionReport) -> Unit) {
        if (!running.get()) return
        val qty = order.qty
        val price = order.price

        scheduler.schedule({
            if (!running.get()) return@schedule
            val now = Instant.now()
            onReport(
                ExecutionReport(
                    orderId = order.orderId,
                    status = OrderStatus.FILLED,
                    filledQtyDelta = qty,
                    filledQtyTotal = qty,
                    price = price,
                    at = now
                )
            )
        }, delayMs, TimeUnit.MILLISECONDS)
    }

    override fun close() {
        running.set(false)
        scheduler.shutdownNow()
    }
}

