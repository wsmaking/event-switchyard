package gateway.exchange

import gateway.order.OrderSnapshot
import gateway.order.OrderStatus
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class ExchangeSimulator(
    private val delayMs: Long = (System.getenv("EXCHANGE_SIM_DELAY_MS") ?: "50").toLong(),
    private val partialSteps: Int = (System.getenv("EXCHANGE_SIM_PARTIAL_STEPS") ?: "2").toInt(),
    private val rejectAll: Boolean = System.getenv("EXCHANGE_SIM_REJECT_ALL") == "1"
) : ExchangeClient {
    private val running = AtomicBoolean(true)
    private val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(1) { r ->
        Thread(r, "exchange-sim").apply { isDaemon = false }
    }
    private val states = ConcurrentHashMap<String, SimState>()

    override fun sendNewOrder(order: OrderSnapshot, onReport: (ExecutionReport) -> Unit) {
        if (!running.get()) return
        val state = SimState(qty = order.qty, price = order.price)
        states[order.orderId] = state

        if (rejectAll) {
            scheduler.schedule({
                if (!running.get()) return@schedule
                if (state.canceled) return@schedule
                state.done = true
                val now = Instant.now()
                onReport(
                    ExecutionReport(
                        orderId = order.orderId,
                        status = OrderStatus.REJECTED,
                        filledQtyDelta = 0,
                        filledQtyTotal = 0,
                        price = order.price,
                        at = now
                    )
                )
            }, delayMs, TimeUnit.MILLISECONDS)
            return
        }

        val steps = if (partialSteps <= 1 || order.qty <= 1) 1 else partialSteps
        val perStep = maxOf(1, order.qty / steps)

        for (i in 1..steps) {
            scheduler.schedule({
                if (!running.get()) return@schedule
                if (state.canceled || state.done) return@schedule

                val now = Instant.now()
                val remaining = state.qty - state.filled
                val delta = if (i == steps) remaining else minOf(perStep, remaining)
                state.filled += delta
                val status =
                    if (state.filled >= state.qty) OrderStatus.FILLED else OrderStatus.PARTIALLY_FILLED
                if (status == OrderStatus.FILLED) state.done = true
                onReport(
                    ExecutionReport(
                        orderId = order.orderId,
                        status = status,
                        filledQtyDelta = delta,
                        filledQtyTotal = state.filled,
                        price = state.price,
                        at = now
                    )
                )
            }, delayMs * i, TimeUnit.MILLISECONDS)
        }
    }

    override fun sendCancel(orderId: String, onReport: (ExecutionReport) -> Unit) {
        if (!running.get()) return
        val state = states[orderId]
        scheduler.schedule({
            if (!running.get()) return@schedule
            val now = Instant.now()

            if (state == null) {
                onReport(
                    ExecutionReport(
                        orderId = orderId,
                        status = OrderStatus.REJECTED,
                        filledQtyDelta = 0,
                        filledQtyTotal = 0,
                        price = null,
                        at = now
                    )
                )
                return@schedule
            }

            if (state.done) {
                onReport(
                    ExecutionReport(
                        orderId = orderId,
                        status = OrderStatus.REJECTED,
                        filledQtyDelta = 0,
                        filledQtyTotal = state.filled,
                        price = state.price,
                        at = now
                    )
                )
                return@schedule
            }

            state.canceled = true
            state.done = true
            onReport(
                ExecutionReport(
                    orderId = orderId,
                    status = OrderStatus.CANCELED,
                    filledQtyDelta = 0,
                    filledQtyTotal = state.filled,
                    price = state.price,
                    at = now
                )
            )
        }, delayMs, TimeUnit.MILLISECONDS)
    }

    override fun close() {
        running.set(false)
        scheduler.shutdownNow()
    }

    private data class SimState(
        val qty: Long,
        val price: Long?
    ) {
        @Volatile var filled: Long = 0
        @Volatile var canceled: Boolean = false
        @Volatile var done: Boolean = false
    }
}
