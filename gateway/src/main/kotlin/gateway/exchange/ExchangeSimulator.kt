package gateway.exchange

import gateway.order.OrderSnapshot
import gateway.order.OrderStatus
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

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
    private val lastActivityAt = AtomicLong(System.currentTimeMillis())
    private val submittedOrders = AtomicLong(0)
    private val rejectedOrders = AtomicLong(0)
    private val canceledOrders = AtomicLong(0)
    private val partialReports = AtomicLong(0)
    private val fillReports = AtomicLong(0)
    private val activeIncidents = CopyOnWriteArrayList<String>()

    @Volatile
    private var mode: ExchangeSimulatorMode = if (rejectAll) ExchangeSimulatorMode.REJECT_ALL else ExchangeSimulatorMode.NORMAL

    @Volatile
    private var dynamicDelayMs: Long = delayMs

    @Volatile
    private var dynamicPartialSteps: Int = partialSteps

    @Volatile
    private var sessionState: String = "RUNNING"

    @Volatile
    private var auctionState: String = "CONTINUOUS"

    @Volatile
    private var entitlementState: String = "FULL_ACCESS"

    @Volatile
    private var dropCopyDivergence: Boolean = false

    @Volatile
    private var throttleState: String = "OPEN"

    override fun sendNewOrder(order: OrderSnapshot, onReport: (ExecutionReport) -> Unit) {
        if (!running.get()) return
        submittedOrders.incrementAndGet()
        touch()
        val state = SimState(qty = order.qty, price = order.price)
        states[order.orderId] = state

        if (shouldRejectNewOrder()) {
            scheduler.schedule({
                if (!running.get()) return@schedule
                if (state.canceled) return@schedule
                state.done = true
                rejectedOrders.incrementAndGet()
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
            }, effectiveDelayMs(), TimeUnit.MILLISECONDS)
            return
        }

        val configuredSteps = if (mode == ExchangeSimulatorMode.PARTIAL_FILL_BURST) maxOf(dynamicPartialSteps, 5) else dynamicPartialSteps
        val steps = if (configuredSteps <= 1 || order.qty <= 1) 1 else configuredSteps
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
                if (status == OrderStatus.FILLED) {
                    fillReports.incrementAndGet()
                } else {
                    partialReports.incrementAndGet()
                }
                touch()
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
            }, effectiveDelayMs() * i, TimeUnit.MILLISECONDS)
        }
    }

    override fun sendCancel(orderId: String, onReport: (ExecutionReport) -> Unit) {
        if (!running.get()) return
        val state = states[orderId]
        scheduler.schedule({
            if (!running.get()) return@schedule
            val now = Instant.now()
            touch()

            if (state == null) {
                rejectedOrders.incrementAndGet()
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
                rejectedOrders.incrementAndGet()
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
            canceledOrders.incrementAndGet()
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
        }, effectiveDelayMs(), TimeUnit.MILLISECONDS)
    }

    override fun close() {
        running.set(false)
        scheduler.shutdownNow()
    }

    fun snapshot(): ExchangeSimulatorStatus {
        val workingOrders = states.values.count { !it.done }
        val incidents = mutableListOf<String>()
        if (mode != ExchangeSimulatorMode.NORMAL) {
            incidents.add("mode=${mode.name}")
        }
        if (sessionState != "RUNNING") {
            incidents.add("session=$sessionState")
        }
        if (auctionState != "CONTINUOUS") {
            incidents.add("auction=$auctionState")
        }
        if (entitlementState != "FULL_ACCESS") {
            incidents.add("entitlement=$entitlementState")
        }
        if (dropCopyDivergence) {
            incidents.add("drop-copy-divergence")
        }
        incidents.addAll(activeIncidents)
        val overallState = when {
            sessionState != "RUNNING" || auctionState == "HALTED" -> "DEGRADED"
            dropCopyDivergence || mode != ExchangeSimulatorMode.NORMAL || throttleState != "OPEN" -> "WATCH"
            else -> "RUNNING"
        }
        return ExchangeSimulatorStatus(
            state = overallState,
            mode = mode.name,
            sessionState = sessionState,
            auctionState = auctionState,
            throttleState = throttleState,
            entitlementState = entitlementState,
            delayMs = dynamicDelayMs,
            partialSteps = dynamicPartialSteps,
            workingOrders = workingOrders,
            submittedOrders = submittedOrders.get(),
            rejectedOrders = rejectedOrders.get(),
            canceledOrders = canceledOrders.get(),
            partialReports = partialReports.get(),
            fillReports = fillReports.get(),
            dropCopyDivergence = dropCopyDivergence,
            activeIncidents = incidents.distinct(),
            lastActivityAgeMs = maxOf(0L, System.currentTimeMillis() - lastActivityAt.get())
        )
    }

    fun applyControl(request: ExchangeSimulatorControlRequest): ExchangeSimulatorStatus {
        request.mode?.trim()?.takeIf { it.isNotBlank() }?.let {
            mode = runCatching { ExchangeSimulatorMode.valueOf(it.uppercase()) }.getOrElse { mode }
        }
        request.delayMs?.let { dynamicDelayMs = maxOf(1L, it) }
        request.partialSteps?.let { dynamicPartialSteps = maxOf(1, it) }
        request.sessionState?.trim()?.takeIf { it.isNotBlank() }?.let { sessionState = it.uppercase() }
        request.auctionState?.trim()?.takeIf { it.isNotBlank() }?.let { auctionState = it.uppercase() }
        request.entitlementState?.trim()?.takeIf { it.isNotBlank() }?.let { entitlementState = it.uppercase() }
        request.dropCopyDivergence?.let { dropCopyDivergence = it }
        request.activeIncidents?.let {
            activeIncidents.clear()
            activeIncidents.addAll(it)
        }
        request.resetCounters.takeIf { it }?.let {
            submittedOrders.set(0)
            rejectedOrders.set(0)
            canceledOrders.set(0)
            partialReports.set(0)
            fillReports.set(0)
        }
        throttleState = when (mode) {
            ExchangeSimulatorMode.THROTTLED -> "SOFT_LIMIT"
            ExchangeSimulatorMode.SESSION_FLAP -> "WATCH"
            else -> "OPEN"
        }
        touch()
        return snapshot()
    }

    fun resetControl(): ExchangeSimulatorStatus {
        mode = if (rejectAll) ExchangeSimulatorMode.REJECT_ALL else ExchangeSimulatorMode.NORMAL
        dynamicDelayMs = delayMs
        dynamicPartialSteps = partialSteps
        sessionState = "RUNNING"
        auctionState = "CONTINUOUS"
        entitlementState = "FULL_ACCESS"
        dropCopyDivergence = false
        throttleState = "OPEN"
        activeIncidents.clear()
        submittedOrders.set(0)
        rejectedOrders.set(0)
        canceledOrders.set(0)
        partialReports.set(0)
        fillReports.set(0)
        touch()
        return snapshot()
    }

    private fun shouldRejectNewOrder(): Boolean {
        if (mode == ExchangeSimulatorMode.REJECT_ALL) return true
        if (sessionState != "RUNNING") return true
        if (auctionState == "HALTED") return true
        if (entitlementState != "FULL_ACCESS") return true
        if (mode == ExchangeSimulatorMode.SESSION_FLAP) {
            return submittedOrders.get() % 2L == 0L
        }
        return false
    }

    private fun effectiveDelayMs(): Long {
        return when (mode) {
            ExchangeSimulatorMode.THROTTLED -> dynamicDelayMs * 4L
            ExchangeSimulatorMode.AUCTION_ONLY -> dynamicDelayMs * 2L
            else -> dynamicDelayMs
        }
    }

    private fun touch() {
        lastActivityAt.set(System.currentTimeMillis())
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

enum class ExchangeSimulatorMode {
    NORMAL,
    REJECT_ALL,
    SESSION_FLAP,
    AUCTION_ONLY,
    PARTIAL_FILL_BURST,
    THROTTLED
}

data class ExchangeSimulatorControlRequest(
    val mode: String? = null,
    val delayMs: Long? = null,
    val partialSteps: Int? = null,
    val sessionState: String? = null,
    val auctionState: String? = null,
    val entitlementState: String? = null,
    val dropCopyDivergence: Boolean? = null,
    val activeIncidents: List<String>? = null,
    val resetCounters: Boolean = false
)

data class ExchangeSimulatorStatus(
    val state: String,
    val mode: String,
    val sessionState: String,
    val auctionState: String,
    val throttleState: String,
    val entitlementState: String,
    val delayMs: Long,
    val partialSteps: Int,
    val workingOrders: Int,
    val submittedOrders: Long,
    val rejectedOrders: Long,
    val canceledOrders: Long,
    val partialReports: Long,
    val fillReports: Long,
    val dropCopyDivergence: Boolean,
    val activeIncidents: List<String>,
    val lastActivityAgeMs: Long
)
