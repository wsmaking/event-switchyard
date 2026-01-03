package app.strategy

import app.events.MarketDataEvent
import app.fast.TradingFastPath
import app.fast.handlers.OrderSubmissionHandler
import app.http.MarketDataController
import app.order.OrderExecutionService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class StrategyAutoTrader(
    private val marketData: MarketDataController,
    private val executionService: OrderExecutionService,
    private val configService: StrategyConfigService,
    schedulerIntervalMs: Long = (System.getenv("STRATEGY_SCHEDULER_MS") ?: "200").toLong()
) : AutoCloseable {
    private val fastPath = TradingFastPath(OrderSubmissionHandler(executionService, configService))
    private val scheduler = Executors.newSingleThreadScheduledExecutor { r ->
        Thread(r, "strategy-auto-trader").apply { isDaemon = true }
    }
    private val schedulerIntervalMs = schedulerIntervalMs.coerceAtLeast(100)
    private var lastTickAtMs: Long = 0

    fun start() {
        scheduler.scheduleAtFixedRate(
            ::publishTick,
            schedulerIntervalMs,
            schedulerIntervalMs,
            TimeUnit.MILLISECONDS
        )
        val config = configService.current()
        println(
            "StrategyAutoTrader enabled (symbols=${config.symbols.joinToString(",")}, tickMs=${config.tickMs})"
        )
    }

    private fun publishTick() {
        val now = System.currentTimeMillis()
        val config = configService.current()
        if (!config.enabled) return
        if (now - lastTickAtMs < config.tickMs) return
        lastTickAtMs = now

        val symbols = if (config.symbols.isEmpty()) listOf("7203") else config.symbols
        for (symbol in symbols) {
            val price = marketData.getCurrentPrice(symbol)
            val spread = (price * 0.001).coerceAtLeast(0.1)
            val event =
                MarketDataEvent(
                    symbol = symbol,
                    timestamp = now,
                    price = price,
                    volume = 1_000L,
                    bid = price - spread,
                    ask = price + spread
                )
            fastPath.publishMarketData(event)
        }
    }

    override fun close() {
        scheduler.shutdown()
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow()
            }
        } catch (_: InterruptedException) {
            scheduler.shutdownNow()
            Thread.currentThread().interrupt()
        }
        fastPath.close()
    }
}
