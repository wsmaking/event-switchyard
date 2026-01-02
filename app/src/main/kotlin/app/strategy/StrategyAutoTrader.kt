package app.strategy

import app.events.MarketDataEvent
import app.fast.TradingFastPath
import app.fast.handlers.OrderSubmissionHandler
import app.http.MarketDataController
import app.clients.gateway.GatewayClient
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class StrategyAutoTrader(
    private val marketData: MarketDataController,
    private val gatewayClient: GatewayClient,
    symbols: List<String> =
        (System.getenv("STRATEGY_SYMBOLS") ?: "7203,6758,9984")
            .split(',')
            .map { it.trim() }
            .filter { it.isNotEmpty() },
    intervalMs: Long = (System.getenv("STRATEGY_TICK_MS") ?: "1000").toLong()
) : AutoCloseable {
    private val fastPath = TradingFastPath(OrderSubmissionHandler(gatewayClient))
    private val symbolList = if (symbols.isEmpty()) listOf("7203") else symbols
    private val scheduler = Executors.newSingleThreadScheduledExecutor { r ->
        Thread(r, "strategy-auto-trader").apply { isDaemon = true }
    }
    private val intervalMs = intervalMs.coerceAtLeast(100)

    fun start() {
        scheduler.scheduleAtFixedRate(
            ::publishTick,
            intervalMs,
            intervalMs,
            TimeUnit.MILLISECONDS
        )
        println("StrategyAutoTrader enabled (symbols=${symbolList.joinToString(",")}, tickMs=$intervalMs)")
    }

    private fun publishTick() {
        val now = System.currentTimeMillis()
        for (symbol in symbolList) {
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
