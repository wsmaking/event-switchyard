package app.fast.handlers

import app.events.MarketDataEvent
import app.events.IndicatorEvent
import app.indicators.TechnicalIndicators
import com.lmax.disruptor.EventHandler
import java.util.concurrent.ConcurrentHashMap

data class RingBufferEvent(
    var eventType: String = "",
    var payload: Any? = null,
    var timestamp: Long = 0L
)

class TechnicalIndicatorHandler : EventHandler<RingBufferEvent> {
    private val priceHistory = ConcurrentHashMap<String, MutableList<Double>>()
    private val maxHistorySize = 200

    override fun onEvent(event: RingBufferEvent, sequence: Long, endOfBatch: Boolean) {
        if (event.eventType != "MarketData") return

        val marketData = event.payload as? MarketDataEvent ?: return
        val symbol = marketData.symbol

        val history = priceHistory.computeIfAbsent(symbol) { mutableListOf() }
        history.add(marketData.price)

        if (history.size > maxHistorySize) {
            history.removeAt(0)
        }

        if (history.size < 50) return

        val sma20 = if (history.size >= 20) {
            TechnicalIndicators.sma(history, 20).lastOrNull()?.value ?: 0.0
        } else 0.0

        val sma50 = if (history.size >= 50) {
            TechnicalIndicators.sma(history, 50).lastOrNull()?.value ?: 0.0
        } else 0.0

        val rsi = if (history.size >= 15) {
            TechnicalIndicators.rsi(history, 14).lastOrNull()?.value ?: 50.0
        } else 50.0

        val (macdLine, signalLine, _) = TechnicalIndicators.macd(history, 12, 26, 9)
        val macd = macdLine.lastOrNull()?.value ?: 0.0
        val macdSignal = signalLine.lastOrNull()?.value ?: 0.0

        val (_, upper, lower) = TechnicalIndicators.bollingerBands(history, 20, 2.0)
        val bbUpper = upper.lastOrNull()?.value ?: 0.0
        val bbLower = lower.lastOrNull()?.value ?: 0.0

        val indicatorEvent = IndicatorEvent(
            symbol = symbol,
            timestamp = marketData.timestamp,
            sma20 = sma20,
            sma50 = sma50,
            rsi = rsi,
            macd = macd,
            macdSignal = macdSignal,
            bbUpper = bbUpper,
            bbLower = bbLower
        )

        event.eventType = "Indicator"
        event.payload = indicatorEvent
        event.timestamp = System.nanoTime()
    }
}
