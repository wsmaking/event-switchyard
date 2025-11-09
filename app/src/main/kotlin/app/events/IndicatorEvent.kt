package app.events

data class IndicatorEvent(
    val symbol: String,
    val timestamp: Long,
    val sma20: Double,
    val sma50: Double,
    val rsi: Double,
    val macd: Double,
    val macdSignal: Double,
    val bbUpper: Double,
    val bbLower: Double
)
