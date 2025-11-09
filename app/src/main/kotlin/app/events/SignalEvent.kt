package app.events

data class SignalEvent(
    val symbol: String,
    val timestamp: Long,
    val signal: Signal,
    val confidence: Double,
    val strategyName: String
) {
    enum class Signal {
        BUY, SELL, HOLD
    }
}
