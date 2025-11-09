package app.strategy

data class Signal(
    val symbol: String,
    val action: Action,
    val strength: Double,
    val timestamp: Long
) {
    enum class Action {
        BUY, SELL, HOLD
    }
}

interface TradingStrategy {
    fun analyze(symbol: String, prices: List<Double>): Signal
    fun getName(): String
}
