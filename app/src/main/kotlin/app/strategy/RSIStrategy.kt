package app.strategy

class RSIStrategy(
    private val period: Int = 14,
    private val oversoldLevel: Double = 30.0,
    private val overboughtLevel: Double = 70.0
) : TradingStrategy {

    override fun getName() = "RSI Strategy (period=$period)"

    override fun analyze(symbol: String, prices: List<Double>): Signal {
        if (prices.size < period + 1) {
            return Signal(symbol, Signal.Action.HOLD, 0.0, System.currentTimeMillis())
        }

        val rsi = calculateRSI(prices)

        return when {
            rsi < oversoldLevel -> {
                val strength = (oversoldLevel - rsi) / oversoldLevel * 100
                Signal(symbol, Signal.Action.BUY, strength, System.currentTimeMillis())
            }
            rsi > overboughtLevel -> {
                val strength = (rsi - overboughtLevel) / (100 - overboughtLevel) * 100
                Signal(symbol, Signal.Action.SELL, strength, System.currentTimeMillis())
            }
            else -> {
                Signal(symbol, Signal.Action.HOLD, 0.0, System.currentTimeMillis())
            }
        }
    }

    private fun calculateRSI(prices: List<Double>): Double {
        val changes = prices.zipWithNext { a, b -> b - a }
        val recentChanges = changes.takeLast(period)

        val gains = recentChanges.filter { it > 0 }.average().takeIf { !it.isNaN() } ?: 0.0
        val losses = recentChanges.filter { it < 0 }.map { -it }.average().takeIf { !it.isNaN() } ?: 0.0

        if (losses == 0.0) return 100.0

        val rs = gains / losses
        return 100.0 - (100.0 / (1.0 + rs))
    }
}
