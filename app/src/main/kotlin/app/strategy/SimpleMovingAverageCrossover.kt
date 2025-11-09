package app.strategy

class SimpleMovingAverageCrossover(
    private val fastPeriod: Int = 10,
    private val slowPeriod: Int = 30
) : TradingStrategy {

    override fun getName() = "SMA Crossover ($fastPeriod/$slowPeriod)"

    override fun analyze(symbol: String, prices: List<Double>): Signal {
        if (prices.size < slowPeriod) {
            return Signal(symbol, Signal.Action.HOLD, 0.0, System.currentTimeMillis())
        }

        val fastSMA = prices.takeLast(fastPeriod).average()
        val slowSMA = prices.takeLast(slowPeriod).average()

        val prevFastSMA = prices.dropLast(1).takeLast(fastPeriod).average()
        val prevSlowSMA = prices.dropLast(1).takeLast(slowPeriod).average()

        return when {
            fastSMA > slowSMA && prevFastSMA <= prevSlowSMA -> {
                val strength = ((fastSMA - slowSMA) / slowSMA) * 100
                Signal(symbol, Signal.Action.BUY, strength, System.currentTimeMillis())
            }
            fastSMA < slowSMA && prevFastSMA >= prevSlowSMA -> {
                val strength = ((slowSMA - fastSMA) / slowSMA) * 100
                Signal(symbol, Signal.Action.SELL, strength, System.currentTimeMillis())
            }
            else -> {
                Signal(symbol, Signal.Action.HOLD, 0.0, System.currentTimeMillis())
            }
        }
    }
}
