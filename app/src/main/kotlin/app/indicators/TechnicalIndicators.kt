package app.indicators

data class IndicatorResult(
    val value: Double,
    val timestamp: Long
)

object TechnicalIndicators {

    fun sma(prices: List<Double>, period: Int): List<IndicatorResult> {
        if (prices.size < period) return emptyList()

        return prices.windowed(period).mapIndexed { index, window ->
            IndicatorResult(
                value = window.average(),
                timestamp = System.currentTimeMillis() - (prices.size - index - period) * 86400000L
            )
        }
    }

    fun ema(prices: List<Double>, period: Int): List<IndicatorResult> {
        if (prices.size < period) return emptyList()

        val multiplier = 2.0 / (period + 1)
        val results = mutableListOf<IndicatorResult>()

        var ema = prices.take(period).average()
        results.add(IndicatorResult(ema, System.currentTimeMillis() - (prices.size - period) * 86400000L))

        for (i in period until prices.size) {
            ema = (prices[i] - ema) * multiplier + ema
            results.add(IndicatorResult(
                value = ema,
                timestamp = System.currentTimeMillis() - (prices.size - i - 1) * 86400000L
            ))
        }

        return results
    }

    fun rsi(prices: List<Double>, period: Int = 14): List<IndicatorResult> {
        if (prices.size < period + 1) return emptyList()

        val changes = prices.zipWithNext { a, b -> b - a }
        val results = mutableListOf<IndicatorResult>()

        for (i in period until changes.size) {
            val window = changes.subList(i - period, i)
            val gains = window.filter { it > 0 }.average().takeIf { !it.isNaN() } ?: 0.0
            val losses = window.filter { it < 0 }.map { -it }.average().takeIf { !it.isNaN() } ?: 0.0

            val rs = if (losses == 0.0) 100.0 else gains / losses
            val rsi = 100.0 - (100.0 / (1.0 + rs))

            results.add(IndicatorResult(
                value = rsi,
                timestamp = System.currentTimeMillis() - (changes.size - i - 1) * 86400000L
            ))
        }

        return results
    }

    fun macd(
        prices: List<Double>,
        fastPeriod: Int = 12,
        slowPeriod: Int = 26,
        signalPeriod: Int = 9
    ): Triple<List<IndicatorResult>, List<IndicatorResult>, List<IndicatorResult>> {
        val fastEma = ema(prices, fastPeriod)
        val slowEma = ema(prices, slowPeriod)

        if (fastEma.size < slowEma.size) {
            return Triple(emptyList(), emptyList(), emptyList())
        }

        val macdLine = fastEma.zip(slowEma) { fast, slow ->
            IndicatorResult(
                value = fast.value - slow.value,
                timestamp = fast.timestamp
            )
        }

        val signalLine = ema(macdLine.map { it.value }, signalPeriod)

        val histogram = macdLine.zip(signalLine) { macd, signal ->
            IndicatorResult(
                value = macd.value - signal.value,
                timestamp = macd.timestamp
            )
        }

        return Triple(macdLine, signalLine, histogram)
    }

    fun bollingerBands(prices: List<Double>, period: Int = 20, stdDev: Double = 2.0): Triple<List<IndicatorResult>, List<IndicatorResult>, List<IndicatorResult>> {
        if (prices.size < period) return Triple(emptyList(), emptyList(), emptyList())

        val smaValues = sma(prices, period)
        val upper = mutableListOf<IndicatorResult>()
        val lower = mutableListOf<IndicatorResult>()

        prices.windowed(period).forEachIndexed { index, window ->
            val mean = window.average()
            val variance = window.map { (it - mean) * (it - mean) }.average()
            val sd = kotlin.math.sqrt(variance)

            val timestamp = System.currentTimeMillis() - (prices.size - index - period) * 86400000L
            upper.add(IndicatorResult(mean + stdDev * sd, timestamp))
            lower.add(IndicatorResult(mean - stdDev * sd, timestamp))
        }

        return Triple(smaValues, upper, lower)
    }
}
