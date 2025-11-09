package app.marketdata

import kotlin.random.Random

class MockMarketDataProvider : MarketDataProvider {
    private val basePrices = mapOf(
        "AAPL" to 175.0,
        "GOOGL" to 140.0,
        "MSFT" to 380.0,
        "TSLA" to 250.0,
        "NVDA" to 500.0,
        "7203" to 2500.0,
        "6758" to 13500.0,
        "9984" to 6200.0,
        "6861" to 52000.0,
        "8306" to 1200.0
    )

    override fun getQuote(symbol: String): Quote? {
        val basePrice = basePrices[symbol] ?: return null
        val variance = basePrice * 0.02
        val price = basePrice + (Random.nextDouble() - 0.5) * variance

        return Quote(
            symbol = symbol,
            price = price,
            timestamp = System.currentTimeMillis(),
            volume = Random.nextLong(1_000_000, 10_000_000),
            bid = price - 0.01,
            ask = price + 0.01
        )
    }

    override fun getHistoricalPrices(symbol: String, days: Int): List<Quote> {
        val basePrice = basePrices[symbol] ?: return emptyList()
        val now = System.currentTimeMillis()
        val dayMillis = 24 * 60 * 60 * 1000L

        return (0 until days).map { i ->
            val variance = basePrice * 0.02
            val price = basePrice + (Random.nextDouble() - 0.5) * variance

            Quote(
                symbol = symbol,
                price = price,
                timestamp = now - (i * dayMillis),
                volume = Random.nextLong(1_000_000, 10_000_000)
            )
        }.reversed()
    }
}
