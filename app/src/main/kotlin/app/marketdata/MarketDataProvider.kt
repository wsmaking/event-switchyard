package app.marketdata

data class Quote(
    val symbol: String,
    val price: Double,
    val timestamp: Long,
    val volume: Long? = null,
    val bid: Double? = null,
    val ask: Double? = null
)

interface MarketDataProvider {
    fun getQuote(symbol: String): Quote?
    fun getHistoricalPrices(symbol: String, days: Int): List<Quote>
}
