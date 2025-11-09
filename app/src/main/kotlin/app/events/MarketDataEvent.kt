package app.events

data class MarketDataEvent(
    val symbol: String,
    val timestamp: Long,
    val price: Double,
    val volume: Long,
    val bid: Double,
    val ask: Double
)
