package app.events

data class OrderEvent(
    val orderId: String,
    val symbol: String,
    val timestamp: Long,
    val side: Side,
    val price: Double,
    val quantity: Int,
    val orderType: OrderType
) {
    enum class Side {
        BUY, SELL
    }

    enum class OrderType {
        MARKET, LIMIT
    }
}
