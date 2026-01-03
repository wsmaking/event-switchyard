package app.events

data class OrderEvent(
    val orderId: String,
    val symbol: String,
    val timestamp: Long,
    val side: Side,
    val price: Double,
    val quantity: Int,
    val orderType: OrderType,
    val timeInForce: TimeInForce = TimeInForce.GTC,
    val expireAt: Long? = null
) {
    enum class Side {
        BUY, SELL
    }

    enum class OrderType {
        MARKET, LIMIT
    }

    enum class TimeInForce {
        GTC, GTD
    }
}
