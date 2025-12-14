package gateway.order

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import java.time.Instant

enum class OrderSide { BUY, SELL }
enum class OrderType { LIMIT, MARKET }

data class CreateOrderRequest @JsonCreator constructor(
    @JsonProperty("symbol") val symbol: String,
    @JsonProperty("side") val side: OrderSide,
    @JsonProperty("type") val type: OrderType,
    @JsonProperty("qty") val qty: Long,
    @JsonProperty("price") val price: Long? = null,
    @JsonProperty("clientOrderId") val clientOrderId: String? = null
)

enum class OrderStatus {
    ACCEPTED,
    SENT,
    CANCEL_REQUESTED,
    PARTIALLY_FILLED,
    FILLED,
    CANCELED,
    REJECTED
}

data class OrderSnapshot(
    val orderId: String,
    val accountId: String,
    val clientOrderId: String?,
    val symbol: String,
    val side: OrderSide,
    val type: OrderType,
    val qty: Long,
    val price: Long?,
    val status: OrderStatus,
    val acceptedAt: Instant,
    val lastUpdateAt: Instant = acceptedAt,
    val filledQty: Long = 0
)
