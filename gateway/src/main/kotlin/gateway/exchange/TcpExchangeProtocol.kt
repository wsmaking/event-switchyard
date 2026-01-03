package gateway.exchange

import gateway.order.OrderSide

enum class TcpExchangeRequestType { NEW, CANCEL }

data class TcpExchangeRequest(
    val type: TcpExchangeRequestType,
    val orderId: String,
    val symbol: String? = null,
    val side: OrderSide? = null,
    val qty: Long? = null,
    val price: Long? = null
)
