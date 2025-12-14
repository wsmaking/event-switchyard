package gateway.exchange

import gateway.order.OrderStatus
import java.time.Instant

data class ExecutionReport(
    val orderId: String,
    val status: OrderStatus,
    val filledQtyDelta: Long,
    val filledQtyTotal: Long,
    val price: Long?,
    val at: Instant
)

