package app.fast.handlers

import app.events.OrderEvent
import app.order.OrderExecutionService
import com.lmax.disruptor.EventHandler

// Fast Pathのイベントを「注文を送る処理」に渡すための橋渡し。
// 「注文を決める」処理と「注文を送る」処理を切り分け、将来の検証/再送/計測をここに集約できる。
class OrderSubmissionHandler(
    private val executionService: OrderExecutionService
) : EventHandler<RingBufferEvent> {
    override fun onEvent(event: RingBufferEvent, sequence: Long, endOfBatch: Boolean) {
        if (event.eventType != "Order") return
        val order = event.payload as? OrderEvent ?: return

        val request = GatewayOrderRequest(
            symbol = order.symbol,
            side = order.side.name,
            type = order.orderType.name,
            qty = order.quantity.toLong(),
            price = order.price.takeIf { it > 0.0 }?.toLong(),
            timeInForce = order.timeInForce.name,
            expireAt = order.expireAt,
            clientOrderId = order.orderId
        )

        val result = gatewayClient.submitOrder(request, idempotencyKey = order.orderId)
        event.eventType = if (result.accepted) "OrderSubmitted" else "OrderRejected"
    }
}
