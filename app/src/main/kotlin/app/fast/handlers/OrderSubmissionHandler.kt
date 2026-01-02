package app.fast.handlers

import app.events.OrderEvent
import app.clients.gateway.GatewayClient
import app.clients.gateway.GatewayOrderRequest
import com.lmax.disruptor.EventHandler

class OrderSubmissionHandler(
    private val gatewayClient: GatewayClient
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
            clientOrderId = order.orderId
        )

        val result = gatewayClient.submitOrder(request, idempotencyKey = order.orderId)
        event.eventType = if (result.accepted) "OrderSubmitted" else "OrderRejected"
    }
}
