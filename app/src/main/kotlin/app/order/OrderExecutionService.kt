package app.order

import app.clients.gateway.GatewayClient
import app.clients.gateway.GatewayOrderRequest
import app.clients.gateway.GatewayOrderSnapshot
import app.clients.gateway.GatewaySubmitResult
import app.events.OrderEvent

/**
 * Gatewayへの注文実行を一箇所に集約するための薄いサービス。
 */
class OrderExecutionService(
    private val gatewayClient: GatewayClient = GatewayClient()
) {
    fun submitOrder(request: GatewayOrderRequest, idempotencyKey: String?): GatewaySubmitResult {
        return gatewayClient.submitOrder(request, idempotencyKey)
    }

    fun submitOrder(order: OrderEvent): GatewaySubmitResult {
        val request = GatewayOrderRequest(
            symbol = order.symbol,
            side = order.side.name,
            type = order.orderType.name,
            qty = order.quantity.toLong(),
            price = order.price.takeIf { it > 0.0 }?.toLong(),
            clientOrderId = order.orderId
        )
        return submitOrder(request, idempotencyKey = order.orderId)
    }

    fun fetchOrder(orderId: String): GatewayOrderSnapshot? {
        return gatewayClient.fetchOrder(orderId)
    }
}
