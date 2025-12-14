package gateway.exchange

import gateway.order.OrderSnapshot

interface ExchangeClient : AutoCloseable {
    fun sendNewOrder(order: OrderSnapshot, onReport: (ExecutionReport) -> Unit)
    fun sendCancel(orderId: String, onReport: (ExecutionReport) -> Unit)
    override fun close() {}
}
