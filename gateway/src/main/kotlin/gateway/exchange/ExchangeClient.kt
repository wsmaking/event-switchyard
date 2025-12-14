package gateway.exchange

import gateway.order.OrderSnapshot

interface ExchangeClient : AutoCloseable {
    fun sendNewOrder(order: OrderSnapshot, onReport: (ExecutionReport) -> Unit)
    override fun close() {}
}

