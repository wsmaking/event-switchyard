package gateway.queue

import gateway.order.OrderSnapshot

data class EnqueueResult(val ok: Boolean, val reason: String? = null)

data class OrderCommand(val order: OrderSnapshot)

interface FastPathQueue : AutoCloseable {
    fun tryEnqueue(cmd: OrderCommand): EnqueueResult
    fun poll(): OrderCommand?
    override fun close() {}
}

