package gateway.queue

data class EnqueueResult(val ok: Boolean, val reason: String? = null)

sealed interface FastPathCommand {
    val orderId: String
}

data class NewOrderCommand(override val orderId: String) : FastPathCommand
data class CancelOrderCommand(override val orderId: String) : FastPathCommand

interface FastPathQueue : AutoCloseable {
    fun tryEnqueue(cmd: FastPathCommand): EnqueueResult
    fun poll(): FastPathCommand?
    override fun close() {}
}
