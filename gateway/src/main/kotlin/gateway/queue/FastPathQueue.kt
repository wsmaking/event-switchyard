package gateway.queue

import java.util.concurrent.CountDownLatch

data class EnqueueResult(val ok: Boolean, val reason: String? = null)

sealed interface FastPathCommand {
    val orderId: String
}

data class NewOrderCommand(override val orderId: String, val auditReady: CountDownLatch? = null) : FastPathCommand
data class CancelOrderCommand(override val orderId: String) : FastPathCommand

interface FastPathQueue : AutoCloseable {
    fun tryEnqueue(cmd: FastPathCommand): EnqueueResult
    fun poll(): FastPathCommand?
    fun depth(): Int
    fun capacity(): Int
    override fun close() {}
}
