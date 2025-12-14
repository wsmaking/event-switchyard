package gateway.queue

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class BlockingFastPathQueue(
    capacity: Int
) : FastPathQueue {
    private val running = AtomicBoolean(true)
    private val queue = ArrayBlockingQueue<OrderCommand>(capacity)

    override fun tryEnqueue(cmd: OrderCommand): EnqueueResult {
        if (!running.get()) return EnqueueResult(false, "QUEUE_CLOSED")
        val ok = queue.offer(cmd)
        return if (ok) EnqueueResult(true) else EnqueueResult(false, "QUEUE_FULL")
    }

    override fun poll(): OrderCommand? {
        if (!running.get()) return null
        return queue.poll(100, TimeUnit.MILLISECONDS)
    }

    override fun close() {
        running.set(false)
    }
}

