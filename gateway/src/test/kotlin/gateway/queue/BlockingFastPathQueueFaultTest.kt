package gateway.queue

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class BlockingFastPathQueueFaultTest {
    @Test
    fun `queue full returns QUEUE_FULL`() {
        val queue = BlockingFastPathQueue(1)

        val first = queue.tryEnqueue(NewOrderCommand("ord-1"))
        val second = queue.tryEnqueue(NewOrderCommand("ord-2"))

        assertTrue(first.ok)
        assertFalse(second.ok)
        assertEquals("QUEUE_FULL", second.reason)
    }
}
