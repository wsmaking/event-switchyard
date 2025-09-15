package app.ownership
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.thread

class AssignmentPoller(
    private val fetch: () -> Set<String>,
    private val ownedRef: AtomicReference<Set<String>>,
    private val intervalMs: Long = 300
) : AutoCloseable {
    @Volatile private var running = true
    private val t = thread(name = "assignment-poller", start = true) {
        while (running) { try { ownedRef.set(fetch().toSet()) } finally { Thread.sleep(intervalMs) } }
    }
    override fun close() { running = false; t.join(1000) }
}
