package app.log

import app.engine.LogRecord
import java.util.concurrent.ArrayBlockingQueue; import java.util.concurrent.atomic.AtomicLong

class LogQueue(capacity: Int = System.getenv("LOG_CAP")?.toIntOrNull() ?: 65536) {
    private val q = ArrayBlockingQueue<LogRecord>(capacity); private val drops = AtomicLong(0)
    fun offer(rec: LogRecord) { if (!q.offer(rec)) drops.incrementAndGet() }
    fun drain(max: Int, out: MutableList<LogRecord>) = q.drainTo(out, max)
    fun dropCount() = drops.get()
}
