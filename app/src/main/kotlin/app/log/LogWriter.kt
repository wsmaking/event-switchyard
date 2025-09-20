package app.log
import app.engine.LogRecord
import com.fasterxml.jackson.module.afterburner.AfterburnerModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.nio.file.Files; import java.nio.file.Path; import java.nio.file.StandardOpenOption.*; import kotlin.concurrent.thread
import java.util.concurrent.atomic.AtomicBoolean
class LogWriter(private val q: LogQueue, private val path: Path,
    private val batch: Int = System.getenv("LOG_BATCH")?.toIntOrNull() ?: 256,
    private val flushMs: Long = System.getenv("LOG_FLUSH_INTERVAL_MS")?.toLongOrNull() ?: 10L
) : AutoCloseable {
    private val running = AtomicBoolean(true)
    private val mapper = jacksonObjectMapper().registerModule(AfterburnerModule())
    private val t = thread(name = "log-writer", start = true) {
        Files.createDirectories(path.parent); val buf = ArrayList<LogRecord>(batch)
        while (running.get()) {
            buf.clear(); val n = q.drain(batch, buf)
            if (n==0) { Thread.sleep(flushMs); continue }
            val sb = StringBuilder(n*64); buf.forEach { r -> sb.append(mapper.writeValueAsString(mapOf("key" to r.key, "len" to r.payload.size))).append('\n') }
            Files.writeString(path, sb.toString(), CREATE, WRITE, APPEND)
        }
    }
    override fun close() { running.set(false); t.join(1000) }
}
