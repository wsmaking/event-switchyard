package gateway.audit

import com.fasterxml.jackson.databind.ObjectMapper
import gateway.json.Json
import java.io.BufferedWriter
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

class FileAuditLog(
    private val path: Path,
    private val mapper: ObjectMapper = Json.mapper
) : AuditLog {
    private val running = AtomicBoolean(true)
    private val queue = LinkedBlockingQueue<AuditEvent>(100_000)
    private val writerThread: Thread
    private var writer: BufferedWriter? = null

    init {
        path.parent?.let { Files.createDirectories(it) }
        writer = Files.newBufferedWriter(
            path,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.APPEND
        )

        writerThread = thread(name = "gateway-auditlog", isDaemon = false) {
            while (running.get() || queue.isNotEmpty()) {
                val event = queue.poll(100, TimeUnit.MILLISECONDS) ?: continue
                try {
                    val line = mapper.writeValueAsString(event)
                    writer?.apply {
                        write(line)
                        newLine()
                        flush()
                    }
                } catch (_: Throwable) {
                    // best-effort for now; production needs a proper failure strategy
                }
            }
        }
    }

    override fun append(event: AuditEvent) {
        queue.offer(event)
    }

    override fun close() {
        running.set(false)
        try {
            writerThread.join(5000)
        } catch (_: InterruptedException) {
        }
        try {
            writer?.close()
        } catch (_: Throwable) {
        }
    }
}
