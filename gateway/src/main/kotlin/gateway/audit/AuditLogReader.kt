package gateway.audit

import com.fasterxml.jackson.databind.ObjectMapper
import gateway.json.Json
import java.nio.file.Files
import java.nio.file.Path

class AuditLogReader(
    private val path: Path,
    private val mapper: ObjectMapper = Json.mapper,
    private val maxLimit: Int = 1000
) {
    fun readOrderEvents(accountId: String, orderId: String, limit: Int?): List<AuditEvent> {
        if (!Files.exists(path)) return emptyList()
        val safeLimit = normalizeLimit(limit)
        val buffer = ArrayDeque<AuditEvent>(safeLimit)

        Files.newBufferedReader(path).useLines { lines ->
            lines.forEach { line ->
                if (line.isBlank()) return@forEach
                val event = parseEvent(line) ?: return@forEach
                if (event.orderId != orderId) return@forEach
                if (event.accountId != null && event.accountId != accountId) return@forEach
                buffer.addLast(event)
                if (buffer.size > safeLimit) buffer.removeFirst()
            }
        }

        return buffer.toList()
    }

    private fun parseEvent(line: String): AuditEvent? {
        return try {
            mapper.readValue(line, AuditEvent::class.java)
        } catch (_: Throwable) {
            null
        }
    }

    private fun normalizeLimit(limit: Int?): Int {
        val raw = limit ?: maxLimit
        return raw.coerceIn(1, maxLimit)
    }
}
