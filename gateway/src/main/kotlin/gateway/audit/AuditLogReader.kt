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
    fun readOrderEvents(
        accountId: String,
        orderId: String,
        limit: Int?,
        since: java.time.Instant?,
        after: java.time.Instant?
    ): List<AuditEvent> {
        return readEvents(limit) { event ->
            if (event.orderId != orderId) return@readEvents false
            if (event.accountId != null && event.accountId != accountId) return@readEvents false
            matchTime(event.at, since, after)
        }
    }

    fun readAccountEvents(
        accountId: String,
        limit: Int?,
        since: java.time.Instant?,
        after: java.time.Instant?
    ): List<AuditEvent> {
        return readEvents(limit) { event ->
            if (event.accountId != accountId) return@readEvents false
            matchTime(event.at, since, after)
        }
    }

    private fun readEvents(limit: Int?, predicate: (AuditEvent) -> Boolean): List<AuditEvent> {
        if (!Files.exists(path)) return emptyList()
        val safeLimit = normalizeLimit(limit)
        val buffer = ArrayDeque<AuditEvent>(safeLimit)

        Files.newBufferedReader(path).useLines { lines ->
            lines.forEach { line ->
                if (line.isBlank()) return@forEach
                val event = parseEvent(line) ?: return@forEach
                if (!predicate(event)) return@forEach
                buffer.addLast(event)
                if (buffer.size > safeLimit) buffer.removeFirst()
            }
        }

        return buffer.toList()
    }

    private fun matchTime(
        at: java.time.Instant,
        since: java.time.Instant?,
        after: java.time.Instant?
    ): Boolean {
        if (since != null && at.isBefore(since)) return false
        if (after != null && !at.isAfter(after)) return false
        return true
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

    private fun matchTime(
        at: java.time.Instant,
        since: java.time.Instant?,
        after: java.time.Instant?
    ): Boolean {
        if (since != null && at.isBefore(since)) return false
        if (after != null && !at.isAfter(after)) return false
        return true
    }

    private fun matchType(type: String, types: Set<String>?): Boolean {
        if (types == null || types.isEmpty()) return true
        return types.contains(type.lowercase())
    }
}
