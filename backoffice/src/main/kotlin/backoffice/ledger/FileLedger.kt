package backoffice.ledger

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import backoffice.json.Json
import backoffice.model.Side
import java.io.BufferedWriter
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.time.Instant

class FileLedger(
    private val path: Path,
    private val mapper: ObjectMapper = Json.mapper
) : AutoCloseable {
    private val lock = Any()
    private val writer: BufferedWriter
    @Volatile private var lineCount: Long = 0

    init {
        path.parent?.let { Files.createDirectories(it) }
        lineCount = if (Files.exists(path)) countLines(path) else 0
        writer =
            Files.newBufferedWriter(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND
            )
    }

    fun append(entry: LedgerEntry) {
        val json = mapper.writeValueAsString(entryToJson(entry))
        synchronized(lock) {
            writer.write(json)
            writer.newLine()
            writer.flush()
            lineCount += 1
        }
    }

    fun replay(apply: (LedgerEntry) -> Unit): ReplayStats {
        return replayFromLine(0, apply)
    }

    fun replayFromLine(startLine: Long, apply: (LedgerEntry) -> Unit): ReplayStats {
        if (!Files.exists(path)) return ReplayStats(lines = 0, applied = 0, skipped = 0, durationMs = 0)

        val startNs = System.nanoTime()
        var lines = 0L
        var applied = 0L
        var skipped = 0L
        Files.newBufferedReader(path).use { reader ->
            while (true) {
                val line = reader.readLine() ?: break
                lines++
                if (lines <= startLine) continue
                if (line.isBlank()) continue
                val entry = parseLine(line)
                if (entry == null) {
                    skipped++
                    continue
                }
                try {
                    apply(entry)
                    applied++
                } catch (_: Throwable) {
                    skipped++
                }
            }
        }
        val durationMs = (System.nanoTime() - startNs) / 1_000_000
        return ReplayStats(lines = lines, applied = applied, skipped = skipped, durationMs = durationMs)
    }

    fun currentLineCount(): Long = lineCount

    fun readEntries(
        accountId: String,
        orderId: String?,
        limit: Int,
        since: Instant?,
        after: Instant?,
        types: Set<String>?
    ): List<LedgerEntry> {
        if (limit <= 0) return emptyList()
        if (!Files.exists(path)) return emptyList()

        val bucket = ArrayDeque<LedgerEntry>(limit.coerceAtMost(10_000))
        Files.newBufferedReader(path).use { reader ->
            while (true) {
                val line = reader.readLine() ?: break
                if (line.isBlank()) continue
                val entry = parseLine(line) ?: continue
                if (entry.accountId != accountId) continue
                if (orderId != null && entry.orderId != orderId) continue
                if (!matchTime(entry.at, since, after)) continue
                if (!matchType(entry.type, types)) continue
                bucket.addLast(entry)
                if (bucket.size > limit) {
                    bucket.removeFirst()
                }
            }
        }
        return bucket.toList()
    }

    fun compactFromLine(startLine: Long, outputPath: Path): ReplayStats {
        if (!Files.exists(path)) return ReplayStats(lines = 0, applied = 0, skipped = 0, durationMs = 0)
        val startNs = System.nanoTime()
        var lines = 0L
        var kept = 0L
        var skipped = 0L
        outputPath.parent?.let { Files.createDirectories(it) }
        Files.newBufferedWriter(
            outputPath,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE
        ).use { writer ->
            Files.newBufferedReader(path).use { reader ->
                while (true) {
                    val line = reader.readLine() ?: break
                    lines++
                    if (lines <= startLine) {
                        skipped++
                        continue
                    }
                    writer.write(line)
                    writer.newLine()
                    kept++
                }
            }
        }
        val durationMs = (System.nanoTime() - startNs) / 1_000_000
        return ReplayStats(lines = lines, applied = kept, skipped = skipped, durationMs = durationMs)
    }

    override fun close() {
        synchronized(lock) {
            try {
                writer.flush()
            } catch (_: Throwable) {
            }
            try {
                writer.close()
            } catch (_: Throwable) {
            }
        }
    }

    private fun countLines(path: Path): Long {
        var count = 0L
        Files.newBufferedReader(path).use { reader ->
            while (reader.readLine() != null) {
                count++
            }
        }
        return count
    }

    private fun matchTime(at: Instant, since: Instant?, after: Instant?): Boolean {
        if (since != null && at.isBefore(since)) return false
        if (after != null && !at.isAfter(after)) return false
        return true
    }

    private fun matchType(type: String, types: Set<String>?): Boolean {
        if (types.isNullOrEmpty()) return true
        return types.contains(type.lowercase())
    }

    data class ReplayStats(
        val lines: Long,
        val applied: Long,
        val skipped: Long,
        val durationMs: Long
    )

    private fun entryToJson(entry: LedgerEntry): Map<String, Any?> {
        return when (entry) {
            is LedgerOrderAccepted ->
                mapOf(
                    "type" to entry.type,
                    "at" to entry.at,
                    "accountId" to entry.accountId,
                    "orderId" to entry.orderId,
                    "symbol" to entry.symbol,
                    "side" to entry.side.name
                )
            is LedgerFill ->
                mapOf(
                    "type" to entry.type,
                    "at" to entry.at,
                    "accountId" to entry.accountId,
                    "orderId" to entry.orderId,
                    "symbol" to entry.symbol,
                    "side" to entry.side.name,
                    "filledQtyDelta" to entry.filledQtyDelta,
                    "filledQtyTotal" to entry.filledQtyTotal,
                    "price" to entry.price,
                    "quoteCcy" to entry.quoteCcy,
                    "quoteCashDelta" to entry.quoteCashDelta,
                    "feeQuote" to entry.feeQuote
                )
        }
    }

    private fun parseLine(line: String): LedgerEntry? {
        val root = try {
            mapper.readTree(line)
        } catch (_: Throwable) {
            return null
        }
        val type = root.text("type") ?: return null
        val at = root.instant("at") ?: return null
        val accountId = root.text("accountId") ?: return null
        val orderId = root.text("orderId") ?: return null

        return when (type) {
            "OrderAccepted" -> {
                val symbol = root.text("symbol") ?: return null
                val side = root.text("side")?.let { Side.valueOf(it) } ?: return null
                LedgerOrderAccepted(at = at, accountId = accountId, orderId = orderId, symbol = symbol, side = side)
            }
            "Fill" -> {
                val symbol = root.text("symbol") ?: return null
                val side = root.text("side")?.let { Side.valueOf(it) } ?: return null
                val filledQtyDelta = root.long("filledQtyDelta") ?: return null
                val filledQtyTotal = root.long("filledQtyTotal") ?: return null
                val price = root.long("price")
                val quoteCcy = root.text("quoteCcy") ?: return null
                val quoteCashDelta = root.long("quoteCashDelta") ?: return null
                val feeQuote = root.long("feeQuote") ?: 0L
                LedgerFill(
                    at = at,
                    accountId = accountId,
                    orderId = orderId,
                    symbol = symbol,
                    side = side,
                    filledQtyDelta = filledQtyDelta,
                    filledQtyTotal = filledQtyTotal,
                    price = price,
                    quoteCcy = quoteCcy,
                    quoteCashDelta = quoteCashDelta,
                    feeQuote = feeQuote
                )
            }
            else -> null
        }
    }

    private fun JsonNode.text(field: String): String? {
        val n = this.get(field) ?: return null
        if (n.isNull) return null
        return n.asText()
    }

    private fun JsonNode.long(field: String): Long? {
        val n = this.get(field) ?: return null
        if (n.isNull) return null
        return n.asLong()
    }

    private fun JsonNode.instant(field: String): Instant? {
        val n = this.get(field) ?: return null
        if (n.isNull) return null
        return when {
            n.isTextual -> {
                try {
                    Instant.parse(n.asText())
                } catch (_: Throwable) {
                    null
                }
            }
            else -> null
        }
    }
}
