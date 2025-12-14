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

    init {
        path.parent?.let { Files.createDirectories(it) }
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
        }
    }

    fun replay(apply: (LedgerEntry) -> Unit): ReplayStats {
        if (!Files.exists(path)) return ReplayStats(lines = 0, applied = 0, skipped = 0)

        var lines = 0L
        var applied = 0L
        var skipped = 0L
        Files.newBufferedReader(path).use { reader ->
            while (true) {
                val line = reader.readLine() ?: break
                lines++
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
        return ReplayStats(lines = lines, applied = applied, skipped = skipped)
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

    data class ReplayStats(
        val lines: Long,
        val applied: Long,
        val skipped: Long
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
