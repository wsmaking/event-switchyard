package gateway.audit

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import gateway.exchange.ExecutionReport
import gateway.json.Json
import gateway.order.InMemoryOrderStore
import gateway.order.OrderSide
import gateway.order.OrderSnapshot
import gateway.order.OrderStatus
import gateway.order.OrderType
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import kotlin.math.floor
import kotlin.math.roundToLong

class AuditLogReplay(
    private val path: Path,
    private val mapper: ObjectMapper = Json.mapper
) {
    data class ReplayStats(
        val lines: Long,
        val applied: Long,
        val skipped: Long
    )

    private data class Parsed(
        val index: Long,
        val type: String,
        val at: Instant,
        val accountId: String?,
        val orderId: String,
        val data: JsonNode
    )

    fun replayInto(orderStore: InMemoryOrderStore): ReplayStats {
        if (!Files.exists(path)) return ReplayStats(lines = 0, applied = 0, skipped = 0)

        var lines = 0L
        var applied = 0L
        var skipped = 0L
        val parsed = mutableListOf<Parsed>()

        Files.newBufferedReader(path).use { reader ->
            while (true) {
                val line = reader.readLine() ?: break
                lines++
                if (line.isBlank()) continue
                val p = parseLine(line, index = lines)
                if (p == null) {
                    skipped++
                } else {
                    parsed.add(p)
                }
            }
        }

        parsed.sortWith(compareBy<Parsed>({ it.at }, { it.index }))
        for (p in parsed) {
            val ok = applyParsed(orderStore, p)
            if (ok) applied++ else skipped++
        }

        return ReplayStats(lines = lines, applied = applied, skipped = skipped)
    }

    private fun parseLine(line: String, index: Long): Parsed? {
        val root = try {
            mapper.readTree(line)
        } catch (_: Throwable) {
            return null
        }

        val type = root.text("type") ?: return null
        val at = root.instant("at") ?: return null
        val accountId = root.text("accountId")?.takeIf { it.isNotBlank() }
        val orderId = root.text("orderId") ?: return null
        val data = root.path("data")
        return Parsed(index = index, type = type, at = at, accountId = accountId, orderId = orderId, data = data)
    }

    private fun applyParsed(orderStore: InMemoryOrderStore, p: Parsed): Boolean {
        val type = p.type
        val at = p.at
        val orderId = p.orderId
        val data = p.data

        return try {
            when (type) {
                "OrderAccepted" -> {
                    val accountId = (p.accountId ?: data.text("accountId") ?: "legacy").trim()
                    val symbol = data.text("symbol") ?: return false
                    val side = data.text("side")?.let { OrderSide.valueOf(it) } ?: return false
                    val orderType = data.text("type")?.let { OrderType.valueOf(it) } ?: return false
                    val qty = data.long("qty") ?: return false
                    val price = data.long("price")
                    val clientOrderId = data.text("clientOrderId")
                    val idempotencyKey = data.text("idempotencyKey")

                    val snapshot = OrderSnapshot(
                        orderId = orderId,
                        accountId = accountId,
                        clientOrderId = clientOrderId,
                        symbol = symbol,
                        side = side,
                        type = orderType,
                        qty = qty,
                        price = price,
                        status = OrderStatus.ACCEPTED,
                        acceptedAt = at,
                        lastUpdateAt = at,
                        filledQty = 0
                    )
                    orderStore.put(snapshot, idempotencyKey)
                    true
                }

                "OrderSent" -> {
                    orderStore.update(orderId) { it.copy(status = OrderStatus.SENT, lastUpdateAt = at) }
                    true
                }

                "CancelRequested" -> {
                    orderStore.update(orderId) { it.copy(status = OrderStatus.CANCEL_REQUESTED, lastUpdateAt = at) }
                    true
                }

                "CancelSent" -> {
                    // keep status as CANCEL_REQUESTED until ER comes back
                    orderStore.update(orderId) { it.copy(lastUpdateAt = at) }
                    true
                }

                "ExecutionReport" -> {
                    val status = data.text("status")?.let { OrderStatus.valueOf(it) } ?: return false
                    val filledQtyDelta = data.long("filledQtyDelta") ?: 0
                    val filledQtyTotal = data.long("filledQtyTotal") ?: return false
                    val price = data.long("price")
                    orderStore.applyExecutionReport(
                        ExecutionReport(
                            orderId = orderId,
                            status = status,
                            filledQtyDelta = filledQtyDelta,
                            filledQtyTotal = filledQtyTotal,
                            price = price,
                            at = at
                        )
                    )
                    true
                }

                "OrderUpdated" -> {
                    val status = data.text("status")?.let { OrderStatus.valueOf(it) } ?: return false
                    val filledQty = data.long("filledQty") ?: 0
                    orderStore.update(orderId) { it.copy(status = status, filledQty = filledQty, lastUpdateAt = at) }
                    true
                }

                else -> false
            }
        } catch (_: Throwable) {
            false
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
            n.isNumber -> {
                val d = n.asDouble()
                val sec = floor(d).toLong()
                val nanos = ((d - sec) * 1_000_000_000.0).roundToLong().coerceIn(0, 999_999_999)
                try {
                    Instant.ofEpochSecond(sec, nanos)
                } catch (_: Throwable) {
                    null
                }
            }
            n.isArray && n.size() == 2 -> {
                val sec = n[0].asLong()
                val nanos = n[1].asLong().coerceIn(0, 999_999_999)
                try {
                    Instant.ofEpochSecond(sec, nanos)
                } catch (_: Throwable) {
                    null
                }
            }
            else -> null
        }
    }
}
