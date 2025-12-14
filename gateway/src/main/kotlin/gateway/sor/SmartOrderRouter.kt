package gateway.sor

import gateway.exchange.ExchangeClient
import gateway.exchange.ExchangeSimulator
import gateway.exchange.ExecutionReport
import gateway.order.OrderSnapshot
import java.util.concurrent.ConcurrentHashMap

/**
 * Minimal Smart Order Router (SOR).
 *
 * - Routes new orders by symbol -> venue (explicit mapping or deterministic hash).
 * - Remembers orderId -> venue to route cancels consistently.
 *
 * This is intentionally small: it provides the architectural "shape" for SOR without implementing
 * smart routing logic (best price / liquidity / latency / fees / rate limits) yet.
 */
class SmartOrderRouter(
    private val venues: Map<String, ExchangeClient>,
    private val defaultVenue: String,
    private val symbolRoutes: Map<String, String> = emptyMap()
) : ExchangeClient {
    private val orderVenue = ConcurrentHashMap<String, String>()

    override fun sendNewOrder(order: OrderSnapshot, onReport: (ExecutionReport) -> Unit) {
        val venue = pickVenue(order.symbol)
        orderVenue[order.orderId] = venue
        (venues[venue] ?: venues.getValue(defaultVenue)).sendNewOrder(order, onReport)
    }

    override fun sendCancel(orderId: String, onReport: (ExecutionReport) -> Unit) {
        val venue = orderVenue[orderId] ?: defaultVenue
        (venues[venue] ?: venues.getValue(defaultVenue)).sendCancel(orderId, onReport)
    }

    override fun close() {
        venues.values.forEach { it.close() }
    }

    private fun pickVenue(symbol: String): String {
        val explicit = symbolRoutes[symbol]
        if (explicit != null && venues.containsKey(explicit)) return explicit

        val keys = venues.keys.sorted()
        if (keys.isEmpty()) return defaultVenue
        if (keys.size == 1) return keys.first()

        val idx = (symbol.hashCode() and Int.MAX_VALUE) % keys.size
        return keys[idx]
    }

    companion object {
        /**
         * Env vars (all optional):
         * - SOR_VENUES:            "simA,simB" (default: "sim")
         * - SOR_DEFAULT_VENUE:     "simA"      (default: first in SOR_VENUES)
         * - SOR_SYMBOL_ROUTES:     "BTC=simA,ETH=simB"
         *
         * Per-venue simulator overrides (optional):
         * - EXCHANGE_SIM_DELAY_MS_<VENUE>
         * - EXCHANGE_SIM_PARTIAL_STEPS_<VENUE>
         * - EXCHANGE_SIM_REJECT_ALL_<VENUE>  (1/true)
         */
        fun fromEnv(): SmartOrderRouter {
            val venueNames =
                (System.getenv("SOR_VENUES") ?: "sim")
                    .split(',')
                    .map { it.trim() }
                    .filter { it.isNotEmpty() }
                    .ifEmpty { listOf("sim") }

            val defaultVenue = (System.getenv("SOR_DEFAULT_VENUE") ?: venueNames.first()).trim().ifEmpty { venueNames.first() }
            val routes = parseSymbolRoutes(System.getenv("SOR_SYMBOL_ROUTES").orEmpty())

            val globalDelayMs = (System.getenv("EXCHANGE_SIM_DELAY_MS") ?: "50").toLong()
            val globalPartialSteps = (System.getenv("EXCHANGE_SIM_PARTIAL_STEPS") ?: "2").toInt()
            val globalRejectAll = envBool(System.getenv("EXCHANGE_SIM_REJECT_ALL"))

            val venues =
                venueNames.associateWith { venue ->
                    val delayMs = envLong("EXCHANGE_SIM_DELAY_MS", venue) ?: globalDelayMs
                    val partialSteps = envInt("EXCHANGE_SIM_PARTIAL_STEPS", venue) ?: globalPartialSteps
                    val rejectAll = envBool(System.getenv(envKey("EXCHANGE_SIM_REJECT_ALL", venue))) || globalRejectAll
                    ExchangeSimulator(delayMs = delayMs, partialSteps = partialSteps, rejectAll = rejectAll)
                }

            return SmartOrderRouter(venues = venues, defaultVenue = defaultVenue, symbolRoutes = routes)
        }

        private fun parseSymbolRoutes(raw: String): Map<String, String> {
            if (raw.isBlank()) return emptyMap()
            return raw
                .split(',')
                .mapNotNull { part ->
                    val p = part.trim()
                    if (p.isEmpty()) return@mapNotNull null
                    val idx = p.indexOf('=')
                    if (idx <= 0 || idx == p.lastIndex) return@mapNotNull null
                    val symbol = p.substring(0, idx).trim()
                    val venue = p.substring(idx + 1).trim()
                    if (symbol.isEmpty() || venue.isEmpty()) return@mapNotNull null
                    symbol to venue
                }
                .toMap()
        }

        private fun envKey(base: String, venue: String): String = "${base}_${venue.uppercase()}"

        private fun envLong(base: String, venue: String): Long? = System.getenv(envKey(base, venue))?.toLongOrNull()

        private fun envInt(base: String, venue: String): Int? = System.getenv(envKey(base, venue))?.toIntOrNull()

        private fun envBool(v: String?): Boolean = v?.let { it == "1" || it.equals("true", ignoreCase = true) } ?: false
    }
}

