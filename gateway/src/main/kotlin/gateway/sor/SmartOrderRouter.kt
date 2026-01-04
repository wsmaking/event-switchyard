package gateway.sor

import gateway.exchange.ExchangeClient
import gateway.exchange.ExchangeSimulator
import gateway.exchange.TcpExchangeClient
import gateway.exchange.ExecutionReport
import gateway.order.OrderSnapshot
import java.util.concurrent.ConcurrentHashMap

/**
 * 最小構成のSmart Order Router (SOR)。
 *
 * - 注文の銘柄から送信先（venue）を決める。
 * - orderId と venue の対応を保持して、取消も同じ宛先に流す。
 *
 * まだ「最良価格/流動性/手数料/レート制限」などの高度な判断は入れていない。
 * あくまでSORとしての形だけを持たせたミニマム実装。
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
         * 環境変数（すべて任意）:
         * - SOR_VENUES:            "simA,simB"（未指定なら "sim"）
         * - SOR_DEFAULT_VENUE:     "simA"（未指定なら SOR_VENUES の先頭）
         * - SOR_SYMBOL_ROUTES:     "BTC=simA,ETH=simB"
         *
         * venueごとのシミュレータ設定（任意）:
         * - EXCHANGE_SIM_DELAY_MS_<VENUE>
         * - EXCHANGE_SIM_PARTIAL_STEPS_<VENUE>
         * - EXCHANGE_SIM_REJECT_ALL_<VENUE>  (1/true)
         *
         * 送信手段の切替:
         * - EXCHANGE_TRANSPORT: "sim"（既定） or "tcp"
         * - EXCHANGE_TCP_HOST / EXCHANGE_TCP_PORT
         * - EXCHANGE_TCP_HOST_<VENUE> / EXCHANGE_TCP_PORT_<VENUE>
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
            val transport = (System.getenv("EXCHANGE_TRANSPORT") ?: "sim").lowercase()

            val venues =
                venueNames.associateWith { venue ->
                    when (transport) {
                        "tcp" -> {
                            // TCP取引所（別プロセス）へ接続してExecutionReportを受け取る。
                            val host = envString("EXCHANGE_TCP_HOST", venue) ?: (System.getenv("EXCHANGE_TCP_HOST") ?: "127.0.0.1")
                            val port = envInt("EXCHANGE_TCP_PORT", venue) ?: (System.getenv("EXCHANGE_TCP_PORT")?.toIntOrNull() ?: 9901)
                            TcpExchangeClient(host = host, port = port)
                        }
                        else -> {
                            val delayMs = envLong("EXCHANGE_SIM_DELAY_MS", venue) ?: globalDelayMs
                            val partialSteps = envInt("EXCHANGE_SIM_PARTIAL_STEPS", venue) ?: globalPartialSteps
                            val rejectAll = envBool(System.getenv(envKey("EXCHANGE_SIM_REJECT_ALL", venue))) || globalRejectAll
                            ExchangeSimulator(delayMs = delayMs, partialSteps = partialSteps, rejectAll = rejectAll)
                        }
                    }
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

        private fun envString(base: String, venue: String): String? = System.getenv(envKey(base, venue))

        private fun envBool(v: String?): Boolean = v?.let { it == "1" || it.equals("true", ignoreCase = true) } ?: false
    }
}
