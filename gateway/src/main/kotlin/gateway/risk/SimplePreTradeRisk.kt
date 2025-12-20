package gateway.risk

import gateway.order.CreateOrderRequest
import gateway.order.OrderType

class SimplePreTradeRisk(
    private val maxQty: Long = (System.getenv("RISK_MAX_QTY") ?: "100000").toLong(),
    private val maxNotional: Long? = System.getenv("RISK_MAX_NOTIONAL")?.toLongOrNull()?.takeIf { it > 0 },
    private val allowMarket: Boolean = System.getenv("RISK_ALLOW_MARKET") != "0",
    private val minPrice: Long? = System.getenv("RISK_MIN_PRICE")?.toLongOrNull(),
    private val maxPrice: Long? = System.getenv("RISK_MAX_PRICE")?.toLongOrNull(),
    private val allowedSymbols: Set<String> = parseSymbols(),
    private val allowedSymbolsByAccount: Map<String, Set<String>> = parseSymbolMap("RISK_ALLOWED_SYMBOLS_BY_ACCOUNT"),
    private val allowedTypes: Set<OrderType> = parseTypes("RISK_ALLOWED_TYPES"),
    private val allowedTypesByAccount: Map<String, Set<OrderType>> = parseTypeMap("RISK_ALLOWED_TYPES_BY_ACCOUNT"),
    private val allowedTypesBySymbol: Map<String, Set<OrderType>> = parseTypeMap("RISK_ALLOWED_TYPES_BY_SYMBOL"),
    private val maxQtyByAccountSymbol: Map<String, Long> = parseAccountSymbolMap("RISK_MAX_QTY_BY_ACCOUNT_SYMBOL"),
    private val maxNotionalByAccountSymbol: Map<String, Long> = parseAccountSymbolMap("RISK_MAX_NOTIONAL_BY_ACCOUNT_SYMBOL"),
    private val maxQtyByAccount: Map<String, Long> = parseLongMap("RISK_MAX_QTY_BY_ACCOUNT"),
    private val maxNotionalByAccount: Map<String, Long> = parseLongMap("RISK_MAX_NOTIONAL_BY_ACCOUNT"),
    private val minPriceByAccount: Map<String, Long> = parseLongMap("RISK_MIN_PRICE_BY_ACCOUNT"),
    private val maxPriceByAccount: Map<String, Long> = parseLongMap("RISK_MAX_PRICE_BY_ACCOUNT"),
    private val minPriceBySymbol: Map<String, Long> = parseLongMap("RISK_MIN_PRICE_BY_SYMBOL"),
    private val maxPriceBySymbol: Map<String, Long> = parseLongMap("RISK_MAX_PRICE_BY_SYMBOL"),
    private val rateConfig: RateLimitConfig = RateLimitConfig.fromEnv()
) : PreTradeRisk {
    private val rateLimiter = AccountRateLimiter(rateConfig)

    override fun validate(accountId: String, order: CreateOrderRequest): RiskResult {
        if (!rateLimiter.tryAcquire(accountId)) {
            return RiskResult(false, "RATE_LIMITED")
        }
        val key = accountSymbolKey(accountId, order.symbol)
        val accountSymbolMaxQty = maxQtyByAccountSymbol[key]
        val accountMaxQty = maxQtyByAccount[accountId] ?: maxQty
        val effectiveMaxQty = listOfNotNull(accountSymbolMaxQty, accountMaxQty).minOrNull() ?: accountMaxQty
        if (order.qty > effectiveMaxQty) return RiskResult(false, "QTY_LIMIT_EXCEEDED")
        val accountSymbols = allowedSymbolsByAccount[accountId] ?: allowedSymbols
        if (accountSymbols.isNotEmpty() && !accountSymbols.contains(order.symbol)) {
            return RiskResult(false, "SYMBOL_NOT_ALLOWED")
        }
        val baseTypes = if (allowedTypes.isNotEmpty()) allowedTypes else setOf(OrderType.LIMIT, OrderType.MARKET)
        val accountTypes = allowedTypesByAccount[accountId]
        val symbolTypes = allowedTypesBySymbol[order.symbol]
        var effectiveTypes = if (allowMarket) baseTypes else baseTypes - OrderType.MARKET
        if (accountTypes != null) effectiveTypes = effectiveTypes.intersect(accountTypes)
        if (symbolTypes != null) effectiveTypes = effectiveTypes.intersect(symbolTypes)
        if (effectiveTypes.isNotEmpty() && !effectiveTypes.contains(order.type)) {
            return RiskResult(false, "TYPE_NOT_ALLOWED")
        }
        val effectiveMinPrice = listOfNotNull(
            minPrice,
            minPriceByAccount[accountId],
            minPriceBySymbol[order.symbol]
        ).maxOrNull()
        val effectiveMaxPrice = listOfNotNull(
            maxPrice,
            maxPriceByAccount[accountId],
            maxPriceBySymbol[order.symbol]
        ).minOrNull()
        val accountSymbolMaxNotional = maxNotionalByAccountSymbol[key]
        val accountMaxNotional = maxNotionalByAccount[accountId] ?: maxNotional
        val effectiveMaxNotional = listOfNotNull(accountSymbolMaxNotional, accountMaxNotional).minOrNull()
        if (effectiveMinPrice != null || effectiveMaxPrice != null || effectiveMaxNotional != null) {
            val price = order.price ?: return RiskResult(false, "MISSING_PRICE")
            if (effectiveMinPrice != null && price < effectiveMinPrice) return RiskResult(false, "PRICE_TOO_LOW")
            if (effectiveMaxPrice != null && price > effectiveMaxPrice) return RiskResult(false, "PRICE_TOO_HIGH")
            if (effectiveMaxNotional != null) {
                if (price <= 0) return RiskResult(false, "INVALID_PRICE")
                val limit = effectiveMaxNotional / price
                if (order.qty > limit) return RiskResult(false, "NOTIONAL_LIMIT_EXCEEDED")
            }
        }
        return RiskResult(true)
    }

    companion object {
        private fun parseSymbols(): Set<String> {
            val raw = System.getenv("RISK_ALLOWED_SYMBOLS") ?: return emptySet()
            return raw.split(',')
                .map { it.trim() }
                .filter { it.isNotEmpty() }
                .toSet()
        }

        private fun parseSymbolMap(envKey: String): Map<String, Set<String>> {
            val raw = System.getenv(envKey) ?: return emptyMap()
            val out = mutableMapOf<String, Set<String>>()
            for (entry in raw.split(',')) {
                if (entry.isBlank()) continue
                val parts = entry.split(':', limit = 2)
                if (parts.size != 2) continue
                val key = parts[0].trim()
                val symbols = parts[1]
                    .split('|', ';')
                    .map { it.trim() }
                    .filter { it.isNotEmpty() }
                    .toSet()
                if (key.isNotEmpty() && symbols.isNotEmpty()) {
                    out[key] = symbols
                }
            }
            return out
        }

        private fun parseTypes(envKey: String): Set<OrderType> {
            val raw = System.getenv(envKey) ?: return emptySet()
            return raw.split(',', '|', ';')
                .map { it.trim().uppercase() }
                .mapNotNull {
                    try {
                        OrderType.valueOf(it)
                    } catch (_: IllegalArgumentException) {
                        null
                    }
                }
                .toSet()
        }

        private fun parseTypeMap(envKey: String): Map<String, Set<OrderType>> {
            val raw = System.getenv(envKey) ?: return emptyMap()
            val out = mutableMapOf<String, Set<OrderType>>()
            for (entry in raw.split(',')) {
                if (entry.isBlank()) continue
                val parts = entry.split(':', limit = 2)
                if (parts.size != 2) continue
                val key = parts[0].trim()
                val types = parts[1]
                    .split('|', ';')
                    .map { it.trim().uppercase() }
                    .mapNotNull {
                        try {
                            OrderType.valueOf(it)
                        } catch (_: IllegalArgumentException) {
                            null
                        }
                    }
                    .toSet()
                if (key.isNotEmpty() && types.isNotEmpty()) {
                    out[key] = types
                }
            }
            return out
        }

        private fun parseAccountSymbolMap(envKey: String): Map<String, Long> {
            val raw = System.getenv(envKey) ?: return emptyMap()
            val out = mutableMapOf<String, Long>()
            for (entry in raw.split(',')) {
                if (entry.isBlank()) continue
                val parts = entry.split(':', limit = 2)
                if (parts.size != 2) continue
                val key = parts[0].trim()
                val value = parts[1].trim().toLongOrNull()
                if (key.isNotEmpty() && value != null && value > 0) {
                    out[key] = value
                }
            }
            return out
        }

        private fun accountSymbolKey(accountId: String, symbol: String): String {
            return "$accountId|$symbol"
        }

        private fun parseLongMap(envKey: String): Map<String, Long> {
            val raw = System.getenv(envKey) ?: return emptyMap()
            val out = mutableMapOf<String, Long>()
            for (entry in raw.split(',')) {
                if (entry.isBlank()) continue
                val parts = entry.split(':', limit = 2)
                if (parts.size != 2) continue
                val key = parts[0].trim()
                val value = parts[1].trim().toLongOrNull()
                if (key.isNotEmpty() && value != null && value > 0) {
                    out[key] = value
                }
            }
            return out
        }
    }

    data class RateLimitConfig(
        val perSecond: Long,
        val burst: Long
    ) {
        companion object {
            fun fromEnv(): RateLimitConfig {
                val perSecond = (System.getenv("RISK_RATE_PER_SEC") ?: "0").toLong()
                val burst = (System.getenv("RISK_RATE_BURST") ?: perSecond.toString()).toLong()
                return RateLimitConfig(
                    perSecond = if (perSecond > 0) perSecond else 0,
                    burst = if (burst > 0) burst else 0
                )
            }
        }
    }

    private class AccountRateLimiter(
        private val config: RateLimitConfig
    ) {
        private val buckets = java.util.concurrent.ConcurrentHashMap<String, TokenBucket>()

        fun tryAcquire(accountId: String): Boolean {
            if (config.perSecond <= 0 || config.burst <= 0) return true
            val bucket = buckets.computeIfAbsent(accountId) {
                TokenBucket(config.perSecond, config.burst)
            }
            return bucket.tryAcquire()
        }
    }

    private class TokenBucket(
        private val ratePerSec: Long,
        private val burst: Long
    ) {
        private val lock = Any()
        private var tokens = burst.toDouble()
        private var lastRefillNs = System.nanoTime()

        fun tryAcquire(): Boolean {
            synchronized(lock) {
                refill()
                if (tokens >= 1.0) {
                    tokens -= 1.0
                    return true
                }
                return false
            }
        }

        private fun refill() {
            val now = System.nanoTime()
            val elapsedSec = (now - lastRefillNs) / 1_000_000_000.0
            if (elapsedSec <= 0) return
            val add = elapsedSec * ratePerSec
            tokens = kotlin.math.min(burst.toDouble(), tokens + add)
            lastRefillNs = now
        }
    }
}
