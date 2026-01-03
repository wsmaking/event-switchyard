package app.strategy

data class StrategyConfig(
    val accountId: String,
    val enabled: Boolean,
    val symbols: List<String>,
    val tickMs: Long,
    val maxOrdersPerMin: Int,
    val cooldownMs: Long,
    val updatedAtMs: Long
) {
    fun symbolsCsv(): String = symbols.joinToString(",")

    companion object {
        fun defaultFromEnv(accountId: String): StrategyConfig {
            val symbols =
                (System.getenv("STRATEGY_SYMBOLS") ?: "7203,6758,9984")
                    .split(',')
                    .map { it.trim() }
                    .filter { it.isNotEmpty() }
            val tickMs = (System.getenv("STRATEGY_TICK_MS") ?: "1000").toLong()
            val maxOrdersPerMin = (System.getenv("STRATEGY_MAX_ORDERS_PER_MIN") ?: "0").toInt()
            val cooldownMs = (System.getenv("STRATEGY_COOLDOWN_MS") ?: "0").toLong()
            return StrategyConfig(
                accountId = accountId,
                enabled = false,
                symbols = if (symbols.isEmpty()) listOf("7203") else symbols,
                tickMs = tickMs.coerceAtLeast(100),
                maxOrdersPerMin = maxOrdersPerMin.coerceAtLeast(0),
                cooldownMs = cooldownMs.coerceAtLeast(0),
                updatedAtMs = System.currentTimeMillis()
            )
        }
    }
}

data class StrategyStorageStatus(
    val storage: String,
    val healthy: Boolean,
    val message: String? = null,
    val lastErrorAtMs: Long? = null
)

data class StrategyConfigSnapshot(
    val config: StrategyConfig,
    val status: StrategyStorageStatus
)

data class StrategyConfigRequest(
    val enabled: Boolean,
    val symbols: List<String>,
    val tickMs: Long,
    val maxOrdersPerMin: Int,
    val cooldownMs: Long
) {
    fun toConfig(accountId: String): StrategyConfig {
        val sanitizedSymbols = symbols.map { it.trim() }.filter { it.isNotEmpty() }
        return StrategyConfig(
            accountId = accountId,
            enabled = enabled,
            symbols = if (sanitizedSymbols.isEmpty()) listOf("7203") else sanitizedSymbols,
            tickMs = tickMs.coerceAtLeast(100),
            maxOrdersPerMin = maxOrdersPerMin.coerceAtLeast(0),
            cooldownMs = cooldownMs.coerceAtLeast(0),
            updatedAtMs = System.currentTimeMillis()
        )
    }
}
