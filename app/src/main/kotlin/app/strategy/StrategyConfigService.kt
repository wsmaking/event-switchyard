package app.strategy

import app.metrics.StrategyMetrics

class StrategyConfigService(
    store: StrategyConfigStore? = null,
    private val accountId: String = System.getenv("ACCOUNT_ID") ?: "acct_demo",
    refreshMs: Long = (System.getenv("STRATEGY_CONFIG_REFRESH_MS") ?: "5000").toLong()
) {
    private val refreshIntervalMs = refreshMs.coerceAtLeast(1000)
    private val lock = Any()
    private val store: StrategyConfigStore?
    @Volatile private var cached: StrategyConfig
    @Volatile private var lastRefreshAtMs: Long = System.currentTimeMillis()
    @Volatile private var status: StrategyStorageStatus

    init {
        val now = System.currentTimeMillis()
        val resolvedStore =
            store ?: run {
                try {
                    StrategyConfigStore()
                } catch (ex: Throwable) {
                    null
                }
            }
        if (resolvedStore == null) {
            store = null
            cached = StrategyConfig.defaultFromEnv(accountId).copy(enabled = false)
            status = StrategyStorageStatus(
                storage = "fallback",
                healthy = false,
                message = "STRATEGY_DB_UNAVAILABLE",
                lastErrorAtMs = now
            )
            StrategyMetrics.recordFallback()
        } else {
            store = resolvedStore
            val loaded = resolvedStore.load(accountId)
            cached = loaded ?: StrategyConfig.defaultFromEnv(accountId).also { resolvedStore.upsert(it) }
            status = StrategyStorageStatus(storage = "db", healthy = true)
        }
    }

    fun current(): StrategyConfig {
        refreshIfNeeded()
        return cached
    }

    fun snapshot(): StrategyConfigSnapshot {
        refreshIfNeeded()
        return StrategyConfigSnapshot(cached, status)
    }

    fun update(request: StrategyConfigRequest): StrategyConfigSnapshot {
        val resolvedStore = store ?: run {
            status = StrategyStorageStatus(
                storage = "fallback",
                healthy = false,
                message = "STRATEGY_DB_UNAVAILABLE",
                lastErrorAtMs = System.currentTimeMillis()
            )
            StrategyMetrics.recordUpdateFailed()
            StrategyMetrics.recordFallback()
            throw IllegalStateException("STRATEGY_DB_UNAVAILABLE")
        }
        val updated = request.toConfig(accountId)
        try {
            resolvedStore.upsert(updated)
        } catch (ex: Throwable) {
            status = StrategyStorageStatus(
                storage = "fallback",
                healthy = false,
                message = "STRATEGY_DB_UNAVAILABLE",
                lastErrorAtMs = System.currentTimeMillis()
            )
            cached = updated.copy(enabled = false)
            StrategyMetrics.recordUpdateFailed()
            StrategyMetrics.recordFallback()
            throw IllegalStateException("STRATEGY_DB_UNAVAILABLE")
        }
        cached = updated
        lastRefreshAtMs = System.currentTimeMillis()
        status = StrategyStorageStatus(storage = "db", healthy = true)
        StrategyMetrics.recordUpdate()
        return StrategyConfigSnapshot(cached, status)
    }

    fun refresh(): StrategyConfig {
        synchronized(lock) {
            return refreshInternal()
        }
    }

    private fun refreshIfNeeded() {
        val now = System.currentTimeMillis()
        if (now - lastRefreshAtMs >= refreshIntervalMs) {
            refresh()
        }
    }

    private fun refreshInternal(): StrategyConfig {
        val now = System.currentTimeMillis()
        val resolvedStore = store
        if (resolvedStore == null) {
            lastRefreshAtMs = now
            return cached
        }
        val latest =
            try {
                resolvedStore.load(accountId)
            } catch (ex: Throwable) {
                status = StrategyStorageStatus(
                    storage = "fallback",
                    healthy = false,
                    message = "STRATEGY_DB_UNAVAILABLE",
                    lastErrorAtMs = now
                )
                cached = cached.copy(enabled = false)
                lastRefreshAtMs = now
                StrategyMetrics.recordFallback()
                return cached
            }
        if (latest != null) {
            cached = latest
            status = StrategyStorageStatus(storage = "db", healthy = true)
        }
        lastRefreshAtMs = now
        return cached
    }
}
