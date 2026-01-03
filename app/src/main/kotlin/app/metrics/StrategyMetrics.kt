package app.metrics

import io.prometheus.client.Counter

object StrategyMetrics {
    private val configUpdateTotal =
        Counter.build()
            .name("strategy_config_update_total")
            .help("Total strategy config updates")
            .register()

    private val configUpdateFailedTotal =
        Counter.build()
            .name("strategy_config_update_failed_total")
            .help("Failed strategy config updates")
            .register()

    private val configValidationFailedTotal =
        Counter.build()
            .name("strategy_config_validation_failed_total")
            .help("Rejected strategy config updates due to validation")
            .register()

    private val storageFallbackTotal =
        Counter.build()
            .name("strategy_storage_fallback_total")
            .help("Times strategy storage fell back from DB")
            .register()

    private val orderFilteredTotal =
        Counter.build()
            .name("strategy_order_filtered_total")
            .labelNames("reason")
            .help("Auto strategy orders filtered by safety rails")
            .register()

    fun recordUpdate() {
        configUpdateTotal.inc()
    }

    fun recordUpdateFailed() {
        configUpdateFailedTotal.inc()
    }

    fun recordValidationFailed() {
        configValidationFailedTotal.inc()
    }

    fun recordFallback() {
        storageFallbackTotal.inc()
    }

    fun recordOrderFiltered(reason: String) {
        orderFilteredTotal.labels(reason).inc()
    }
}
