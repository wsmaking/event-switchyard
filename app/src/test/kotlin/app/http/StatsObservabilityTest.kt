package app.http

import app.engine.RouterStats
import app.fast.MetricsSnapshot
import app.fast.PersistenceQueueStats
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class StatsObservabilityTest {
    @Test
    fun `stats expose required observability fields`() {
        val metrics = MetricsSnapshot(
            avgPublishLatencyNs = 1_000,
            avgProcessLatencyNs = 2_000,
            dropCount = 3,
            eventCount = 4,
            publishP50Ns = 10_000,
            publishP99Ns = 20_000,
            publishP999Ns = 30_000,
            processP50Ns = 40_000,
            processP99Ns = 50_000,
            processP999Ns = 60_000
        )
        val pq = PersistenceQueueStats(
            publishCount = 10,
            dropCount = 0,
            processCount = 9,
            errorCount = 1,
            lag = 1,
            writeP50Ns = 1_000,
            writeP99Ns = 2_000,
            writeP999Ns = 3_000
        )

        val stats = RouterStats(
            fastPathCount = 1,
            slowPathCount = 0,
            fallbackCount = 0,
            fastPathMetrics = metrics,
            persistenceQueueStats = pq,
            chronicleQueueStats = null,
            kafkaBridgeStats = null
        )

        val map = stats.toMap()
        val required = listOf(
            "fast_path_process_p50_us",
            "fast_path_process_p99_us",
            "fast_path_process_p999_us",
            "fast_path_drop_count",
            "persistence_queue_error_count",
            "persistence_queue_lag"
        )

        for (key in required) {
            assertTrue(map.containsKey(key), "missing $key")
        }
    }
}
