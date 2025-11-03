package app.engine

import app.audit.AuditLogger
import app.fast.FastPathEngine
import app.fast.PersistenceQueueEngine
import app.kafka.ChronicleQueueWriter
import app.kafka.KafkaBridge
import app.tracing.SimpleTracer
import java.util.concurrent.atomic.AtomicLong

/**
 * レイテンシ要件に基づいて、リクエストをFast PathまたはSlow Pathにルーティングする
 *
 * 環境変数:
 * - FAST_PATH_ENABLE: "1"でFast Pathを有効化 (デフォルト: "0")
 * - FAST_PATH_SYMBOLS: Fast Path対象のシンボルをカンマ区切りで指定 (例: "BTC,ETH")
 * - FAST_PATH_FALLBACK: "1"でFast Path失敗時にSlow Pathへフォールバック (デフォルト: "1")
 * - KAFKA_BRIDGE_ENABLE: "1"でKafka Bridgeを有効化 (デフォルト: "0")
 * - AUDIT_LOG_ENABLE: "1"で監査ログを有効化 (デフォルト: "0")
 * - TRACING_ENABLE: "1"で分散トレースを有効化 (デフォルト: "0")
 */
class Router(
    private val slowPath: Engine,
    private val fastPathEnabled: Boolean = System.getenv("FAST_PATH_ENABLE") == "1",
    private val fastPathSymbols: Set<String> = parseFastPathSymbols(),
    private val fallbackEnabled: Boolean = System.getenv("FAST_PATH_FALLBACK") != "0",
    private val kafkaBridgeEnabled: Boolean = System.getenv("KAFKA_BRIDGE_ENABLE") == "1",
    private val auditLogEnabled: Boolean = System.getenv("AUDIT_LOG_ENABLE") == "1"
) : AutoCloseable {

    // 監査ログ (Tick-by-Tick記録、証券コンプライアンス対応)
    private val auditLogger: AuditLogger? = if (auditLogEnabled) {
        AuditLogger()
    } else null

    // 分散トレース (OpenTelemetry互換)
    private val tracer = SimpleTracer()

    // Chronicle Queue (Fast Path → Kafka送信のための永続化)
    private val chronicleWriter: ChronicleQueueWriter? = if (fastPathEnabled && kafkaBridgeEnabled) {
        ChronicleQueueWriter()
    } else null

    // Persistence Queue Engine (Chronicle Queue書き込み専用、別スレッド)
    private val persistenceQueue: PersistenceQueueEngine? = if (chronicleWriter != null) {
        PersistenceQueueEngine(chronicleWriter, auditLogger)
    } else null

    // Fast Path Engine (Persistence Queueを渡す)
    private val fastPath: FastPathEngine? = if (fastPathEnabled) {
        FastPathEngine(persistenceQueue = persistenceQueue, auditLogger = auditLogger)
    } else null

    // Kafka Bridge (Chronicle Queue → Kafka非同期転送)
    private val kafkaBridge: KafkaBridge? = if (kafkaBridgeEnabled && chronicleWriter != null) {
        KafkaBridge(auditLogger = auditLogger).apply { start() }
    } else null

    private val fastPathCount = AtomicLong(0)
    private val slowPathCount = AtomicLong(0)
    private val fallbackCount = AtomicLong(0)

    /**
     * キーに基づいてリクエストをルーティング
     * @return 処理された場合true、拒否された場合false (オーナーシップ)
     */
    fun handle(key: String, payload: ByteArray): Boolean {
        // 分散トレース開始
        val trace = tracer.startTrace("router.handle")

        try {
            // Fast Path判定
            if (fastPath != null && shouldUseFastPath(key)) {
                val published = fastPath.tryPublish(key, payload)

                if (published) {
                    fastPathCount.incrementAndGet()
                    tracer.endTrace(trace, mapOf(
                        "key" to key,
                        "path" to "fast",
                        "payload_size" to payload.size
                    ))
                    return true
                }

                // Fast Pathバッファが満杯
                if (fallbackEnabled) {
                    fallbackCount.incrementAndGet()
                    val result = handleSlowPath(key, payload)
                    tracer.endTrace(trace, mapOf(
                        "key" to key,
                        "path" to "fallback",
                        "payload_size" to payload.size
                    ))
                    return result
                }

                tracer.endTrace(trace, mapOf(
                    "key" to key,
                    "path" to "dropped",
                    "payload_size" to payload.size
                ))
                return false  // フォールバック無効時はドロップ
            }

            // Slow Path (既存処理)
            val result = handleSlowPath(key, payload)
            tracer.endTrace(trace, mapOf(
                "key" to key,
                "path" to "slow",
                "payload_size" to payload.size
            ))
            return result
        } catch (e: Exception) {
            tracer.endTrace(trace, mapOf(
                "key" to key,
                "error" to e.message.toString()
            ))
            throw e
        }
    }

    private fun handleSlowPath(key: String, payload: ByteArray): Boolean {
        val handled = slowPath.handle(key, payload)
        if (handled) {
            slowPathCount.incrementAndGet()
        }
        return handled
    }

    private fun shouldUseFastPath(key: String): Boolean {
        // シンボル未指定の場合、全てFast Pathを使用
        if (fastPathSymbols.isEmpty()) return true

        // キーがFast Path対象シンボルに含まれるか確認
        return fastPathSymbols.contains(key)
    }

    fun getStats(): RouterStats {
        return RouterStats(
            fastPathCount = fastPathCount.get(),
            slowPathCount = slowPathCount.get(),
            fallbackCount = fallbackCount.get(),
            fastPathMetrics = fastPath?.getMetrics()?.snapshot(),
            persistenceQueueStats = persistenceQueue?.getStats(),
            chronicleQueueStats = chronicleWriter?.getStats(),
            kafkaBridgeStats = kafkaBridge?.getStats()
        )
    }

    override fun close() {
        // 順序重要: KafkaBridge → FastPath → PersistenceQueue → ChronicleWriter → AuditLogger
        kafkaBridge?.close()
        fastPath?.close()
        persistenceQueue?.close()
        chronicleWriter?.close()
        auditLogger?.close()
    }

    companion object {
        private fun parseFastPathSymbols(): Set<String> {
            val symbols = System.getenv("FAST_PATH_SYMBOLS") ?: return emptySet()
            return symbols.split(',')
                .map { it.trim() }
                .filter { it.isNotEmpty() }
                .toSet()
        }
    }
}

data class RouterStats(
    val fastPathCount: Long,
    val slowPathCount: Long,
    val fallbackCount: Long,
    val fastPathMetrics: app.fast.MetricsSnapshot?,
    val persistenceQueueStats: app.fast.PersistenceQueueStats?,
    val chronicleQueueStats: app.kafka.ChronicleQueueStats?,
    val kafkaBridgeStats: app.kafka.KafkaBridgeStats?
) {
    fun toMap(): Map<String, Any> {
        val map = mutableMapOf<String, Any>(
            "fast_path_count" to fastPathCount,
            "slow_path_count" to slowPathCount,
            "fallback_count" to fallbackCount,
            "fast_path_ratio" to if (totalCount() > 0) fastPathCount.toDouble() / totalCount() else 0.0
        )

        fastPathMetrics?.let {
            map["fast_path_avg_publish_us"] = it.avgPublishLatencyUs()
            map["fast_path_avg_process_us"] = it.avgProcessLatencyUs()
            map["fast_path_drop_count"] = it.dropCount
            map["fast_path_publish_p50_us"] = it.publishP50Us()
            map["fast_path_publish_p99_us"] = it.publishP99Us()
            map["fast_path_publish_p999_us"] = it.publishP999Us()
            map["fast_path_process_p50_us"] = it.processP50Us()
            map["fast_path_process_p99_us"] = it.processP99Us()
            map["fast_path_process_p999_us"] = it.processP999Us()
        }

        persistenceQueueStats?.let {
            map.putAll(it.toMap())
        }

        chronicleQueueStats?.let {
            map.putAll(it.toMap())
        }

        kafkaBridgeStats?.let {
            map.putAll(it.toMap())
        }

        return map
    }

    private fun totalCount() = fastPathCount + slowPathCount
}
