package app.http

import app.engine.Router
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets

/**
 * /health エンドポイント - ヘルスチェック & システム情報
 *
 * 本番環境でロードバランサーやモニタリングツールから使用される。
 * - ステータス: healthy / degraded / unhealthy
 * - JVM情報: メモリ、スレッド、GC
 * - システム情報: CPU、稼働時間
 */
class HealthController(private val router: Router) : HttpHandler {
    private val objectMapper = jacksonObjectMapper()
    private val runtime = Runtime.getRuntime()
    private val memoryMxBean = ManagementFactory.getMemoryMXBean()
    private val threadMxBean = ManagementFactory.getThreadMXBean()
    private val gcMxBeans = ManagementFactory.getGarbageCollectorMXBeans()
    private val startTimeMs = System.currentTimeMillis()

    override fun handle(exchange: HttpExchange) {
        try {
            if (exchange.requestMethod != "GET") {
                sendResponse(exchange, 405, "Method Not Allowed")
                return
            }

            val health = collectHealth()
            val statusCode = when (health["status"]) {
                "healthy" -> 200
                "degraded" -> 200  // 警告だがサービス継続
                "unhealthy" -> 503
                else -> 500
            }

            val json = objectMapper.writeValueAsString(health)
            exchange.responseHeaders.set("Content-Type", "application/json")
            sendResponse(exchange, statusCode, json)
        } catch (e: Exception) {
            sendResponse(exchange, 500, "Internal Server Error: ${e.message}")
        } finally {
            exchange.close()
        }
    }

    private fun collectHealth(): Map<String, Any> {
        val stats = router.getStats()
        val heapMemory = memoryMxBean.heapMemoryUsage
        val nonHeapMemory = memoryMxBean.nonHeapMemoryUsage

        // ヘルスステータス判定
        val status = determineStatus(stats.toMap())

        val uptimeMs = System.currentTimeMillis() - startTimeMs

        return mapOf(
            "status" to status,
            "timestamp" to System.currentTimeMillis(),
            "uptime_ms" to uptimeMs,
            "uptime_human" to formatUptime(uptimeMs),

            // JVM情報
            "jvm" to mapOf(
                "memory" to mapOf(
                    "heap_used_mb" to (heapMemory.used / 1024 / 1024),
                    "heap_max_mb" to (heapMemory.max / 1024 / 1024),
                    "heap_usage_percent" to (heapMemory.used.toDouble() / heapMemory.max * 100),
                    "non_heap_used_mb" to (nonHeapMemory.used / 1024 / 1024)
                ),
                "threads" to mapOf(
                    "count" to threadMxBean.threadCount,
                    "peak" to threadMxBean.peakThreadCount,
                    "daemon" to threadMxBean.daemonThreadCount
                ),
                "gc" to gcMxBeans.map { gc ->
                    mapOf(
                        "name" to gc.name,
                        "collection_count" to gc.collectionCount,
                        "collection_time_ms" to gc.collectionTime
                    )
                }
            ),

            // Fast Path健全性
            "fast_path" to mapOf(
                "enabled" to (stats.fastPathMetrics != null),
                "p99_us" to (stats.fastPathMetrics?.processP99Us() ?: 0.0),
                "drop_count" to (stats.fastPathMetrics?.dropCount ?: 0),
                "error_count" to (stats.persistenceQueueStats?.errorCount ?: 0)
            )
        )
    }

    private fun determineStatus(stats: Map<String, Any>): String {
        val p99Us = stats["fast_path_process_p99_us"] as? Double ?: 0.0
        val dropCount = stats["fast_path_drop_count"] as? Long ?: 0
        val errorCount = stats["persistence_queue_error_count"] as? Long ?: 0
        val heapMemory = memoryMxBean.heapMemoryUsage
        val heapUsagePercent = (heapMemory.used.toDouble() / heapMemory.max * 100)

        // unhealthy判定
        if (p99Us > 200) return "unhealthy"  // p99が目標の2倍超過
        if (dropCount > 100) return "unhealthy"  // ドロップが多い
        if (errorCount > 10) return "unhealthy"  // エラーが多い
        if (heapUsagePercent > 95) return "unhealthy"  // メモリ枯渇

        // degraded判定
        if (p99Us > 100) return "degraded"  // p99が目標超過
        if (dropCount > 0) return "degraded"  // ドロップあり
        if (errorCount > 0) return "degraded"  // エラーあり
        if (heapUsagePercent > 85) return "degraded"  // メモリ使用率高

        return "healthy"
    }

    private fun formatUptime(uptimeMs: Long): String {
        val seconds = uptimeMs / 1000
        val minutes = seconds / 60
        val hours = minutes / 60
        val days = hours / 24

        return when {
            days > 0 -> "${days}d ${hours % 24}h"
            hours > 0 -> "${hours}h ${minutes % 60}m"
            minutes > 0 -> "${minutes}m ${seconds % 60}s"
            else -> "${seconds}s"
        }
    }

    private fun sendResponse(exchange: HttpExchange, statusCode: Int, body: String) {
        val bytes = body.toByteArray(StandardCharsets.UTF_8)
        exchange.sendResponseHeaders(statusCode, bytes.size.toLong())
        exchange.responseBody.use { it.write(bytes) }
    }
}
