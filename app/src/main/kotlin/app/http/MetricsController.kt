package app.http

import app.engine.Router
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.Counter
import io.prometheus.client.Gauge
import io.prometheus.client.Histogram
import io.prometheus.client.exporter.common.TextFormat
import java.io.StringWriter
import java.nio.charset.StandardCharsets

/**
 * /metrics エンドポイント - Prometheus形式でメトリクス公開
 *
 * HFT Fast Pathの全メトリクスをPrometheus Exposition Formatで出力。
 * Prometheusサーバーから定期的にスクレイピングされる。
 */
class MetricsController(private val router: Router) : HttpHandler {

    // Prometheus メトリクス定義
    private val fastPathLatencyHistogram = Histogram.build()
        .name("fast_path_process_latency_microseconds")
        .help("Fast Path processing latency in microseconds")
        .buckets(1.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 1000.0, 2000.0, 5000.0)
        .register()

    private val fastPathPublishLatencyHistogram = Histogram.build()
        .name("fast_path_publish_latency_microseconds")
        .help("Fast Path publish latency in microseconds")
        .buckets(1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0)
        .register()

    private val persistenceQueueWriteLatencyHistogram = Histogram.build()
        .name("persistence_queue_write_latency_microseconds")
        .help("Persistence Queue write latency in microseconds")
        .buckets(10.0, 50.0, 100.0, 200.0, 500.0, 1000.0, 2000.0, 5000.0)
        .register()

    private val fastPathEventCounter = Counter.build()
        .name("fast_path_events_total")
        .help("Total number of Fast Path events processed")
        .register()

    private val fastPathDropCounter = Counter.build()
        .name("fast_path_drops_total")
        .help("Total number of Fast Path events dropped")
        .register()

    private val persistenceQueueErrorCounter = Counter.build()
        .name("persistence_queue_errors_total")
        .help("Total number of Persistence Queue write errors")
        .register()

    private val fastPathLagGauge = Gauge.build()
        .name("fast_path_lag")
        .help("Fast Path ring buffer lag (events queued)")
        .register()

    private val persistenceQueueLagGauge = Gauge.build()
        .name("persistence_queue_lag")
        .help("Persistence Queue lag (events queued)")
        .register()

    private val jvmHeapUsedGauge = Gauge.build()
        .name("jvm_heap_used_bytes")
        .help("JVM heap memory used in bytes")
        .register()

    private val jvmHeapMaxGauge = Gauge.build()
        .name("jvm_heap_max_bytes")
        .help("JVM heap memory max in bytes")
        .register()

    private val jvmThreadCountGauge = Gauge.build()
        .name("jvm_thread_count")
        .help("JVM thread count")
        .register()

    override fun handle(exchange: HttpExchange) {
        try {
            if (exchange.requestMethod != "GET") {
                sendResponse(exchange, 405, "Method Not Allowed")
                return
            }

            // Routerから最新メトリクスを取得してPrometheusメトリクスを更新
            updatePrometheusMetrics()

            // Prometheus形式でメトリクスを出力
            val writer = StringWriter()
            TextFormat.write004(writer, CollectorRegistry.defaultRegistry.metricFamilySamples())
            val metricsText = writer.toString()

            exchange.responseHeaders.set("Content-Type", TextFormat.CONTENT_TYPE_004)
            sendResponse(exchange, 200, metricsText)
        } catch (e: Exception) {
            sendResponse(exchange, 500, "Internal Server Error: ${e.message}")
        } finally {
            exchange.close()
        }
    }

    /**
     * Routerの現在のメトリクスを取得してPrometheusメトリクスに反映
     */
    private fun updatePrometheusMetrics() {
        val stats = router.getStats()
        val statsMap = stats.toMap()

        // Fast Path Latency (Histogram)
        // 注意: PrometheusのHistogramはobserve()で個別の値を記録するが、
        // ここでは既に集計済みのp50/p99を別途Gaugeとして公開する方が実用的
        // 実際の値は既にHDR Histogramで計測済みなので、ここではGaugeで公開

        // Fast Path Event Count (Counter)
        val currentEventCount = statsMap["fast_path_count"] as? Long ?: 0
        val currentDropCount = statsMap["fast_path_drop_count"] as? Long ?: 0
        fastPathEventCounter.clear()
        fastPathEventCounter.inc(currentEventCount.toDouble())
        fastPathDropCounter.clear()
        fastPathDropCounter.inc(currentDropCount.toDouble())

        // Persistence Queue Error Count (Counter)
        val currentErrorCount = statsMap["persistence_queue_error_count"] as? Long ?: 0
        persistenceQueueErrorCounter.clear()
        persistenceQueueErrorCounter.inc(currentErrorCount.toDouble())

        // Persistence Queue Lag (Gauge)
        val persistenceQueueLag = statsMap["persistence_queue_lag"] as? Long ?: 0
        persistenceQueueLagGauge.set(persistenceQueueLag.toDouble())

        // JVM Metrics (Gauge)
        val runtime = Runtime.getRuntime()
        val memoryMxBean = java.lang.management.ManagementFactory.getMemoryMXBean()
        val heapMemory = memoryMxBean.heapMemoryUsage

        jvmHeapUsedGauge.set(heapMemory.used.toDouble())
        jvmHeapMaxGauge.set(heapMemory.max.toDouble())

        val threadMxBean = java.lang.management.ManagementFactory.getThreadMXBean()
        jvmThreadCountGauge.set(threadMxBean.threadCount.toDouble())
    }

    private fun sendResponse(exchange: HttpExchange, statusCode: Int, body: String) {
        val bytes = body.toByteArray(StandardCharsets.UTF_8)
        exchange.sendResponseHeaders(statusCode, bytes.size.toLong())
        exchange.responseBody.use { it.write(bytes) }
    }

    companion object {
        // Prometheus Gaugeでパーセンタイル値を直接公開
        // (HDR Histogramで計算済みのp50/p99/p999をそのまま公開)
        private val fastPathProcessP50Gauge = Gauge.build()
            .name("fast_path_process_latency_p50_microseconds")
            .help("Fast Path processing latency p50 in microseconds")
            .register()

        private val fastPathProcessP99Gauge = Gauge.build()
            .name("fast_path_process_latency_p99_microseconds")
            .help("Fast Path processing latency p99 in microseconds")
            .register()

        private val fastPathProcessP999Gauge = Gauge.build()
            .name("fast_path_process_latency_p999_microseconds")
            .help("Fast Path processing latency p999 in microseconds")
            .register()

        private val persistenceQueueWriteP99Gauge = Gauge.build()
            .name("persistence_queue_write_latency_p99_microseconds")
            .help("Persistence Queue write latency p99 in microseconds")
            .register()

        /**
         * RouterのメトリクスからPrometheusのGaugeを更新
         */
        fun updatePercentileGauges(statsMap: Map<String, Any>) {
            val p50 = statsMap["fast_path_process_p50_us"] as? Double ?: 0.0
            val p99 = statsMap["fast_path_process_p99_us"] as? Double ?: 0.0
            val p999 = statsMap["fast_path_process_p999_us"] as? Double ?: 0.0
            val persistenceP99 = statsMap["persistence_queue_write_p99_us"] as? Double ?: 0.0

            fastPathProcessP50Gauge.set(p50)
            fastPathProcessP99Gauge.set(p99)
            fastPathProcessP999Gauge.set(p999)
            persistenceQueueWriteP99Gauge.set(persistenceP99)
        }
    }
}
