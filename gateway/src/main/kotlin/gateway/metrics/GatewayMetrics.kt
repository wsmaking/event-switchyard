package gateway.metrics

import gateway.http.SseHub
import gateway.kafka.KafkaEventPublisher
import gateway.queue.FastPathQueue
import java.lang.management.ManagementFactory
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

class GatewayMetrics(
    private val startedAt: Instant = Instant.now(),
    private val queue: FastPathQueue,
    private val sseHub: SseHub,
    private val kafka: KafkaEventPublisher?
) {
    private val httpRequestsTotal = AtomicLong(0)
    private val httpUnauthorizedTotal = AtomicLong(0)

    private val ordersAcceptedTotal = AtomicLong(0)
    private val ordersRejectedTotal = AtomicLong(0)
    private val cancelsRequestedTotal = AtomicLong(0)

    private val fastPathEnqueueOkTotal = AtomicLong(0)
    private val fastPathEnqueueRejectedTotal = AtomicLong(0)

    private val ordersSentTotal = AtomicLong(0)
    private val cancelsSentTotal = AtomicLong(0)
    private val executionReportsTotal = AtomicLong(0)
    private val ordersUpdatedTotal = AtomicLong(0)

    fun onHttpRequest() {
        httpRequestsTotal.incrementAndGet()
    }

    fun onHttpUnauthorized() {
        httpUnauthorizedTotal.incrementAndGet()
    }

    fun onOrderAccepted() {
        ordersAcceptedTotal.incrementAndGet()
    }

    fun onOrderRejected() {
        ordersRejectedTotal.incrementAndGet()
    }

    fun onCancelRequested() {
        cancelsRequestedTotal.incrementAndGet()
    }

    fun onFastPathEnqueue(ok: Boolean) {
        if (ok) fastPathEnqueueOkTotal.incrementAndGet() else fastPathEnqueueRejectedTotal.incrementAndGet()
    }

    fun onOrderSent() {
        ordersSentTotal.incrementAndGet()
    }

    fun onCancelSent() {
        cancelsSentTotal.incrementAndGet()
    }

    fun onExecutionReport() {
        executionReportsTotal.incrementAndGet()
    }

    fun onOrderUpdated() {
        ordersUpdatedTotal.incrementAndGet()
    }

    fun renderPrometheus(now: Instant = Instant.now()): String {
        val runtime = Runtime.getRuntime()
        val heap = ManagementFactory.getMemoryMXBean().heapMemoryUsage
        val threads = ManagementFactory.getThreadMXBean()
        val uptimeSec = (now.toEpochMilli() - startedAt.toEpochMilli()).coerceAtLeast(0) / 1000.0

        val sb = StringBuilder(2048)

        fun help(name: String, text: String) {
            sb.append("# HELP ").append(name).append(' ').append(text).append('\n')
        }
        fun type(name: String, kind: String) {
            sb.append("# TYPE ").append(name).append(' ').append(kind).append('\n')
        }
        fun counter(name: String, v: Long) {
            sb.append(name).append(' ').append(v).append('\n')
        }
        fun gauge(name: String, v: Number) {
            sb.append(name).append(' ').append(v).append('\n')
        }

        help("gateway_uptime_seconds", "Gateway process uptime in seconds")
        type("gateway_uptime_seconds", "gauge")
        gauge("gateway_uptime_seconds", uptimeSec)

        help("gateway_http_requests_total", "Total number of HTTP requests received by the gateway")
        type("gateway_http_requests_total", "counter")
        counter("gateway_http_requests_total", httpRequestsTotal.get())

        help("gateway_http_unauthorized_total", "Total number of unauthorized HTTP requests (JWT failures)")
        type("gateway_http_unauthorized_total", "counter")
        counter("gateway_http_unauthorized_total", httpUnauthorizedTotal.get())

        help("gateway_orders_accepted_total", "Total number of orders accepted (202)")
        type("gateway_orders_accepted_total", "counter")
        counter("gateway_orders_accepted_total", ordersAcceptedTotal.get())

        help("gateway_orders_rejected_total", "Total number of orders rejected (4xx/5xx)")
        type("gateway_orders_rejected_total", "counter")
        counter("gateway_orders_rejected_total", ordersRejectedTotal.get())

        help("gateway_cancels_requested_total", "Total number of cancel requests accepted")
        type("gateway_cancels_requested_total", "counter")
        counter("gateway_cancels_requested_total", cancelsRequestedTotal.get())

        help("gateway_fastpath_enqueue_ok_total", "Total number of fastpath enqueues accepted")
        type("gateway_fastpath_enqueue_ok_total", "counter")
        counter("gateway_fastpath_enqueue_ok_total", fastPathEnqueueOkTotal.get())

        help("gateway_fastpath_enqueue_rejected_total", "Total number of fastpath enqueues rejected (queue full/closed)")
        type("gateway_fastpath_enqueue_rejected_total", "counter")
        counter("gateway_fastpath_enqueue_rejected_total", fastPathEnqueueRejectedTotal.get())

        help("gateway_fastpath_queue_depth", "Current fastpath queue depth")
        type("gateway_fastpath_queue_depth", "gauge")
        gauge("gateway_fastpath_queue_depth", queue.depth())

        help("gateway_fastpath_queue_capacity", "Fastpath queue capacity")
        type("gateway_fastpath_queue_capacity", "gauge")
        gauge("gateway_fastpath_queue_capacity", queue.capacity())

        help("gateway_orders_sent_total", "Total number of orders sent to exchange")
        type("gateway_orders_sent_total", "counter")
        counter("gateway_orders_sent_total", ordersSentTotal.get())

        help("gateway_cancels_sent_total", "Total number of cancel requests sent to exchange")
        type("gateway_cancels_sent_total", "counter")
        counter("gateway_cancels_sent_total", cancelsSentTotal.get())

        help("gateway_execution_reports_total", "Total number of execution reports processed")
        type("gateway_execution_reports_total", "counter")
        counter("gateway_execution_reports_total", executionReportsTotal.get())

        help("gateway_order_updates_total", "Total number of order updates applied")
        type("gateway_order_updates_total", "counter")
        counter("gateway_order_updates_total", ordersUpdatedTotal.get())

        help("gateway_sse_clients", "Current SSE client count (scope=order/account)")
        type("gateway_sse_clients", "gauge")
        sb.append("gateway_sse_clients{scope=\"order\"} ").append(sseHub.orderClientCount()).append('\n')
        sb.append("gateway_sse_clients{scope=\"account\"} ").append(sseHub.accountClientCount()).append('\n')

        help("gateway_sse_dispatch_queue_depth", "Current depth of SSE dispatch queue")
        type("gateway_sse_dispatch_queue_depth", "gauge")
        gauge("gateway_sse_dispatch_queue_depth", sseHub.dispatchQueueDepth())

        help("gateway_sse_dispatch_dropped_total", "Total number of SSE dispatches dropped due to a full queue")
        type("gateway_sse_dispatch_dropped_total", "counter")
        counter("gateway_sse_dispatch_dropped_total", sseHub.dispatchDroppedTotal())

        if (kafka != null) {
            help("gateway_kafka_publisher_enabled", "Kafka publisher enabled (1/0)")
            type("gateway_kafka_publisher_enabled", "gauge")
            gauge("gateway_kafka_publisher_enabled", if (kafka.isEnabled()) 1 else 0)

            help("gateway_kafka_publisher_queue_depth", "Kafka publisher internal queue depth")
            type("gateway_kafka_publisher_queue_depth", "gauge")
            gauge("gateway_kafka_publisher_queue_depth", kafka.queueDepth())

            help("gateway_kafka_publisher_dropped_total", "Total number of events dropped by kafka publisher due to a full queue")
            type("gateway_kafka_publisher_dropped_total", "counter")
            counter("gateway_kafka_publisher_dropped_total", kafka.droppedTotal())

            help("gateway_kafka_publisher_errors_total", "Total number of kafka publish errors observed in callbacks")
            type("gateway_kafka_publisher_errors_total", "counter")
            counter("gateway_kafka_publisher_errors_total", kafka.publishErrorsTotal())
        }

        help("jvm_heap_used_bytes", "JVM heap used bytes")
        type("jvm_heap_used_bytes", "gauge")
        gauge("jvm_heap_used_bytes", heap.used)

        help("jvm_heap_max_bytes", "JVM heap max bytes")
        type("jvm_heap_max_bytes", "gauge")
        gauge("jvm_heap_max_bytes", heap.max)

        help("jvm_threads", "JVM thread count")
        type("jvm_threads", "gauge")
        gauge("jvm_threads", threads.threadCount)

        help("process_max_memory_bytes", "Maximum memory that the JVM will attempt to use")
        type("process_max_memory_bytes", "gauge")
        gauge("process_max_memory_bytes", runtime.maxMemory())

        return sb.toString()
    }
}

