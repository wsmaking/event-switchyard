package gateway.http

import gateway.audit.AuditLogReader
import gateway.audit.FileAuditLog
import gateway.auth.JwtAuth
import gateway.bus.NoopEventPublisher
import gateway.metrics.GatewayMetrics
import gateway.order.InMemoryOrderStore
import gateway.order.OrderService
import gateway.queue.BlockingFastPathQueue
import gateway.risk.SimplePreTradeRisk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.net.HttpURLConnection
import java.net.ServerSocket
import java.net.URL
import java.nio.file.Files

class HttpGatewaySmokeTest {
    @Test
    fun `health and metrics respond`() {
        val port = ServerSocket(0).use { it.localPort }
        val auditPath = Files.createTempFile("audit", ".log")

        val queue = BlockingFastPathQueue(8)
        val sseHub = SseHub()
        val metrics = GatewayMetrics(queue = queue, sseHub = sseHub, kafka = null)
        val auditLog = FileAuditLog(auditPath)
        val service = OrderService(
            orderStore = InMemoryOrderStore(),
            auditLog = auditLog,
            eventPublisher = NoopEventPublisher,
            metrics = metrics,
            risk = SimplePreTradeRisk(),
            fastPathQueue = queue
        )

        val gateway = HttpGateway(
            port = port,
            orderService = service,
            sseHub = sseHub,
            auditLogReader = AuditLogReader(auditPath),
            jwtAuth = JwtAuth("test-secret"),
            metrics = metrics
        )

        gateway.start()
        try {
            val health = URL("http://localhost:$port/health").openConnection() as HttpURLConnection
            health.requestMethod = "GET"
            assertEquals(200, health.responseCode)
            health.disconnect()

            val metricsConn = URL("http://localhost:$port/metrics").openConnection() as HttpURLConnection
            metricsConn.requestMethod = "GET"
            val body = metricsConn.inputStream.bufferedReader().readText()
            assertTrue(body.contains("gateway_http_requests_total"))
            assertTrue(body.contains("gateway_fastpath_queue_capacity"))
            metricsConn.disconnect()
        } finally {
            gateway.close()
            sseHub.close()
            auditLog.close()
        }
    }
}
