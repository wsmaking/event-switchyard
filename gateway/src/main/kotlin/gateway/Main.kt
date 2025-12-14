package gateway

import gateway.audit.AuditLogReplay
import gateway.audit.FileAuditLog
import gateway.auth.JwtAuth
import gateway.engine.FastPathEngine
import gateway.exchange.ExchangeSimulator
import gateway.http.SseHub
import gateway.http.HttpGateway
import gateway.order.InMemoryOrderStore
import gateway.order.OrderService
import gateway.risk.SimplePreTradeRisk
import gateway.queue.BlockingFastPathQueue
import java.nio.file.Path

fun main() {
    val port = (System.getenv("GATEWAY_PORT") ?: "8081").toInt()
    val auditPath = System.getenv("GATEWAY_AUDIT_PATH") ?: "var/gateway/audit.log"
    val queueCapacity = (System.getenv("GATEWAY_QUEUE_CAPACITY") ?: "65536").toInt()

    val orderStore = InMemoryOrderStore()
    val auditFile = Path.of(auditPath)
    val replayStats = AuditLogReplay(auditFile).replayInto(orderStore)
    if (replayStats.lines > 0) {
        println("Audit replay: $replayStats (path=$auditFile)")
    }

    val auditLog = FileAuditLog(auditFile)
    val sseHub = SseHub()
    val risk = SimplePreTradeRisk()
    val fastPathQueue = BlockingFastPathQueue(capacity = queueCapacity)
    val exchange = ExchangeSimulator()
    val jwtAuth = JwtAuth()
    val fastPathEngine = FastPathEngine(
        queue = fastPathQueue,
        orderStore = orderStore,
        auditLog = auditLog,
        exchange = exchange,
        sseHub = sseHub
    )

    val orderService = OrderService(
        orderStore = orderStore,
        auditLog = auditLog,
        risk = risk,
        fastPathQueue = fastPathQueue
    )

    val server = HttpGateway(port = port, orderService = orderService, sseHub = sseHub, jwtAuth = jwtAuth)
    Runtime.getRuntime().addShutdownHook(Thread {
        server.close()
        fastPathEngine.close()
        exchange.close()
        auditLog.close()
        fastPathQueue.close()
    })
    fastPathEngine.start()
    server.start()
}
