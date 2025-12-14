package gateway

import gateway.audit.AuditLogReplay
import gateway.audit.FileAuditLog
import gateway.auth.JwtAuth
import gateway.bus.EventPublisher
import gateway.engine.FastPathEngine
import gateway.exchange.ExchangeClient
import gateway.http.SseHub
import gateway.http.HttpGateway
import gateway.kafka.KafkaEventPublisher
import gateway.metrics.GatewayMetrics
import gateway.order.InMemoryOrderStore
import gateway.order.OrderService
import gateway.risk.SimplePreTradeRisk
import gateway.queue.BlockingFastPathQueue
import gateway.sor.SmartOrderRouter
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
    val eventPublisher: EventPublisher = KafkaEventPublisher()
    val risk = SimplePreTradeRisk()
    val fastPathQueue = BlockingFastPathQueue(capacity = queueCapacity)
    val exchange: ExchangeClient = SmartOrderRouter.fromEnv()
    val jwtAuth = JwtAuth()
    val metrics = GatewayMetrics(queue = fastPathQueue, sseHub = sseHub, kafka = (eventPublisher as? KafkaEventPublisher))
    val fastPathEngine = FastPathEngine(
        queue = fastPathQueue,
        orderStore = orderStore,
        auditLog = auditLog,
        eventPublisher = eventPublisher,
        metrics = metrics,
        exchange = exchange,
        sseHub = sseHub
    )

    val orderService = OrderService(
        orderStore = orderStore,
        auditLog = auditLog,
        eventPublisher = eventPublisher,
        metrics = metrics,
        risk = risk,
        fastPathQueue = fastPathQueue
    )

    val server = HttpGateway(port = port, orderService = orderService, sseHub = sseHub, jwtAuth = jwtAuth, metrics = metrics)
    Runtime.getRuntime().addShutdownHook(Thread {
        server.close()
        fastPathEngine.close()
        exchange.close()
        eventPublisher.close()
        auditLog.close()
        fastPathQueue.close()
        sseHub.close()
    })
    fastPathEngine.start()
    server.start()
}
