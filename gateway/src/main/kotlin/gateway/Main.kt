package gateway

import gateway.audit.FileAuditLog
import gateway.http.HttpGateway
import gateway.order.InMemoryOrderStore
import gateway.order.OrderService
import gateway.risk.SimplePreTradeRisk
import java.nio.file.Path

fun main() {
    val port = (System.getenv("GATEWAY_PORT") ?: "8081").toInt()
    val auditPath = System.getenv("GATEWAY_AUDIT_PATH") ?: "var/gateway/audit.log"

    val orderStore = InMemoryOrderStore()
    val auditLog = FileAuditLog(Path.of(auditPath))
    val risk = SimplePreTradeRisk()
    val orderService = OrderService(orderStore = orderStore, auditLog = auditLog, risk = risk)

    val server = HttpGateway(port = port, orderService = orderService)
    Runtime.getRuntime().addShutdownHook(Thread { server.close() })
    server.start()
}

