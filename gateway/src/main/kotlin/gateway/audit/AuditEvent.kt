package gateway.audit

import java.time.Instant

data class AuditEvent(
    val type: String,
    val at: Instant,
    val orderId: String,
    val data: Map<String, Any?> = emptyMap()
)

