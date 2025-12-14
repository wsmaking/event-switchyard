package gateway.bus

import java.time.Instant

data class BusEvent(
    val type: String,
    val at: Instant,
    val accountId: String,
    val orderId: String?,
    val data: Map<String, Any?> = emptyMap()
)

