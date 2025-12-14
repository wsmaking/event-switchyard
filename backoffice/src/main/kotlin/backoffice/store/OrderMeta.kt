package backoffice.store

import backoffice.model.Side

data class OrderMeta(
    val accountId: String,
    val orderId: String,
    val symbol: String,
    val side: Side
)

