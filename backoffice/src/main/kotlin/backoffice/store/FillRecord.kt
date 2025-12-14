package backoffice.store

import backoffice.model.Side
import java.time.Instant

data class FillRecord(
    val at: Instant,
    val accountId: String,
    val orderId: String,
    val symbol: String,
    val side: Side,
    val filledQtyDelta: Long,
    val filledQtyTotal: Long,
    val price: Long?,
    val quoteCcy: String,
    val quoteCashDelta: Long,
    val feeQuote: Long
)

