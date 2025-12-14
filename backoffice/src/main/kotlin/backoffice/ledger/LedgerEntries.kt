package backoffice.ledger

import backoffice.model.Side
import java.time.Instant

sealed interface LedgerEntry {
    val type: String
    val at: Instant
    val accountId: String
    val orderId: String?
}

data class LedgerOrderAccepted(
    override val at: Instant,
    override val accountId: String,
    override val orderId: String,
    val symbol: String,
    val side: Side
) : LedgerEntry {
    override val type: String = "OrderAccepted"
}

data class LedgerFill(
    override val at: Instant,
    override val accountId: String,
    override val orderId: String,
    val symbol: String,
    val side: Side,
    val filledQtyDelta: Long,
    val filledQtyTotal: Long,
    val price: Long?,
    val quoteCcy: String,
    val quoteCashDelta: Long,
    val feeQuote: Long
) : LedgerEntry {
    override val type: String = "Fill"
}

