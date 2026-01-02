package backoffice.store

import backoffice.model.Side
import java.time.Instant

interface BackOfficeStore : AutoCloseable {
    fun upsertOrderMeta(meta: OrderMeta)

    fun findOrderMeta(orderId: String): OrderMeta?

    fun lastFilledTotal(orderId: String): Long

    fun setLastFilledTotal(orderId: String, filledQtyTotal: Long)

    fun applyFill(
        at: Instant,
        accountId: String,
        orderId: String,
        symbol: String,
        side: Side,
        filledQtyDelta: Long,
        filledQtyTotal: Long,
        price: Long?,
        quoteCcy: String,
        quoteCashDelta: Long,
        feeQuote: Long
    )

    fun listPositions(accountId: String? = null): List<Position>

    fun listBalances(accountId: String? = null): List<Balance>

    fun listFills(accountId: String): List<FillRecord>

    fun listRealizedPnl(accountId: String? = null): List<RealizedPnl>

    fun snapshotState(): BackOfficeSnapshotState

    fun restoreState(state: BackOfficeSnapshotState)

    fun snapshotBalances(accountId: String): Map<String, Long>

    fun snapshotPositions(accountId: String): Map<String, Long>

    override fun close() {}
}
