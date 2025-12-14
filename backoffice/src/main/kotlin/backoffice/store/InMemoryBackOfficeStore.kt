package backoffice.store

import java.util.concurrent.ConcurrentHashMap

class InMemoryBackOfficeStore {
    private val orderMetaById = ConcurrentHashMap<String, OrderMeta>()
    private val positions = ConcurrentHashMap<String, MutablePosition>()

    fun upsertOrderMeta(meta: OrderMeta) {
        orderMetaById[meta.orderId] = meta
    }

    fun findOrderMeta(orderId: String): OrderMeta? = orderMetaById[orderId]

    fun applyFill(accountId: String, symbol: String, signedQtyDelta: Long, price: Double?) {
        val key = "$accountId::$symbol"
        positions.compute(key) { _, existing ->
            val cur = existing ?: MutablePosition(accountId, symbol, 0, null)
            cur.applyDelta(signedQtyDelta, price)
            cur
        }
    }

    fun listPositions(accountId: String? = null): List<Position> {
        val items = positions.values.toList()
        val filtered = if (accountId == null) items else items.filter { it.accountId == accountId }
        return filtered
            .sortedWith(compareBy<MutablePosition> { it.accountId }.thenBy { it.symbol })
            .map { it.toPosition() }
    }

    private class MutablePosition(
        val accountId: String,
        val symbol: String,
        var netQty: Long,
        var avgPrice: Double?
    ) {
        fun applyDelta(signedQtyDelta: Long, price: Double?) {
            if (signedQtyDelta == 0L) return

            val newQty = netQty + signedQtyDelta

            // Simplified average price model:
            // - When increasing absolute exposure in the same direction, update WAP.
            // - When reducing/closing/reversing, keep avgPrice as-is (or reset to last price when crossing zero).
            val sameDirection = netQty == 0L || (netQty > 0 && signedQtyDelta > 0) || (netQty < 0 && signedQtyDelta < 0)
            if (price != null && sameDirection) {
                val oldAbs = kotlin.math.abs(netQty).toDouble()
                val addAbs = kotlin.math.abs(signedQtyDelta).toDouble()
                val oldAvg = avgPrice
                avgPrice =
                    if (oldAvg == null) {
                        price
                    } else {
                        ((oldAvg * oldAbs) + (price * addAbs)) / (oldAbs + addAbs)
                    }
            } else if (price != null && netQty != 0L && newQty == 0L) {
                // fully closed
                avgPrice = null
            } else if (price != null && netQty != 0L && (netQty > 0 && newQty < 0 || netQty < 0 && newQty > 0)) {
                // reversed
                avgPrice = price
            }

            netQty = newQty
        }

        fun toPosition(): Position = Position(accountId = accountId, symbol = symbol, netQty = netQty, avgPrice = avgPrice)
    }
}

