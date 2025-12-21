package backoffice.store

import backoffice.model.Side
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

class InMemoryBackOfficeStore(
    private val maxFills: Int = (System.getenv("BACKOFFICE_MAX_FILLS") ?: "10000").toInt()
) {
    private val orderMetaById = ConcurrentHashMap<String, OrderMeta>()
    private val lastFilledTotalByOrderId = ConcurrentHashMap<String, Long>()

    private val positions = ConcurrentHashMap<String, MutablePosition>()
    private val balances = ConcurrentHashMap<String, Long>() // accountId::CCY -> amount
    private val realizedPnl = ConcurrentHashMap<String, Long>() // accountId::symbol::CCY -> pnl
    private val fillsByAccount = ConcurrentHashMap<String, ArrayDeque<FillRecord>>()

    fun upsertOrderMeta(meta: OrderMeta) {
        orderMetaById[meta.orderId] = meta
    }

    fun findOrderMeta(orderId: String): OrderMeta? = orderMetaById[orderId]

    fun lastFilledTotal(orderId: String): Long = lastFilledTotalByOrderId[orderId] ?: 0L

    fun setLastFilledTotal(orderId: String, filledQtyTotal: Long) {
        lastFilledTotalByOrderId[orderId] = filledQtyTotal
    }

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
    ) {
        if (filledQtyDelta <= 0L) return
        val lastTotal = lastFilledTotal(orderId)
        if (filledQtyTotal <= lastTotal) return

        val signedQtyDelta = if (side == Side.BUY) filledQtyDelta else -filledQtyDelta
        val posKey = "$accountId::$symbol"
        val realizedDelta =
            positions.compute(posKey) { _, existing ->
                val cur = existing ?: MutablePosition(accountId, symbol, 0, null)
                val realized = cur.applyDeltaWithRealized(signedQtyDelta, price?.toDouble())
                cur.lastRealizedDelta = realized
                cur
            }?.lastRealizedDelta ?: 0L

        if (realizedDelta != 0L) {
            realizedPnl.merge(pnlKey(accountId, symbol, quoteCcy), realizedDelta) { a, b -> a + b }
        }

        balances.merge(balanceKey(accountId, quoteCcy), quoteCashDelta) { a, b -> a + b }
        setLastFilledTotal(orderId, filledQtyTotal)

        val q = fillsByAccount.computeIfAbsent(accountId) { ArrayDeque(maxFills) }
        synchronized(q) {
            q.addLast(
                FillRecord(
                    at = at,
                    accountId = accountId,
                    orderId = orderId,
                    symbol = symbol,
                    side = side,
                    filledQtyDelta = filledQtyDelta,
                    filledQtyTotal = filledQtyTotal,
                    price = price,
                    quoteCcy = quoteCcy,
                    quoteCashDelta = quoteCashDelta,
                    feeQuote = feeQuote
                )
            )
            while (q.size > maxFills) q.removeFirst()
        }
    }

    fun listPositions(accountId: String? = null): List<Position> {
        val items = positions.values.toList()
        val filtered = if (accountId == null) items else items.filter { it.accountId == accountId }
        return filtered
            .sortedWith(compareBy<MutablePosition> { it.accountId }.thenBy { it.symbol })
            .map { it.toPosition() }
    }

    fun listBalances(accountId: String? = null): List<Balance> {
        val entries = balances.entries.toList()
        val filtered =
            if (accountId == null) {
                entries
            } else {
                entries.filter { it.key.startsWith("$accountId::") }
            }
        return filtered
            .mapNotNull { (k, v) ->
                val parts = k.split("::")
                if (parts.size != 2) return@mapNotNull null
                Balance(accountId = parts[0], currency = parts[1], amount = v)
            }
            .sortedWith(compareBy<Balance> { it.accountId }.thenBy { it.currency })
    }

    fun listFills(accountId: String): List<FillRecord> {
        val q = fillsByAccount[accountId] ?: return emptyList()
        synchronized(q) {
            return q.toList()
        }
    }

    fun listRealizedPnl(accountId: String? = null): List<RealizedPnl> {
        val entries = realizedPnl.entries.toList()
        val filtered =
            if (accountId == null) {
                entries
            } else {
                entries.filter { it.key.startsWith("$accountId::") }
            }
        return filtered
            .mapNotNull { (k, v) ->
                val parts = k.split("::")
                if (parts.size != 3) return@mapNotNull null
                RealizedPnl(accountId = parts[0], symbol = parts[1], quoteCcy = parts[2], realizedPnl = v)
            }
            .sortedWith(compareBy<RealizedPnl> { it.accountId }.thenBy { it.symbol }.thenBy { it.quoteCcy })
    }

    fun snapshotState(): BackOfficeSnapshotState {
        val fills =
            fillsByAccount.mapValues { (_, q) ->
                synchronized(q) { q.toList() }
            }
        return BackOfficeSnapshotState(
            orderMeta = orderMetaById.values.toList(),
            lastFilledTotals = HashMap(lastFilledTotalByOrderId),
            positions = positions.values.map { it.toPosition() },
            balances = listBalances(),
            realizedPnl = listRealizedPnl(),
            fillsByAccount = fills
        )
    }

    fun restoreState(state: BackOfficeSnapshotState) {
        orderMetaById.clear()
        lastFilledTotalByOrderId.clear()
        positions.clear()
        balances.clear()
        realizedPnl.clear()
        fillsByAccount.clear()

        state.orderMeta.forEach { meta -> orderMetaById[meta.orderId] = meta }
        state.lastFilledTotals.forEach { (k, v) -> lastFilledTotalByOrderId[k] = v }
        state.positions.forEach { pos ->
            positions["${pos.accountId}::${pos.symbol}"] = MutablePosition(pos.accountId, pos.symbol, pos.netQty, pos.avgPrice)
        }
        state.balances.forEach { bal ->
            balances["${bal.accountId}::${bal.currency}"] = bal.amount
        }
        state.realizedPnl.forEach { pnl ->
            realizedPnl["${pnl.accountId}::${pnl.symbol}::${pnl.quoteCcy}"] = pnl.realizedPnl
        }
        state.fillsByAccount.forEach { (accountId, fills) ->
            val q = ArrayDeque<FillRecord>(maxFills)
            val tail = if (fills.size > maxFills) fills.takeLast(maxFills) else fills
            tail.forEach { q.addLast(it) }
            fillsByAccount[accountId] = q
        }
    }

    fun snapshotBalances(accountId: String): Map<String, Long> {
        return balances
            .filterKeys { it.startsWith("$accountId::") }
            .mapKeys { it.key.substringAfter("::") }
    }

    fun snapshotPositions(accountId: String): Map<String, Long> {
        return positions
            .filterValues { it.accountId == accountId }
            .mapValues { it.value.netQty }
    }

    private fun balanceKey(accountId: String, ccy: String): String = "$accountId::$ccy"

    private fun pnlKey(accountId: String, symbol: String, ccy: String): String = "$accountId::$symbol::$ccy"

    private class MutablePosition(
        val accountId: String,
        val symbol: String,
        var netQty: Long,
        var avgPrice: Double?
    ) {
        @Volatile var lastRealizedDelta: Long = 0

        fun applyDeltaWithRealized(signedQtyDelta: Long, price: Double?): Long {
            if (signedQtyDelta == 0L) return 0L
            val px = price ?: return applyDeltaNoPrice(signedQtyDelta)

            val beforeQty = netQty
            val beforeAvg = avgPrice

            val realizedQuote = realizedFromClose(beforeQty, signedQtyDelta, beforeAvg, px)

            val newQty = beforeQty + signedQtyDelta
            val sameDirection =
                beforeQty == 0L || (beforeQty > 0 && signedQtyDelta > 0) || (beforeQty < 0 && signedQtyDelta < 0)
            if (sameDirection) {
                val oldAbs = kotlin.math.abs(beforeQty).toDouble()
                val addAbs = kotlin.math.abs(signedQtyDelta).toDouble()
                val oldAvg = beforeAvg
                avgPrice =
                    if (oldAvg == null) {
                        px
                    } else {
                        ((oldAvg * oldAbs) + (px * addAbs)) / (oldAbs + addAbs)
                    }
            } else if (beforeQty != 0L && newQty == 0L) {
                avgPrice = null
            } else if (beforeQty != 0L && (beforeQty > 0 && newQty < 0 || beforeQty < 0 && newQty > 0)) {
                avgPrice = px
            }

            netQty = newQty
            return realizedQuote
        }

        private fun applyDeltaNoPrice(signedQtyDelta: Long): Long {
            netQty += signedQtyDelta
            return 0L
        }

        private fun realizedFromClose(beforeQty: Long, signedQtyDelta: Long, beforeAvg: Double?, px: Double): Long {
            if (beforeAvg == null) return 0L

            // Closing long with SELL (negative delta)
            if (beforeQty > 0 && signedQtyDelta < 0) {
                val closeQty = minOf(beforeQty, kotlin.math.abs(signedQtyDelta))
                val pnl = (px - beforeAvg) * closeQty.toDouble()
                return pnl.toLong()
            }
            // Closing short with BUY (positive delta)
            if (beforeQty < 0 && signedQtyDelta > 0) {
                val closeQty = minOf(kotlin.math.abs(beforeQty), signedQtyDelta)
                val pnl = (beforeAvg - px) * closeQty.toDouble()
                return pnl.toLong()
            }
            return 0L
        }

        fun toPosition(): Position = Position(accountId = accountId, symbol = symbol, netQty = netQty, avgPrice = avgPrice)
    }
}
