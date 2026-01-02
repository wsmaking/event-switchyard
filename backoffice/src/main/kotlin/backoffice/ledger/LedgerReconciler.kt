package backoffice.ledger

import backoffice.model.Side
import backoffice.store.BackOfficeStore
import java.util.SortedMap
import java.util.TreeMap

class LedgerReconciler(
    private val ledger: FileLedger,
    private val store: BackOfficeStore
) {
    data class ReconResult(
        val accountId: String,
        val balances: ReconSection,
        val positions: ReconSection
    )

    data class ReconSection(
        val ok: Boolean,
        val expected: SortedMap<String, Long>,
        val actual: SortedMap<String, Long>,
        val diffs: SortedMap<String, Long>
    )

    fun reconcile(accountId: String, limit: Int, quoteCcy: String): ReconResult {
        val expectedBalances = TreeMap<String, Long>()
        val expectedPositions = TreeMap<String, Long>()

        val entries = ledger.readEntries(accountId, orderId = null, limit = limit, since = null, after = null, types = null)
        for (entry in entries) {
            when (entry) {
                is LedgerFill -> {
                    val signedQty = if (entry.side == Side.BUY) entry.filledQtyDelta else -entry.filledQtyDelta
                    expectedPositions.merge(entry.symbol, signedQty) { a, b -> a + b }
                    expectedBalances.merge(entry.quoteCcy, entry.quoteCashDelta) { a, b -> a + b }
                }
                else -> {
                    // OrderAccepted does not affect balances/positions
                }
            }
        }

        if (!expectedBalances.containsKey(quoteCcy)) {
            expectedBalances[quoteCcy] = 0L
        }

        val actualBalances = TreeMap(store.snapshotBalances(accountId))
        val actualPositions = TreeMap(store.snapshotPositions(accountId))

        val balanceDiffs = diffMap(expectedBalances, actualBalances)
        val positionDiffs = diffMap(expectedPositions, actualPositions)

        return ReconResult(
            accountId = accountId,
            balances = ReconSection(balanceDiffs.all { it.value == 0L }, expectedBalances, actualBalances, balanceDiffs),
            positions = ReconSection(positionDiffs.all { it.value == 0L }, expectedPositions, actualPositions, positionDiffs)
        )
    }

    private fun diffMap(expected: SortedMap<String, Long>, actual: SortedMap<String, Long>): SortedMap<String, Long> {
        val keys = TreeMap<String, Long>()
        keys.putAll(expected)
        for (key in actual.keys) {
            keys.putIfAbsent(key, 0L)
        }
        val diffs = TreeMap<String, Long>()
        for (key in keys.keys) {
            val exp = expected[key] ?: 0L
            val act = actual[key] ?: 0L
            diffs[key] = act - exp
        }
        return diffs
    }
}
