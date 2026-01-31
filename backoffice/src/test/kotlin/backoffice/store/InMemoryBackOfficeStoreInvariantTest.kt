package backoffice.store

import backoffice.model.Side
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Instant

class InMemoryBackOfficeStoreInvariantTest {
    @Test
    fun `filled total is monotonic`() {
        val store = InMemoryBackOfficeStore()
        val now = Instant.now()

        store.applyFill(
            at = now,
            accountId = "acct-1",
            orderId = "ord-1",
            symbol = "BTC",
            side = Side.BUY,
            filledQtyDelta = 5,
            filledQtyTotal = 5,
            price = 100,
            quoteCcy = "USD",
            quoteCashDelta = -500,
            feeQuote = 0
        )

        store.applyFill(
            at = now,
            accountId = "acct-1",
            orderId = "ord-1",
            symbol = "BTC",
            side = Side.BUY,
            filledQtyDelta = 1,
            filledQtyTotal = 3,
            price = 100,
            quoteCcy = "USD",
            quoteCashDelta = -100,
            feeQuote = 0
        )

        assertEquals(5, store.lastFilledTotal("ord-1"))
    }

    @Test
    fun `duplicate fill does not double apply`() {
        val store = InMemoryBackOfficeStore()
        val now = Instant.now()

        store.applyFill(
            at = now,
            accountId = "acct-1",
            orderId = "ord-1",
            symbol = "BTC",
            side = Side.BUY,
            filledQtyDelta = 2,
            filledQtyTotal = 2,
            price = 100,
            quoteCcy = "USD",
            quoteCashDelta = -200,
            feeQuote = 0
        )

        store.applyFill(
            at = now,
            accountId = "acct-1",
            orderId = "ord-1",
            symbol = "BTC",
            side = Side.BUY,
            filledQtyDelta = 2,
            filledQtyTotal = 2,
            price = 100,
            quoteCcy = "USD",
            quoteCashDelta = -200,
            feeQuote = 0
        )

        val balances = store.listBalances("acct-1")
        val usd = balances.first { it.currency == "USD" }
        assertEquals(-200, usd.amount)
        assertTrue(store.listFills("acct-1").size == 1)
    }
}
