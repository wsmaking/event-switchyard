package gateway.order

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import java.time.Instant

class IdempotencyScopeInvariantTest {
    private fun snapshot(orderId: String, accountId: String): OrderSnapshot {
        val now = Instant.now()
        return OrderSnapshot(
            orderId = orderId,
            accountId = accountId,
            clientOrderId = null,
            symbol = "BTC",
            side = OrderSide.BUY,
            type = OrderType.LIMIT,
            qty = 1,
            price = 100,
            timeInForce = TimeInForce.GTC,
            expireAt = null,
            status = OrderStatus.ACCEPTED,
            acceptedAt = now,
            lastUpdateAt = now,
            filledQty = 0
        )
    }

    @Test
    fun `idempotency keys are scoped per account`() {
        val store = InMemoryOrderStore()
        store.put(snapshot("ord-a", "acct-a"), "dup-key")
        store.put(snapshot("ord-b", "acct-b"), "dup-key")

        val a = store.findByIdempotencyKey("acct-a", "dup-key")
        val b = store.findByIdempotencyKey("acct-b", "dup-key")

        assertNotNull(a)
        assertNotNull(b)
        assertEquals("ord-a", a!!.orderId)
        assertEquals("ord-b", b!!.orderId)
    }
}
