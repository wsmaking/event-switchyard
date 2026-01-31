package gateway.order

import gateway.exchange.ExecutionReport
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import java.time.Instant

class InMemoryOrderStoreInvariantTest {
    private fun snapshot(
        orderId: String,
        status: OrderStatus = OrderStatus.ACCEPTED,
        filledQty: Long = 0
    ): OrderSnapshot {
        val now = Instant.now()
        return OrderSnapshot(
            orderId = orderId,
            accountId = "acct-1",
            clientOrderId = null,
            symbol = "BTC",
            side = OrderSide.BUY,
            type = OrderType.LIMIT,
            qty = 10,
            price = 100,
            timeInForce = TimeInForce.GTC,
            expireAt = null,
            status = status,
            acceptedAt = now,
            lastUpdateAt = now,
            filledQty = filledQty
        )
    }

    @Test
    fun `idempotency key maps to first order`() {
        val store = InMemoryOrderStore()
        val first = snapshot("ord-1")
        val second = snapshot("ord-2")

        store.put(first, "key-1")
        store.put(second, "key-1")

        val resolved = store.findByIdempotencyKey("acct-1", "key-1")
        assertNotNull(resolved)
        assertEquals("ord-1", resolved!!.orderId)
    }

    @Test
    fun `filled qty never decreases on out-of-order reports`() {
        val store = InMemoryOrderStore()
        store.put(snapshot("ord-1"), null)

        store.applyExecutionReport(
            ExecutionReport(
                orderId = "ord-1",
                status = OrderStatus.PARTIALLY_FILLED,
                filledQtyDelta = 5,
                filledQtyTotal = 5,
                price = 100,
                at = Instant.now()
            )
        )

        val outOfOrder = store.applyExecutionReport(
            ExecutionReport(
                orderId = "ord-1",
                status = OrderStatus.PARTIALLY_FILLED,
                filledQtyDelta = -2,
                filledQtyTotal = 3,
                price = 100,
                at = Instant.now()
            )
        )

        assertEquals(5, outOfOrder!!.filledQty)
    }

    @Test
    fun `reject does not downgrade after fills`() {
        val store = InMemoryOrderStore()
        store.put(snapshot("ord-1", status = OrderStatus.PARTIALLY_FILLED, filledQty = 5), null)

        val result = store.applyExecutionReport(
            ExecutionReport(
                orderId = "ord-1",
                status = OrderStatus.REJECTED,
                filledQtyDelta = 0,
                filledQtyTotal = 5,
                price = 100,
                at = Instant.now()
            )
        )

        assertEquals(OrderStatus.PARTIALLY_FILLED, result!!.status)
    }
}
