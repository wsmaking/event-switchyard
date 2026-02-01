package gateway.order

import gateway.exchange.ExecutionReport
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Instant

class ExecutionReportOrderingInvariantTest {
    @Test
    fun `reject does not override existing fills`() {
        val store = InMemoryOrderStore()
        val order = baseSnapshot(status = OrderStatus.PARTIALLY_FILLED, filledQty = 5)
        store.put(order, idempotencyKey = null)

        store.applyExecutionReport(
            ExecutionReport(
                orderId = order.orderId,
                status = OrderStatus.REJECTED,
                filledQtyDelta = 0,
                filledQtyTotal = 0,
                price = order.price,
                at = Instant.now()
            )
        )

        val updated = store.findById(order.orderId)!!
        assertEquals(OrderStatus.PARTIALLY_FILLED, updated.status)
        assertEquals(5, updated.filledQty)
    }

    @Test
    fun `terminal cancel ignores late fill`() {
        val store = InMemoryOrderStore()
        val order = baseSnapshot(status = OrderStatus.CANCELED, filledQty = 2)
        store.put(order, idempotencyKey = null)

        store.applyExecutionReport(
            ExecutionReport(
                orderId = order.orderId,
                status = OrderStatus.FILLED,
                filledQtyDelta = 8,
                filledQtyTotal = 10,
                price = order.price,
                at = Instant.now()
            )
        )

        val updated = store.findById(order.orderId)!!
        assertEquals(OrderStatus.CANCELED, updated.status)
        assertEquals(2, updated.filledQty)
    }

    @Test
    fun `filled quantity total is monotonic`() {
        val store = InMemoryOrderStore()
        val order = baseSnapshot(status = OrderStatus.PARTIALLY_FILLED, filledQty = 5)
        store.put(order, idempotencyKey = null)

        store.applyExecutionReport(
            ExecutionReport(
                orderId = order.orderId,
                status = OrderStatus.PARTIALLY_FILLED,
                filledQtyDelta = 0,
                filledQtyTotal = 3,
                price = order.price,
                at = Instant.now()
            )
        )

        val updated = store.findById(order.orderId)!!
        assertEquals(5, updated.filledQty)
    }

    private fun baseSnapshot(status: OrderStatus, filledQty: Long): OrderSnapshot {
        val now = Instant.now()
        return OrderSnapshot(
            orderId = "ord-1",
            accountId = "acct-1",
            clientOrderId = "c-1",
            symbol = "BTC",
            side = OrderSide.BUY,
            type = OrderType.LIMIT,
            qty = 10,
            price = 100L,
            timeInForce = TimeInForce.GTC,
            expireAt = null,
            status = status,
            acceptedAt = now,
            lastUpdateAt = now,
            filledQty = filledQty
        )
    }
}
