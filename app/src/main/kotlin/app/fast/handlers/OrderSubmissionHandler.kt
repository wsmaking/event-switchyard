package app.fast.handlers

import app.events.OrderEvent
import app.order.OrderExecutionService
import com.lmax.disruptor.EventHandler

// Fast Pathのイベントを「注文を送る処理」に渡すための橋渡し。
// 「注文を決める」処理と「注文を送る」処理を切り分け、将来の検証/再送/計測をここに集約できる。
class OrderSubmissionHandler(
    private val executionService: OrderExecutionService
) : EventHandler<RingBufferEvent> {
    override fun onEvent(event: RingBufferEvent, sequence: Long, endOfBatch: Boolean) {
        if (event.eventType != "Order") return
        val order = event.payload as? OrderEvent ?: return

        val result = executionService.submitOrder(order)
        event.eventType = if (result.accepted) "OrderSubmitted" else "OrderRejected"
    }
}
