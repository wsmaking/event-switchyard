package app.fast.handlers

import app.events.SignalEvent
import app.events.OrderEvent
import com.lmax.disruptor.EventHandler
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

class RiskManagementHandler : EventHandler<RingBufferEvent> {
    private val positions = ConcurrentHashMap<String, Int>()
    private val maxPositionSize = 100
    private val accountBalance = 100000.0

    override fun onEvent(event: RingBufferEvent, sequence: Long, endOfBatch: Boolean) {
        if (event.eventType != "Signal") return

        val signal = event.payload as? SignalEvent ?: return

        if (signal.signal == SignalEvent.Signal.HOLD || signal.confidence < 0.5) {
            event.eventType = "Filtered"
            return
        }

        val currentPosition = positions.getOrDefault(signal.symbol, 0)

        val canTrade = when (signal.signal) {
            SignalEvent.Signal.BUY -> currentPosition < maxPositionSize
            SignalEvent.Signal.SELL -> currentPosition > -maxPositionSize
            else -> false
        }

        if (!canTrade) {
            event.eventType = "Filtered"
            return
        }

        val quantity = (maxPositionSize * 0.1).toInt().coerceAtLeast(1)

        val orderEvent = OrderEvent(
            orderId = UUID.randomUUID().toString(),
            symbol = signal.symbol,
            timestamp = signal.timestamp,
            side = when (signal.signal) {
                SignalEvent.Signal.BUY -> OrderEvent.Side.BUY
                SignalEvent.Signal.SELL -> OrderEvent.Side.SELL
                else -> return
            },
            price = 0.0,
            quantity = quantity,
            orderType = OrderEvent.OrderType.MARKET,
            timeInForce = OrderEvent.TimeInForce.GTC
        )

        when (signal.signal) {
            SignalEvent.Signal.BUY -> positions[signal.symbol] = currentPosition + quantity
            SignalEvent.Signal.SELL -> positions[signal.symbol] = currentPosition - quantity
            else -> {}
        }

        event.eventType = "Order"
        event.payload = orderEvent
        event.timestamp = System.nanoTime()
    }
}
