package app.fast.handlers

import app.events.IndicatorEvent
import app.events.SignalEvent
import com.lmax.disruptor.EventHandler

class StrategySignalHandler : EventHandler<RingBufferEvent> {

    override fun onEvent(event: RingBufferEvent, sequence: Long, endOfBatch: Boolean) {
        if (event.eventType != "Indicator") return

        val indicator = event.payload as? IndicatorEvent ?: return

        val signal = when {
            indicator.sma20 > indicator.sma50 && indicator.rsi < 70 -> SignalEvent.Signal.BUY
            indicator.sma20 < indicator.sma50 && indicator.rsi > 30 -> SignalEvent.Signal.SELL
            else -> SignalEvent.Signal.HOLD
        }

        val confidence = when {
            signal == SignalEvent.Signal.BUY && indicator.rsi < 30 -> 0.9
            signal == SignalEvent.Signal.SELL && indicator.rsi > 70 -> 0.9
            signal == SignalEvent.Signal.BUY -> 0.6
            signal == SignalEvent.Signal.SELL -> 0.6
            else -> 0.0
        }

        val signalEvent = SignalEvent(
            symbol = indicator.symbol,
            timestamp = indicator.timestamp,
            signal = signal,
            confidence = confidence,
            strategyName = "SMA_RSI_Cross"
        )

        event.eventType = "Signal"
        event.payload = signalEvent
        event.timestamp = System.nanoTime()
    }
}
