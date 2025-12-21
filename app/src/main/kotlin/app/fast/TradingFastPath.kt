package app.fast

import app.events.MarketDataEvent
import app.fast.handlers.RingBufferEvent
import app.fast.handlers.TechnicalIndicatorHandler
import app.fast.handlers.StrategySignalHandler
import app.fast.handlers.RiskManagementHandler
import app.fast.handlers.OrderSubmissionHandler
import com.lmax.disruptor.dsl.Disruptor
import com.lmax.disruptor.dsl.ProducerType
import com.lmax.disruptor.util.DaemonThreadFactory
import com.lmax.disruptor.YieldingWaitStrategy

class TradingFastPath(
    private val orderSubmissionHandler: OrderSubmissionHandler? = null
) : AutoCloseable {
    private val bufferSize = 65536
    private val disruptor: Disruptor<RingBufferEvent>

    init {
        disruptor = Disruptor(
            { RingBufferEvent() },
            bufferSize,
            DaemonThreadFactory.INSTANCE,
            ProducerType.MULTI,
            YieldingWaitStrategy()
        )

        val pipeline =
            disruptor.handleEventsWith(TechnicalIndicatorHandler())
                .then(StrategySignalHandler())
                .then(RiskManagementHandler())
        if (orderSubmissionHandler != null) {
            pipeline.then(orderSubmissionHandler)
        }

        disruptor.start()
    }

    fun publishMarketData(marketData: MarketDataEvent) {
        val ringBuffer = disruptor.ringBuffer
        val sequence = ringBuffer.next()
        try {
            val event = ringBuffer.get(sequence)
            event.eventType = "MarketData"
            event.payload = marketData
            event.timestamp = System.nanoTime()
        } finally {
            ringBuffer.publish(sequence)
        }
    }

    override fun close() {
        disruptor.shutdown()
    }
}
