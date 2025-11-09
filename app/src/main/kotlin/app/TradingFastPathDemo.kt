package app

import app.events.MarketDataEvent
import app.fast.TradingFastPath
import app.marketdata.MockMarketDataProvider
import kotlin.concurrent.thread

fun main() {
    println("=== Trading FastPath Integration Demo ===\n")

    val fastPath = TradingFastPath()
    val provider = MockMarketDataProvider()

    println("Starting market data feed...")
    println("Processing: MarketData -> Indicators -> Strategy -> Risk -> Order\n")

    val symbols = listOf("AAPL", "GOOGL", "MSFT")
    var eventCount = 0

    val feeder = thread {
        repeat(100) {
            symbols.forEach { symbol ->
                val quote = provider.getQuote(symbol)
                if (quote != null) {
                    val marketData = MarketDataEvent(
                        symbol = quote.symbol,
                        timestamp = quote.timestamp,
                        price = quote.price,
                        volume = quote.volume ?: 0L,
                        bid = quote.bid ?: quote.price - 0.01,
                        ask = quote.ask ?: quote.price + 0.01
                    )
                    fastPath.publishMarketData(marketData)
                    eventCount++
                }
            }
            Thread.sleep(100)
        }
    }

    feeder.join()
    Thread.sleep(2000)

    println("Processed $eventCount market data events")
    println("Events flowed through LMAX Disruptor handlers:")
    println("  1. TechnicalIndicatorHandler (SMA, RSI, MACD, Bollinger)")
    println("  2. StrategySignalHandler (BUY/SELL/HOLD signals)")
    println("  3. RiskManagementHandler (Position limits, Order generation)")

    fastPath.close()
    println("\n=== Demo Complete ===")
}
