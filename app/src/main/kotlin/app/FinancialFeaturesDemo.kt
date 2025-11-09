package app

import app.marketdata.MockMarketDataProvider
import app.indicators.TechnicalIndicators
import app.strategy.SimpleMovingAverageCrossover
import app.strategy.RSIStrategy
import app.risk.RiskManager
import app.risk.RiskProfile
import app.backtest.BacktestEngine

fun main() {
    println("=== Financial Features Demo ===\n")

    val provider = MockMarketDataProvider()
    val symbol = "AAPL"

    println("1. Market Data Provider")
    val quote = provider.getQuote(symbol)
    println("  Quote for $symbol: price=${quote?.price}, volume=${quote?.volume}")

    val historicalPrices = provider.getHistoricalPrices(symbol, 60)
    println("  Historical prices: ${historicalPrices.size} days\n")

    println("2. Technical Indicators")
    val prices = historicalPrices.map { it.price }
    val sma = TechnicalIndicators.sma(prices, 20)
    val rsi = TechnicalIndicators.rsi(prices, 14)
    val (macdLine, signalLine, histogram) = TechnicalIndicators.macd(prices)
    val (middle, upper, lower) = TechnicalIndicators.bollingerBands(prices, 20)

    println("  SMA(20): ${sma.lastOrNull()?.value}")
    println("  RSI(14): ${rsi.lastOrNull()?.value}")
    println("  MACD: ${macdLine.lastOrNull()?.value}")
    println("  Bollinger Bands: upper=${upper.lastOrNull()?.value}, lower=${lower.lastOrNull()?.value}\n")

    println("3. Trading Strategies")
    val smaStrategy = SimpleMovingAverageCrossover(10, 30)
    val rsiStrategy = RSIStrategy(14)

    val smaSignal = smaStrategy.analyze(symbol, prices)
    val rsiSignal = rsiStrategy.analyze(symbol, prices)

    println("  ${smaStrategy.getName()}: ${smaSignal.action} (strength=${String.format("%.2f", smaSignal.strength)})")
    println("  ${rsiStrategy.getName()}: ${rsiSignal.action} (strength=${String.format("%.2f", rsiSignal.strength)})\n")

    println("4. Risk Management")
    val riskProfile = RiskProfile(
        maxPositionSize = 0.1,
        maxPortfolioRisk = 50.0,
        stopLossPercent = 5.0,
        takeProfitPercent = 10.0
    )
    val riskManager = RiskManager(riskProfile)
    val accountBalance = 100000.0
    val positionSize = riskManager.calculatePositionSize(accountBalance, quote?.price ?: 0.0)

    println("  Account Balance: $$accountBalance")
    println("  Position Size for $symbol: $positionSize shares\n")

    println("5. Backtest Engine")
    val backtestEngine = BacktestEngine(smaStrategy, riskManager, accountBalance)
    val historicalData = historicalPrices.map { it.timestamp to it.price }
    val result = backtestEngine.run(symbol, historicalData)

    println("  Strategy: ${smaStrategy.getName()}")
    println("  Initial Balance: $${String.format("%.2f", result.initialBalance)}")
    println("  Final Balance: $${String.format("%.2f", result.finalBalance)}")
    println("  Total Return: ${String.format("%.2f", result.totalReturn)}%")
    println("  Total Trades: ${result.totalTrades}")
    println("  Win Rate: ${String.format("%.2f", result.winRate)}%")
    println("  Max Drawdown: ${String.format("%.2f", result.maxDrawdown)}%")
    println("  Sharpe Ratio: ${String.format("%.2f", result.sharpeRatio)}")

    println("\n=== Demo Complete ===")
}
