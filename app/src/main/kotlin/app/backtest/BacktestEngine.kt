package app.backtest

import app.strategy.Signal
import app.strategy.TradingStrategy
import app.risk.RiskManager
import app.risk.Position

data class Trade(
    val symbol: String,
    val action: Signal.Action,
    val quantity: Int,
    val price: Double,
    val timestamp: Long
)

data class BacktestResult(
    val initialBalance: Double,
    val finalBalance: Double,
    val totalTrades: Int,
    val winningTrades: Int,
    val losingTrades: Int,
    val maxDrawdown: Double,
    val sharpeRatio: Double,
    val trades: List<Trade>
) {
    val totalReturn: Double get() = ((finalBalance - initialBalance) / initialBalance) * 100
    val winRate: Double get() = if (totalTrades > 0) (winningTrades.toDouble() / totalTrades) * 100 else 0.0
}

class BacktestEngine(
    private val strategy: TradingStrategy,
    private val riskManager: RiskManager,
    private val initialBalance: Double = 100000.0
) {

    fun run(symbol: String, historicalPrices: List<Pair<Long, Double>>): BacktestResult {
        var balance = initialBalance
        val positions = mutableListOf<Position>()
        val trades = mutableListOf<Trade>()
        val balanceHistory = mutableListOf<Double>()

        for (i in 30 until historicalPrices.size) {
            val currentPrice = historicalPrices[i].second
            val priceHistory = historicalPrices.subList(0, i).map { it.second }

            positions.forEach { pos ->
                val updatedPos = pos.copy(currentPrice = currentPrice)

                if (riskManager.shouldStopLoss(updatedPos) || riskManager.shouldTakeProfit(updatedPos)) {
                    balance += updatedPos.quantity * currentPrice
                    trades.add(Trade(symbol, Signal.Action.SELL, updatedPos.quantity, currentPrice, historicalPrices[i].first))
                    positions.remove(pos)
                }
            }

            val signal = strategy.analyze(symbol, priceHistory)

            when (signal.action) {
                Signal.Action.BUY -> {
                    if (riskManager.canOpenPosition(positions, balance)) {
                        val quantity = riskManager.calculatePositionSize(balance, currentPrice)
                        if (quantity > 0 && balance >= quantity * currentPrice) {
                            balance -= quantity * currentPrice
                            positions.add(Position(symbol, quantity, currentPrice, currentPrice))
                            trades.add(Trade(symbol, Signal.Action.BUY, quantity, currentPrice, historicalPrices[i].first))
                        }
                    }
                }
                Signal.Action.SELL -> {
                    if (positions.isNotEmpty()) {
                        val pos = positions.removeAt(0)
                        balance += pos.quantity * currentPrice
                        trades.add(Trade(symbol, Signal.Action.SELL, pos.quantity, currentPrice, historicalPrices[i].first))
                    }
                }
                Signal.Action.HOLD -> {}
            }

            balanceHistory.add(balance + positions.sumOf { it.quantity * it.currentPrice })
        }

        positions.forEach { pos ->
            balance += pos.quantity * pos.currentPrice
        }

        val winningTrades = trades.windowed(2, 2).count { (buy, sell) ->
            sell.price > buy.price
        }
        val losingTrades = trades.windowed(2, 2).count { (buy, sell) ->
            sell.price < buy.price
        }

        val maxDrawdown = calculateMaxDrawdown(balanceHistory)
        val sharpeRatio = calculateSharpeRatio(balanceHistory)

        return BacktestResult(
            initialBalance = initialBalance,
            finalBalance = balance,
            totalTrades = trades.size / 2,
            winningTrades = winningTrades,
            losingTrades = losingTrades,
            maxDrawdown = maxDrawdown,
            sharpeRatio = sharpeRatio,
            trades = trades
        )
    }

    private fun calculateMaxDrawdown(balanceHistory: List<Double>): Double {
        var maxDrawdown = 0.0
        var peak = balanceHistory.firstOrNull() ?: return 0.0

        balanceHistory.forEach { balance ->
            if (balance > peak) {
                peak = balance
            }
            val drawdown = ((peak - balance) / peak) * 100
            if (drawdown > maxDrawdown) {
                maxDrawdown = drawdown
            }
        }

        return maxDrawdown
    }

    private fun calculateSharpeRatio(balanceHistory: List<Double>): Double {
        if (balanceHistory.size < 2) return 0.0

        val returns = balanceHistory.zipWithNext { a, b -> (b - a) / a }
        val avgReturn = returns.average()
        val stdDev = kotlin.math.sqrt(returns.map { (it - avgReturn) * (it - avgReturn) }.average())

        return if (stdDev > 0) avgReturn / stdDev else 0.0
    }
}
