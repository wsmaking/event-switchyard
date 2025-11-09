package app.risk

data class RiskProfile(
    val maxPositionSize: Double,
    val maxPortfolioRisk: Double,
    val stopLossPercent: Double,
    val takeProfitPercent: Double
)

data class Position(
    val symbol: String,
    val quantity: Int,
    val entryPrice: Double,
    val currentPrice: Double
) {
    val unrealizedPnL: Double
        get() = (currentPrice - entryPrice) * quantity

    val unrealizedPnLPercent: Double
        get() = ((currentPrice - entryPrice) / entryPrice) * 100
}

class RiskManager(private val profile: RiskProfile) {

    fun calculatePositionSize(accountBalance: Double, price: Double): Int {
        val maxInvestment = accountBalance * profile.maxPositionSize
        return (maxInvestment / price).toInt()
    }

    fun shouldStopLoss(position: Position): Boolean {
        return position.unrealizedPnLPercent <= -profile.stopLossPercent
    }

    fun shouldTakeProfit(position: Position): Boolean {
        return position.unrealizedPnLPercent >= profile.takeProfitPercent
    }

    fun calculateRisk(positions: List<Position>, accountBalance: Double): Double {
        val totalExposure = positions.sumOf { it.entryPrice * it.quantity }
        return (totalExposure / accountBalance) * 100
    }

    fun canOpenPosition(positions: List<Position>, accountBalance: Double): Boolean {
        val currentRisk = calculateRisk(positions, accountBalance)
        return currentRisk < profile.maxPortfolioRisk
    }
}
