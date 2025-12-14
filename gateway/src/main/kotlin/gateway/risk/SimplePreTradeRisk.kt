package gateway.risk

import gateway.order.CreateOrderRequest

class SimplePreTradeRisk(
    private val maxQty: Long = (System.getenv("RISK_MAX_QTY") ?: "100000").toLong()
) : PreTradeRisk {
    override fun validate(order: CreateOrderRequest): RiskResult {
        if (order.qty > maxQty) return RiskResult(false, "QTY_LIMIT_EXCEEDED")
        return RiskResult(true)
    }
}

