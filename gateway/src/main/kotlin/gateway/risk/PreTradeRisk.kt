package gateway.risk

import gateway.order.CreateOrderRequest

data class RiskResult(val ok: Boolean, val reason: String? = null)

interface PreTradeRisk {
    fun validate(order: CreateOrderRequest): RiskResult
}

