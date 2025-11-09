package app.http

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import java.nio.charset.StandardCharsets

data class Position(
    val symbol: String,
    val quantity: Int,
    val avgPrice: Double,
    val currentPrice: Double,
    val unrealizedPnL: Double,
    val unrealizedPnLPercent: Double
)

/**
 * 保有銘柄API
 * OrderControllerの注文履歴から計算
 */
class PositionController(
    private val orderController: OrderController,
    private val marketDataController: MarketDataController
) : HttpHandler {
    private val objectMapper = jacksonObjectMapper()

    override fun handle(exchange: HttpExchange) {
        try {
            // CORS headers
            exchange.responseHeaders.set("Access-Control-Allow-Origin", "*")
            exchange.responseHeaders.set("Access-Control-Allow-Methods", "GET, OPTIONS")
            exchange.responseHeaders.set("Access-Control-Allow-Headers", "Content-Type")

            if (exchange.requestMethod == "OPTIONS") {
                sendResponse(exchange, 204, "")
                return
            }

            if (exchange.requestMethod != "GET") {
                sendResponse(exchange, 405, "Method Not Allowed")
                return
            }

            val positions = calculatePositions()
            val json = objectMapper.writeValueAsString(positions)

            exchange.responseHeaders.set("Content-Type", "application/json")
            sendResponse(exchange, 200, json)
        } catch (e: Exception) {
            sendResponse(exchange, 500, "Internal Server Error: ${e.message}")
        } finally {
            exchange.close()
        }
    }

    private fun calculatePositions(): List<Position> {
        val orders = orderController.getAllOrders()

        // 銘柄ごとにグループ化して保有数量・平均単価を計算
        val positionMap = mutableMapOf<String, Pair<Int, Double>>() // symbol -> (quantity, totalCost)

        for (order in orders) {
            if (order.status != OrderStatus.FILLED) continue

            val symbol = order.symbol
            val (currentQty, currentCost) = positionMap.getOrDefault(symbol, 0 to 0.0)

            // 約定価格（注文時に記録された価格を使用）
            val executionPrice = order.price ?: marketDataController.getCurrentPrice(symbol)

            when (order.side) {
                OrderSide.BUY -> {
                    positionMap[symbol] = (currentQty + order.quantity) to (currentCost + executionPrice * order.quantity)
                }
                OrderSide.SELL -> {
                    positionMap[symbol] = (currentQty - order.quantity) to (currentCost - executionPrice * order.quantity)
                }
            }
        }

        // 数量が0以外のポジションのみ返す
        return positionMap
            .filter { (_, pair) -> pair.first > 0 }
            .map { (symbol, pair) ->
                val (qty, totalCost) = pair
                val avgPrice = if (qty > 0) totalCost / qty else 0.0
                val currentPrice = marketDataController.getCurrentPrice(symbol)
                val unrealizedPnL = (currentPrice - avgPrice) * qty
                val unrealizedPnLPercent = if (avgPrice > 0) (unrealizedPnL / totalCost) * 100 else 0.0

                Position(
                    symbol = symbol,
                    quantity = qty,
                    avgPrice = String.format("%.2f", avgPrice).toDouble(),
                    currentPrice = String.format("%.2f", currentPrice).toDouble(),
                    unrealizedPnL = String.format("%.2f", unrealizedPnL).toDouble(),
                    unrealizedPnLPercent = String.format("%.2f", unrealizedPnLPercent).toDouble()
                )
            }
    }

    private fun sendResponse(exchange: HttpExchange, statusCode: Int, body: String) {
        val bytes = body.toByteArray(StandardCharsets.UTF_8)
        exchange.sendResponseHeaders(statusCode, bytes.size.toLong())
        exchange.responseBody.use { it.write(bytes) }
    }
}
