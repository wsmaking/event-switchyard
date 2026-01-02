package app.http

import app.clients.backoffice.BackOfficeClient
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import java.nio.charset.StandardCharsets
import kotlin.math.abs

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
 * BackOfficeの台帳を参照して計算
 */
class PositionController(
    private val backOfficeClient: BackOfficeClient,
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
        val positions = backOfficeClient.fetchPositions()
        return positions
            .filter { it.netQty != 0L }
            .map { pos ->
                val qty = pos.netQty.toInt()
                val avgPrice = pos.avgPrice ?: 0.0
                val currentPrice = marketDataController.getCurrentPrice(pos.symbol)
                val unrealizedPnL = (currentPrice - avgPrice) * pos.netQty
                val baseCost = avgPrice * abs(pos.netQty.toDouble())
                val unrealizedPnLPercent = if (baseCost > 0.0) (unrealizedPnL / baseCost) * 100 else 0.0

                Position(
                    symbol = pos.symbol,
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
