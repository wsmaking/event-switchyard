package app.http

import app.engine.Router
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

enum class OrderSide { BUY, SELL }
enum class OrderType { MARKET, LIMIT }
enum class OrderStatus { PENDING, FILLED, REJECTED }

data class Order(
    val id: String,
    val symbol: String,
    val side: OrderSide,
    val type: OrderType,
    val quantity: Int,
    val price: Double?,
    val status: OrderStatus,
    val submittedAt: Long,
    val filledAt: Long? = null,
    val executionTimeMs: Double? = null
)

data class OrderRequest(
    val symbol: String,
    val side: OrderSide,
    val type: OrderType,
    val quantity: Int,
    val price: Double?
)

/**
 * 証券取引注文管理API
 * 個人投資家向けの注文受付・履歴管理
 */
class OrderController(
    private val router: Router,
    private val marketDataController: MarketDataController
) : HttpHandler {
    private val objectMapper = jacksonObjectMapper()
    private val orders = ConcurrentHashMap<String, Order>()

    override fun handle(exchange: HttpExchange) {
        try {
            // CORS headers
            exchange.responseHeaders.set("Access-Control-Allow-Origin", "*")
            exchange.responseHeaders.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            exchange.responseHeaders.set("Access-Control-Allow-Headers", "Content-Type")

            // Handle preflight requests
            if (exchange.requestMethod == "OPTIONS") {
                sendResponse(exchange, 204, "")
                return
            }

            when (exchange.requestMethod) {
                "POST" -> handleSubmitOrder(exchange)
                "GET" -> handleGetOrders(exchange)
                else -> sendResponse(exchange, 405, "Method Not Allowed")
            }
        } catch (e: Exception) {
            sendResponse(exchange, 500, "Internal Server Error: ${e.message}")
        } finally {
            exchange.close()
        }
    }

    private fun handleSubmitOrder(exchange: HttpExchange) {
        val requestBody = exchange.requestBody.readAllBytes().toString(StandardCharsets.UTF_8)
        val request = objectMapper.readValue<OrderRequest>(requestBody)

        // 注文ID生成
        val orderId = UUID.randomUUID().toString()
        val submittedAt = System.currentTimeMillis()
        val startTime = System.nanoTime()

        // 注文をRouterに送信（symbolをkeyとして使用）
        val payload = objectMapper.writeValueAsBytes(request)

        // ルーティングキーは銘柄コードそのまま（OWNED_KEYSと一致させる）
        val routingKey = request.symbol

        val accepted = router.handle(routingKey, payload)

        val order = if (accepted) {
            // OrderBookマッチングエンジンを使用した実際の約定処理
            val orderBook = marketDataController.getOrderBook(request.symbol)

            if (orderBook != null) {
                // 成行注文の場合は現在価格を基準価格として使用
                val limitPrice = if (request.type == OrderType.MARKET) {
                    marketDataController.getCurrentPrice(request.symbol).toInt()
                } else {
                    request.price?.toInt() ?: 0
                }

                // OrderBookでマッチング実行
                val execution = when (request.side) {
                    OrderSide.BUY -> orderBook.processBuyOrder(
                        limitPrice,
                        request.quantity,
                        System.nanoTime()
                    )
                    OrderSide.SELL -> orderBook.processSellOrder(
                        limitPrice,
                        request.quantity,
                        System.nanoTime()
                    )
                }

                val executionTimeMs = (System.nanoTime() - startTime) / 1_000_000.0

                if (execution != null) {
                    // 約定成功
                    Order(
                        id = orderId,
                        symbol = request.symbol,
                        side = request.side,
                        type = request.type,
                        quantity = execution.quantity,
                        price = execution.price.toDouble(),
                        status = OrderStatus.FILLED,
                        submittedAt = submittedAt,
                        filledAt = System.currentTimeMillis(),
                        executionTimeMs = executionTimeMs
                    )
                } else {
                    // マッチング失敗（板に対向注文がない）
                    Order(
                        id = orderId,
                        symbol = request.symbol,
                        side = request.side,
                        type = request.type,
                        quantity = request.quantity,
                        price = request.price,
                        status = OrderStatus.REJECTED,
                        submittedAt = submittedAt,
                        executionTimeMs = executionTimeMs
                    )
                }
            } else {
                // OrderBookが見つからない
                Order(
                    id = orderId,
                    symbol = request.symbol,
                    side = request.side,
                    type = request.type,
                    quantity = request.quantity,
                    price = request.price,
                    status = OrderStatus.REJECTED,
                    submittedAt = submittedAt
                )
            }
        } else {
            // ルーティング失敗
            Order(
                id = orderId,
                symbol = request.symbol,
                side = request.side,
                type = request.type,
                quantity = request.quantity,
                price = request.price,
                status = OrderStatus.REJECTED,
                submittedAt = submittedAt
            )
        }

        orders[orderId] = order

        val json = objectMapper.writeValueAsString(order)
        exchange.responseHeaders.set("Content-Type", "application/json")
        sendResponse(exchange, if (order.status == OrderStatus.FILLED) 200 else 409, json)
    }

    private fun handleGetOrders(exchange: HttpExchange) {
        // 注文履歴を新しい順にソート
        val orderList = orders.values.sortedByDescending { it.submittedAt }
        val json = objectMapper.writeValueAsString(orderList)

        exchange.responseHeaders.set("Content-Type", "application/json")
        sendResponse(exchange, 200, json)
    }

    private fun sendResponse(exchange: HttpExchange, statusCode: Int, body: String) {
        val bytes = body.toByteArray(StandardCharsets.UTF_8)
        exchange.sendResponseHeaders(statusCode, bytes.size.toLong())
        exchange.responseBody.use { it.write(bytes) }
    }

    // PositionController用の公開メソッド
    fun getAllOrders(): List<Order> = orders.values.toList()
}
