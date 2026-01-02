package app.http

import app.engine.Router
import app.clients.gateway.GatewayClient
import app.clients.gateway.GatewayExecutionReport
import app.clients.gateway.GatewayOrderRequest
import app.clients.gateway.GatewayOrderSnapshot
import app.clients.gateway.GatewaySseEvent
import app.clients.gateway.GatewaySseListener
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import java.nio.charset.StandardCharsets
import java.time.Instant
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
    private val gatewayClient: GatewayClient
) : HttpHandler, GatewaySseListener {
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

        // まず戦略エンジン側でローカル判定（担当外や抑止は拒否）
        val payload = objectMapper.writeValueAsBytes(request)
        val routingKey = request.symbol
        val locallyAccepted = router.handle(routingKey, payload)

        val order = if (locallyAccepted) {
            val gatewayRequest = GatewayOrderRequest(
                symbol = request.symbol,
                side = request.side.name,
                type = request.type.name,
                qty = request.quantity.toLong(),
                price = request.price?.toLong(),
                clientOrderId = orderId
            )
            val gatewayResult = gatewayClient.submitOrder(gatewayRequest, idempotencyKey = orderId)
            val executionTimeMs = (System.nanoTime() - startTime) / 1_000_000.0
            if (gatewayResult.accepted) {
                Order(
                    id = gatewayResult.orderId ?: orderId,
                    symbol = request.symbol,
                    side = request.side,
                    type = request.type,
                    quantity = request.quantity,
                    price = request.price,
                    status = OrderStatus.PENDING,
                    submittedAt = submittedAt,
                    filledAt = null,
                    executionTimeMs = executionTimeMs
                )
            } else {
                Order(
                    id = gatewayResult.orderId ?: orderId,
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

        orders[order.id] = order

        val json = objectMapper.writeValueAsString(order)
        exchange.responseHeaders.set("Content-Type", "application/json")
        val httpStatus = when (order.status) {
            OrderStatus.PENDING -> 202
            OrderStatus.FILLED -> 200
            OrderStatus.REJECTED -> 409
        }
        sendResponse(exchange, httpStatus, json)
    }

    private fun handleGetOrders(exchange: HttpExchange) {
        refreshPendingOrders()
        // 注文履歴を新しい順にソート
        val orderList = orders.values
            .distinctBy { it.id }
            .sortedByDescending { it.submittedAt }
        val json = objectMapper.writeValueAsString(orderList)

        exchange.responseHeaders.set("Content-Type", "application/json")
        sendResponse(exchange, 200, json)
    }

    private fun refreshPendingOrders() {
        val pending = orders.values.filter { it.status == OrderStatus.PENDING }
        if (pending.isEmpty()) return
        for (order in pending) {
            val snapshot = gatewayClient.fetchOrder(order.id) ?: continue
            val mapped = mapGatewayStatus(snapshot.status) ?: continue
            if (mapped == order.status) continue
            val updated = order.copy(
                status = mapped,
                price = order.price ?: snapshot.price?.toDouble(),
                filledAt = updatedFilledAt(order, mapped, snapshot),
                executionTimeMs = order.executionTimeMs
            )
            orders[order.id] = updated
        }
    }

    private fun mapGatewayStatus(status: String?): OrderStatus? {
        val normalized = status?.uppercase() ?: return null
        return when (normalized) {
            "FILLED" -> OrderStatus.FILLED
            "REJECTED", "CANCELED" -> OrderStatus.REJECTED
            "ACCEPTED", "SENT", "CANCEL_REQUESTED", "PARTIALLY_FILLED" -> OrderStatus.PENDING
            else -> OrderStatus.PENDING
        }
    }

    private fun updatedFilledAt(
        order: Order,
        status: OrderStatus,
        snapshot: GatewayOrderSnapshot
    ): Long? {
        if (status != OrderStatus.FILLED) return order.filledAt
        val ts = snapshot.lastUpdateAt ?: snapshot.acceptedAt
        return ts?.let { parseInstantMillis(it) } ?: System.currentTimeMillis()
    }

    private fun parseInstantMillis(raw: String): Long? {
        return try {
            Instant.parse(raw).toEpochMilli()
        } catch (_: Throwable) {
            null
        }
    }

    override fun onGatewayEvent(event: GatewaySseEvent) {
        when (event.event) {
            "order_update", "order_snapshot" -> applyGatewaySnapshot(event.data)
            "execution_report" -> applyGatewayExecution(event.data)
        }
    }

    private fun applyGatewaySnapshot(raw: String) {
        val snapshot =
            try {
                objectMapper.readValue<GatewayOrderSnapshot>(raw)
            } catch (_: Throwable) {
                null
            } ?: return

        val orderId = snapshot.orderId ?: return
        val mapped = mapGatewayStatus(snapshot.status) ?: return
        val existing = orders[orderId]

        val submittedAt =
            snapshot.acceptedAt?.let { parseInstantMillis(it) } ?: existing?.submittedAt ?: System.currentTimeMillis()
        val updated =
            if (existing == null) {
                Order(
                    id = orderId,
                    symbol = snapshot.symbol ?: "UNKNOWN",
                    side = parseSide(snapshot.side) ?: OrderSide.BUY,
                    type = parseType(snapshot.type) ?: OrderType.MARKET,
                    quantity = snapshot.qty?.toInt() ?: 0,
                    price = snapshot.price?.toDouble(),
                    status = mapped,
                    submittedAt = submittedAt,
                    filledAt = updatedFilledAtFromSnapshot(mapped, snapshot),
                    executionTimeMs = null
                )
            } else {
                existing.copy(
                    status = mapped,
                    price = existing.price ?: snapshot.price?.toDouble(),
                    filledAt = updatedFilledAt(existing, mapped, snapshot)
                )
            }
        orders[orderId] = updated
    }

    private fun applyGatewayExecution(raw: String) {
        val report =
            try {
                objectMapper.readValue<GatewayExecutionReport>(raw)
            } catch (_: Throwable) {
                null
            } ?: return
        val orderId = report.orderId ?: return
        val existing = orders[orderId] ?: return
        val mapped = mapGatewayStatus(report.status) ?: return
        val updated =
            existing.copy(
                status = mapped,
                filledAt = if (mapped == OrderStatus.FILLED) System.currentTimeMillis() else existing.filledAt
            )
        orders[orderId] = updated
    }

    private fun updatedFilledAtFromSnapshot(status: OrderStatus, snapshot: GatewayOrderSnapshot): Long? {
        if (status != OrderStatus.FILLED) return null
        val ts = snapshot.lastUpdateAt ?: snapshot.acceptedAt
        return ts?.let { parseInstantMillis(it) } ?: System.currentTimeMillis()
    }

    private fun parseSide(raw: String?): OrderSide? {
        val normalized = raw?.uppercase() ?: return null
        return when (normalized) {
            "BUY" -> OrderSide.BUY
            "SELL" -> OrderSide.SELL
            else -> null
        }
    }

    private fun parseType(raw: String?): OrderType? {
        val normalized = raw?.uppercase() ?: return null
        return when (normalized) {
            "MARKET" -> OrderType.MARKET
            "LIMIT" -> OrderType.LIMIT
            else -> null
        }
    }

    private fun sendResponse(exchange: HttpExchange, statusCode: Int, body: String) {
        val bytes = body.toByteArray(StandardCharsets.UTF_8)
        exchange.sendResponseHeaders(statusCode, bytes.size.toLong())
        exchange.responseBody.use { it.write(bytes) }
    }

    // PositionController用の公開メソッド
    fun getAllOrders(): List<Order> = orders.values.toList()
}
