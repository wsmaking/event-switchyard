package gateway.http

import com.fasterxml.jackson.databind.ObjectMapper
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import gateway.audit.AuditLogReader
import gateway.auth.JwtAuth
import gateway.auth.Principal
import gateway.json.Json
import gateway.metrics.GatewayMetrics
import gateway.order.CreateOrderRequest
import gateway.order.OrderService
import java.net.InetSocketAddress
import java.net.URLDecoder
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.Executors

class HttpGateway(
    private val port: Int,
    private val orderService: OrderService,
    private val sseHub: SseHub,
    private val auditLogReader: AuditLogReader,
    private val jwtAuth: JwtAuth,
    private val metrics: GatewayMetrics,
    private val mapper: ObjectMapper = Json.mapper
) : AutoCloseable {
    private val server: HttpServer =
        HttpServer.create(InetSocketAddress(port), 0).apply {
            createContext("/health") { ex ->
                try {
                    metrics.onHttpRequest()
                    if (ex.requestMethod != "GET") return@createContext sendText(ex, 405, "METHOD_NOT_ALLOWED")
                    sendJson(ex, 200, mapOf("status" to "ok"))
                } finally {
                    ex.close()
                }
            }

            createContext("/metrics") { ex ->
                try {
                    metrics.onHttpRequest()
                    if (ex.requestMethod != "GET") return@createContext sendText(ex, 405, "METHOD_NOT_ALLOWED")
                    val token = System.getenv("GATEWAY_METRICS_TOKEN")?.trim().orEmpty()
                    if (token.isNotEmpty()) {
                        val got = ex.requestHeaders.getFirst("X-Metrics-Token")?.trim().orEmpty()
                        if (got != token) return@createContext sendText(ex, 401, "UNAUTHORIZED")
                    }
                    val body = metrics.renderPrometheus()
                    val bytes = body.toByteArray(UTF_8)
                    ex.responseHeaders.add("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                    ex.sendResponseHeaders(200, bytes.size.toLong())
                    ex.responseBody.use { it.write(bytes) }
                } catch (_: Throwable) {
                    sendText(ex, 500, "ERROR")
                } finally {
                    ex.close()
                }
            }

            createContext("/stream") { ex ->
                metrics.onHttpRequest()
                val keepOpen = handleAccountStream(ex)
                if (!keepOpen) ex.close()
            }

            createContext("/orders") { ex ->
                try {
                    metrics.onHttpRequest()
                    if (ex.requestMethod == "POST") {
                        handleCreateOrder(ex)
                    } else {
                        sendText(ex, 405, "METHOD_NOT_ALLOWED")
                    }
                } finally {
                    ex.close()
                }
            }

            createContext("/orders/") { ex ->
                metrics.onHttpRequest()
                val keepOpen = handleOrderRoute(ex)
                if (!keepOpen) ex.close()
            }

            executor = Executors.newCachedThreadPool()
        }

    fun start() {
        server.start()
        println("Gateway listening on :$port")
    }

    private fun handleCreateOrder(ex: HttpExchange) {
        try {
            val principal = requirePrincipal(ex) ?: return
            val idempotencyKey = ex.requestHeaders.getFirst("Idempotency-Key")?.trim()?.takeIf { it.isNotEmpty() }
            val raw = ex.requestBody.readAllBytes()

            val req = try {
                mapper.readValue(raw, CreateOrderRequest::class.java)
            } catch (_: Throwable) {
                return sendText(ex, 400, "INVALID_JSON")
            }

            val result = orderService.acceptOrder(principal, req, idempotencyKey)
            when (result) {
                is gateway.order.AcceptOrderResult.Accepted -> {
                    sendJson(ex, result.httpStatus, mapOf("orderId" to result.orderId, "status" to "ACCEPTED"))
                }
                is gateway.order.AcceptOrderResult.Rejected -> {
                    sendJson(ex, result.httpStatus, mapOf("status" to "REJECTED", "reason" to result.reason))
                }
            }
        } catch (_: Throwable) {
            sendText(ex, 500, "ERROR")
        }
    }

    /**
     * @return true if the handler keeps the connection open (SSE)
     */
    private fun handleOrderRoute(ex: HttpExchange): Boolean {
        val principal = requirePrincipal(ex) ?: return false
        val rest = ex.requestURI.path.removePrefix("/orders/").trim('/')
        if (rest.isEmpty()) {
            sendText(ex, 400, "MISSING_ORDER_ID")
            return false
        }

        val parts = rest.split('/')
        val orderId = parts.firstOrNull()?.trim().orEmpty()
        if (orderId.isEmpty()) {
            sendText(ex, 400, "MISSING_ORDER_ID")
            return false
        }

        return when (ex.requestMethod) {
            "GET" -> {
                val isStream = parts.size == 2 && parts[1] == "stream"
                val isEvents = parts.size == 2 && parts[1] == "events"
                if (isStream) {
                    val status = orderService.getOrder(orderId) ?: run {
                        sendText(ex, 404, "NOT_FOUND")
                        return false
                    }
                    if (status.accountId != principal.accountId) {
                        sendText(ex, 404, "NOT_FOUND")
                        return false
                    }
                    sseHub.open(orderId, parseLastEventId(ex), ex)
                    sseHub.publish(orderId, event = "order_snapshot", data = status)
                    true
                } else if (isEvents) {
                    handleOrderEvents(ex, principal, orderId)
                    false
                } else {
                    val status = orderService.getOrder(orderId) ?: run {
                        sendText(ex, 404, "NOT_FOUND")
                        return false
                    }
                    if (status.accountId != principal.accountId) {
                        sendText(ex, 404, "NOT_FOUND")
                        return false
                    }
                    sendJson(ex, 200, status)
                    false
                }
            }

            "POST" -> {
                val isCancel = parts.size == 2 && parts[1] == "cancel"
                if (!isCancel) {
                    sendText(ex, 405, "METHOD_NOT_ALLOWED")
                    return false
                }
                val result = orderService.requestCancel(principal, orderId)
                when (result) {
                    is gateway.order.CancelOrderResult.Accepted -> sendJson(ex, result.httpStatus, mapOf("orderId" to result.orderId, "status" to "CANCEL_REQUESTED"))
                    is gateway.order.CancelOrderResult.Rejected -> sendJson(ex, result.httpStatus, mapOf("status" to "REJECTED", "reason" to result.reason))
                }
                false
            }

            else -> {
                sendText(ex, 405, "METHOD_NOT_ALLOWED")
                false
            }
        }
    }

    private fun handleOrderEvents(ex: HttpExchange, principal: Principal, orderId: String) {
        val status = orderService.getOrder(orderId) ?: run {
            sendText(ex, 404, "NOT_FOUND")
            return
        }
        if (status.accountId != principal.accountId) {
            sendText(ex, 404, "NOT_FOUND")
            return
        }
        val params = parseQueryParams(ex.requestURI.rawQuery)
        val limit = params["limit"]?.toIntOrNull()
        val events = auditLogReader.readOrderEvents(principal.accountId, orderId, limit)
        sendJson(ex, 200, mapOf("orderId" to orderId, "events" to events))
    }

    /**
     * @return true if the handler keeps the connection open (SSE)
     */
    private fun handleAccountStream(ex: HttpExchange): Boolean {
        if (ex.requestMethod != "GET") {
            sendText(ex, 405, "METHOD_NOT_ALLOWED")
            return false
        }
        val principal = requirePrincipal(ex) ?: return false
        sseHub.openAccount(principal.accountId, parseLastEventId(ex), ex)
        return true
    }

    private fun parseLastEventId(ex: HttpExchange): Long? {
        val h = ex.requestHeaders.getFirst("Last-Event-ID")?.trim()?.takeIf { it.isNotEmpty() } ?: return null
        return h.toLongOrNull()
    }

    private fun parseQueryParams(rawQuery: String?): Map<String, String> {
        val raw = rawQuery?.trim().orEmpty()
        if (raw.isEmpty()) return emptyMap()
        return raw
            .split('&')
            .mapNotNull { part ->
                val idx = part.indexOf('=')
                if (idx <= 0 || idx == part.lastIndex) return@mapNotNull null
                val key = URLDecoder.decode(part.substring(0, idx), UTF_8)
                val value = URLDecoder.decode(part.substring(idx + 1), UTF_8)
                key to value
            }
            .toMap()
    }

    private fun requirePrincipal(ex: HttpExchange): Principal? {
        val auth = jwtAuth.authenticate(ex.requestHeaders.getFirst("Authorization"))
        return when (auth) {
            is JwtAuth.Result.Ok -> auth.principal
            is JwtAuth.Result.Err -> {
                metrics.onHttpUnauthorized()
                sendJson(ex, 401, mapOf("status" to "UNAUTHORIZED", "reason" to auth.reason))
                null
            }
        }
    }

    private fun sendJson(ex: HttpExchange, status: Int, body: Any) {
        val bytes = mapper.writeValueAsBytes(body)
        ex.responseHeaders.add("Content-Type", "application/json; charset=utf-8")
        ex.sendResponseHeaders(status, bytes.size.toLong())
        ex.responseBody.use { it.write(bytes) }
    }

    private fun sendText(ex: HttpExchange, status: Int, text: String) {
        val bytes = text.toByteArray(UTF_8)
        ex.responseHeaders.add("Content-Type", "text/plain; charset=utf-8")
        ex.sendResponseHeaders(status, bytes.size.toLong())
        ex.responseBody.use { it.write(bytes) }
    }

    override fun close() {
        server.stop(0)
    }
}
