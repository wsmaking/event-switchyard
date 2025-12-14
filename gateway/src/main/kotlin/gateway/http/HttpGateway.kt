package gateway.http

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import gateway.order.CreateOrderRequest
import gateway.order.OrderService
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.Executors

class HttpGateway(
    private val port: Int,
    private val orderService: OrderService,
    private val sseHub: SseHub,
    private val mapper: ObjectMapper = jacksonObjectMapper().findAndRegisterModules()
) : AutoCloseable {
    private val server: HttpServer =
        HttpServer.create(InetSocketAddress(port), 0).apply {
            createContext("/health") { ex ->
                try {
                    if (ex.requestMethod != "GET") return@createContext sendText(ex, 405, "METHOD_NOT_ALLOWED")
                    sendJson(ex, 200, mapOf("status" to "ok"))
                } finally {
                    ex.close()
                }
            }

            createContext("/orders") { ex ->
                try {
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
                val keepOpen = handleOrderGetOrStream(ex)
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
            val idempotencyKey = ex.requestHeaders.getFirst("Idempotency-Key")?.trim()?.takeIf { it.isNotEmpty() }
            val raw = ex.requestBody.readAllBytes()

            val req = try {
                mapper.readValue(raw, CreateOrderRequest::class.java)
            } catch (_: Throwable) {
                return sendText(ex, 400, "INVALID_JSON")
            }

            val result = orderService.acceptOrder(req, idempotencyKey)
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
    private fun handleOrderGetOrStream(ex: HttpExchange): Boolean {
        if (ex.requestMethod != "GET") {
            sendText(ex, 405, "METHOD_NOT_ALLOWED")
            return false
        }

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

        val isStream = parts.size == 2 && parts[1] == "stream"
        if (isStream) {
            val status = orderService.getOrder(orderId) ?: run {
                sendText(ex, 404, "NOT_FOUND")
                return false
            }
            sseHub.open(orderId, ex)
            sseHub.publish(orderId, event = "order_snapshot", data = status)
            return true
        }

        val status = orderService.getOrder(orderId) ?: run {
            sendText(ex, 404, "NOT_FOUND")
            return false
        }
        sendJson(ex, 200, status)
        return false
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
