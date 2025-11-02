package app.http

import app.engine.Router
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import java.nio.charset.StandardCharsets

/**
 * /statsエンドポイントでRouter統計情報を公開
 */
class StatsController(private val router: Router) : HttpHandler {
    private val objectMapper = jacksonObjectMapper()

    override fun handle(exchange: HttpExchange) {
        try {
            if (exchange.requestMethod != "GET") {
                sendResponse(exchange, 405, "Method Not Allowed")
                return
            }

            val stats = router.getStats()
            val json = objectMapper.writeValueAsString(stats.toMap())

            exchange.responseHeaders.set("Content-Type", "application/json")
            sendResponse(exchange, 200, json)
        } catch (e: Exception) {
            sendResponse(exchange, 500, "Internal Server Error: ${e.message}")
        } finally {
            exchange.close()
        }
    }

    private fun sendResponse(exchange: HttpExchange, statusCode: Int, body: String) {
        val bytes = body.toByteArray(StandardCharsets.UTF_8)
        exchange.sendResponseHeaders(statusCode, bytes.size.toLong())
        exchange.responseBody.use { it.write(bytes) }
    }
}
