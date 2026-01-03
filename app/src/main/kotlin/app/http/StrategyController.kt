package app.http

import app.metrics.StrategyMetrics
import app.strategy.StrategyConfigRequest
import app.strategy.StrategyConfigService
import app.strategy.StrategyConfigSnapshot
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import java.nio.charset.StandardCharsets

class StrategyController(
    private val configService: StrategyConfigService
) : HttpHandler {
    private val objectMapper = jacksonObjectMapper()
    private val adminToken = System.getenv("STRATEGY_ADMIN_TOKEN")?.trim().orEmpty()

    override fun handle(exchange: HttpExchange) {
        try {
            exchange.responseHeaders.set("Access-Control-Allow-Origin", "*")
            exchange.responseHeaders.set("Access-Control-Allow-Methods", "GET, PUT, OPTIONS")
            exchange.responseHeaders.set("Access-Control-Allow-Headers", "Content-Type, Authorization")

            if (exchange.requestMethod == "OPTIONS") {
                sendResponse(exchange, 204, "")
                return
            }

            when (exchange.requestMethod) {
                "GET" -> handleGet(exchange)
                "PUT" -> {
                    if (!isAuthorized(exchange)) {
                        sendResponse(exchange, 401, "Unauthorized")
                        return
                    }
                    handlePut(exchange)
                }
                else -> sendResponse(exchange, 405, "Method Not Allowed")
            }
        } catch (e: Exception) {
            val status = if (e.message == "STRATEGY_DB_UNAVAILABLE") 503 else 500
            sendResponse(exchange, status, "Internal Server Error: ${e.message}")
        } finally {
            exchange.close()
        }
    }

    private fun handleGet(exchange: HttpExchange) {
        val snapshot = configService.snapshot()
        val json = objectMapper.writeValueAsString(snapshot.toResponse())
        exchange.responseHeaders.set("Content-Type", "application/json")
        sendResponse(exchange, 200, json)
    }

    private fun handlePut(exchange: HttpExchange) {
        val requestBody = exchange.requestBody.readAllBytes().toString(StandardCharsets.UTF_8)
        val request = objectMapper.readValue<StrategyConfigRequest>(requestBody)
        val errors = request.validate()
        if (errors.isNotEmpty()) {
            StrategyMetrics.recordValidationFailed()
            println("strategy_config: validation failed -> ${errors.joinToString(", ")}")
            val json = objectMapper.writeValueAsString(mapOf("errors" to errors))
            exchange.responseHeaders.set("Content-Type", "application/json")
            sendResponse(exchange, 400, json)
            return
        }
        val updated = configService.update(request)
        val json = objectMapper.writeValueAsString(updated.toResponse())
        exchange.responseHeaders.set("Content-Type", "application/json")
        sendResponse(exchange, 200, json)
    }

    private fun sendResponse(exchange: HttpExchange, status: Int, body: String) {
        val bytes = body.toByteArray(StandardCharsets.UTF_8)
        exchange.sendResponseHeaders(status, bytes.size.toLong())
        exchange.responseBody.use { it.write(bytes) }
    }

    private fun isAuthorized(exchange: HttpExchange): Boolean {
        if (adminToken.isBlank()) return true
        val header = exchange.requestHeaders.getFirst("Authorization") ?: return false
        val prefix = "Bearer "
        if (!header.startsWith(prefix)) return false
        return header.substring(prefix.length).trim() == adminToken
    }
}

private data class StrategyConfigResponse(
    val enabled: Boolean,
    val symbols: List<String>,
    val tickMs: Long,
    val maxOrdersPerMin: Int,
    val cooldownMs: Long,
    val updatedAtMs: Long,
    val storage: String,
    val storageHealthy: Boolean,
    val storageMessage: String?,
    val storageErrorAtMs: Long?
)

private fun StrategyConfigSnapshot.toResponse(): StrategyConfigResponse {
    val config = config
    val status = status
    return StrategyConfigResponse(
        enabled = config.enabled,
        symbols = config.symbols,
        tickMs = config.tickMs,
        maxOrdersPerMin = config.maxOrdersPerMin,
        cooldownMs = config.cooldownMs,
        updatedAtMs = config.updatedAtMs,
        storage = status.storage,
        storageHealthy = status.healthy,
        storageMessage = status.message,
        storageErrorAtMs = status.lastErrorAtMs
    )
}
