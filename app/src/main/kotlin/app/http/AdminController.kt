package app.http

import app.engine.Router
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import java.nio.charset.StandardCharsets

data class ConfigUpdate(
    val key: String,
    val value: String
)

data class ConfigResponse(
    val success: Boolean,
    val message: String,
    val config: Map<String, String>? = null
)

class AdminController(
    private val router: Router
) : HttpHandler {
    private val objectMapper = jacksonObjectMapper()

    override fun handle(exchange: HttpExchange) {
        try {
            exchange.responseHeaders.set("Access-Control-Allow-Origin", "*")
            exchange.responseHeaders.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            exchange.responseHeaders.set("Access-Control-Allow-Headers", "Content-Type")

            if (exchange.requestMethod == "OPTIONS") {
                sendResponse(exchange, 204, "")
                return
            }

            when (exchange.requestMethod) {
                "GET" -> handleGetConfig(exchange)
                "POST" -> handleUpdateConfig(exchange)
                else -> sendResponse(exchange, 405, "Method Not Allowed")
            }
        } catch (e: Exception) {
            sendResponse(exchange, 500, "Internal Server Error: ${e.message}")
        } finally {
            exchange.close()
        }
    }

    private fun handleGetConfig(exchange: HttpExchange) {
        val config = mapOf(
            "FAST_PATH_ENABLE" to System.getenv("FAST_PATH_ENABLE").orEmpty(),
            "KAFKA_BRIDGE_ENABLE" to System.getenv("KAFKA_BRIDGE_ENABLE").orEmpty(),
            "FAST_PATH_SYMBOLS" to System.getenv("FAST_PATH_SYMBOLS").orEmpty(),
            "OWNED_KEYS" to System.getenv("OWNED_KEYS").orEmpty(),
            "JWT_HS256_SECRET_SET" to (System.getenv("JWT_HS256_SECRET")?.isNotBlank() == true).toString(),
            "GATEWAY_JWT_SET" to (System.getenv("GATEWAY_JWT")?.isNotBlank() == true).toString(),
            "GATEWAY_JWT_FILE_SET" to (System.getenv("GATEWAY_JWT_FILE")?.isNotBlank() == true).toString(),
            "BACKOFFICE_JWT_SET" to (System.getenv("BACKOFFICE_JWT")?.isNotBlank() == true).toString(),
            "BACKOFFICE_JWT_FILE_SET" to (System.getenv("BACKOFFICE_JWT_FILE")?.isNotBlank() == true).toString()
        )

        val response = ConfigResponse(
            success = true,
            message = "Current configuration",
            config = config
        )

        val json = objectMapper.writeValueAsString(response)
        exchange.responseHeaders.set("Content-Type", "application/json")
        sendResponse(exchange, 200, json)
    }

    private fun handleUpdateConfig(exchange: HttpExchange) {
        val requestBody = exchange.requestBody.readAllBytes().toString(StandardCharsets.UTF_8)
        val request = objectMapper.readValue<ConfigUpdate>(requestBody)

        val response = ConfigResponse(
            success = false,
            message = "Runtime configuration update not implemented. Restart required."
        )

        val json = objectMapper.writeValueAsString(response)
        exchange.responseHeaders.set("Content-Type", "application/json")
        sendResponse(exchange, 501, json)
    }

    private fun sendResponse(exchange: HttpExchange, statusCode: Int, body: String) {
        val bytes = body.toByteArray(StandardCharsets.UTF_8)
        exchange.sendResponseHeaders(statusCode, bytes.size.toLong())
        exchange.responseBody.use { it.write(bytes) }
    }
}
