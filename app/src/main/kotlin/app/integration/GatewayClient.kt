package app.integration

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.TimeUnit

data class GatewayOrderRequest(
    val symbol: String,
    val side: String,
    val type: String,
    val qty: Long,
    val price: Long? = null,
    val clientOrderId: String? = null
)

data class GatewayOrderSnapshot(
    val orderId: String? = null,
    val accountId: String? = null,
    val clientOrderId: String? = null,
    val symbol: String? = null,
    val side: String? = null,
    val type: String? = null,
    val qty: Long? = null,
    val price: Long? = null,
    val status: String? = null,
    val acceptedAt: String? = null,
    val lastUpdateAt: String? = null,
    val filledQty: Long? = null
)

data class GatewayOrderResponse(
    val orderId: String? = null,
    val status: String? = null,
    val reason: String? = null
)

data class GatewaySubmitResult(
    val accepted: Boolean,
    val orderId: String?,
    val reason: String?,
    val httpStatus: Int
)

class GatewayClient(
    baseUrl: String = System.getenv("GATEWAY_BASE_URL") ?: "http://localhost:8081",
    private val accountId: String = System.getenv("ACCOUNT_ID") ?: "acct_demo",
    private val staticJwt: String? = System.getenv("GATEWAY_JWT")?.trim()?.takeIf { it.isNotEmpty() },
    private val jwtFilePath: String? = System.getenv("GATEWAY_JWT_FILE")?.trim()?.takeIf { it.isNotEmpty() },
    private val jwtSigner: JwtSigner? = JwtSigner.fromEnv(),
    private val mapper: ObjectMapper = jacksonObjectMapper(),
    private val httpClient: OkHttpClient = OkHttpClient.Builder()
        .callTimeout(5, TimeUnit.SECONDS)
        .build()
) {
    private val url = baseUrl.trimEnd('/')
    private val ordersUrl = url + "/orders"

    fun submitOrder(request: GatewayOrderRequest, idempotencyKey: String?): GatewaySubmitResult {
        val token = loadJwt() ?: jwtSigner?.sign(accountId)
            ?: return GatewaySubmitResult(
                accepted = false,
                orderId = null,
                reason = "JWT_SECRET_NOT_CONFIGURED",
                httpStatus = 401
            )
        if (System.getenv("GATEWAY_JWT_DEBUG") == "1") {
            val preview = if (token.length > 16) token.substring(0, 16) else token
            println("GatewayClient JWT len=${token.length} head=${preview}")
        }

        val body = mapper.writeValueAsBytes(request).toRequestBody(JSON)
        val reqBuilder = Request.Builder()
            .url(ordersUrl)
            .post(body)
            .header("Authorization", "Bearer $token")
            .header("Content-Type", "application/json")

        if (!idempotencyKey.isNullOrBlank()) {
            reqBuilder.header("Idempotency-Key", idempotencyKey)
        }

        httpClient.newCall(reqBuilder.build()).execute().use { resp ->
            val raw = resp.body?.bytes()?.toString(UTF_8).orEmpty()
            val parsed = parseResponse(raw)
            val status = parsed?.status?.uppercase()
            val accepted = status == "ACCEPTED"
            val reason = parsed?.reason ?: if (accepted) null else raw.ifBlank { "REJECTED" }
            return GatewaySubmitResult(
                accepted = accepted,
                orderId = parsed?.orderId,
                reason = reason,
                httpStatus = resp.code
            )
        }
    }

    fun fetchOrder(orderId: String): GatewayOrderSnapshot? {
        val token = loadJwt() ?: jwtSigner?.sign(accountId) ?: return null
        val req = Request.Builder()
            .url("${ordersUrl}/${orderId}")
            .get()
            .header("Authorization", "Bearer $token")
            .build()
        httpClient.newCall(req).execute().use { resp ->
            if (!resp.isSuccessful) return null
            val raw = resp.body?.bytes()?.toString(UTF_8).orEmpty()
            return parseSnapshot(raw)
        }
    }

    private fun parseResponse(raw: String): GatewayOrderResponse? {
        return try {
            if (raw.isBlank()) null else mapper.readValue(raw, GatewayOrderResponse::class.java)
        } catch (_: Throwable) {
            null
        }
    }

    private fun parseSnapshot(raw: String): GatewayOrderSnapshot? {
        return try {
            if (raw.isBlank()) null else mapper.readValue(raw, GatewayOrderSnapshot::class.java)
        } catch (_: Throwable) {
            null
        }
    }

    private fun loadJwt(): String? {
        if (!jwtFilePath.isNullOrBlank()) {
            val text =
                try {
                    java.io.File(jwtFilePath).readText()
                } catch (_: Throwable) {
                    null
                }
            val trimmed = text?.trim()
            if (!trimmed.isNullOrEmpty()) return trimmed
        }
        return staticJwt
    }

    companion object {
        private val JSON = "application/json; charset=utf-8".toMediaType()
    }
}
