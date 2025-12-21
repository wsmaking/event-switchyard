package app.integration

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import okhttp3.OkHttpClient
import okhttp3.Request
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.TimeUnit

data class BackOfficePosition(
    val accountId: String,
    val symbol: String,
    val netQty: Long,
    val avgPrice: Double? = null
)

data class BackOfficePositionsResponse(
    val positions: List<BackOfficePosition> = emptyList()
)

class BackOfficeClient(
    baseUrl: String = System.getenv("BACKOFFICE_BASE_URL") ?: "http://localhost:8082",
    private val accountId: String = System.getenv("ACCOUNT_ID") ?: "acct_demo",
    private val staticJwt: String? = System.getenv("BACKOFFICE_JWT")?.trim()?.takeIf { it.isNotEmpty() },
    private val jwtFilePath: String? = System.getenv("BACKOFFICE_JWT_FILE")?.trim()?.takeIf { it.isNotEmpty() },
    private val jwtSigner: JwtSigner? = JwtSigner.fromEnv(),
    private val mapper: ObjectMapper = jacksonObjectMapper(),
    private val httpClient: OkHttpClient = OkHttpClient.Builder()
        .callTimeout(5, TimeUnit.SECONDS)
        .build()
) {
    private val url = baseUrl.trimEnd('/') + "/positions"

    fun fetchPositions(): List<BackOfficePosition> {
        val token = loadJwt() ?: jwtSigner?.sign(accountId) ?: throw IllegalStateException("JWT_SECRET_NOT_CONFIGURED")
        val req = Request.Builder()
            .url(url)
            .get()
            .header("Authorization", "Bearer $token")
            .build()

        httpClient.newCall(req).execute().use { resp ->
            if (!resp.isSuccessful) {
                throw IllegalStateException("BACKOFFICE_HTTP_${resp.code}")
            }
            val raw = resp.body?.bytes()?.toString(UTF_8).orEmpty()
            if (raw.isBlank()) return emptyList()
            val parsed = mapper.readValue(raw, BackOfficePositionsResponse::class.java)
            return parsed.positions
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
}
