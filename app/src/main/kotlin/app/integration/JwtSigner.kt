package app.integration

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

class JwtSigner(
    private val secret: ByteArray,
    private val issuer: String?,
    private val audience: String?,
    private val ttlSec: Long,
    private val mapper: ObjectMapper = jacksonObjectMapper()
) {
    fun sign(accountId: String): String {
        val now = Instant.now().epochSecond
        val header = mapOf(
            "alg" to "HS256",
            "typ" to "JWT"
        )
        val payload = mutableMapOf<String, Any>(
            "accountId" to accountId,
            "sub" to accountId,
            "exp" to (now + ttlSec),
            "nbf" to (now - 1)
        )
        if (!issuer.isNullOrBlank()) payload["iss"] = issuer
        if (!audience.isNullOrBlank()) payload["aud"] = audience

        val headerB64 = b64Url(mapper.writeValueAsBytes(header))
        val payloadB64 = b64Url(mapper.writeValueAsBytes(payload))
        val signingInput = "${headerB64}.${payloadB64}".toByteArray(UTF_8)
        val signature = hmacSha256(secret, signingInput)
        val signatureB64 = b64Url(signature)
        return "${headerB64}.${payloadB64}.${signatureB64}"
    }

    private fun hmacSha256(key: ByteArray, msg: ByteArray): ByteArray {
        val mac = Mac.getInstance("HmacSHA256")
        mac.init(SecretKeySpec(key, "HmacSHA256"))
        return mac.doFinal(msg)
    }

    private fun b64Url(bytes: ByteArray): String {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes)
    }

    companion object {
        fun fromEnv(): JwtSigner? {
            val secret = (System.getenv("JWT_HS256_SECRET") ?: "").toByteArray(UTF_8)
            if (secret.isEmpty()) return null
            val issuer = System.getenv("JWT_ISSUER")?.trim()?.takeIf { it.isNotEmpty() }
            val audience = System.getenv("JWT_AUDIENCE")?.trim()?.takeIf { it.isNotEmpty() }
            val ttl = (System.getenv("JWT_TTL_SEC") ?: "300").toLong()
            return JwtSigner(secret = secret, issuer = issuer, audience = audience, ttlSec = ttl)
        }
    }
}
