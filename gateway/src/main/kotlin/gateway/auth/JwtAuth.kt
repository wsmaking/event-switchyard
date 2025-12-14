package gateway.auth

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import gateway.json.Json
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

class JwtAuth(
    private val secret: ByteArray = (System.getenv("JWT_HS256_SECRET") ?: "").toByteArray(UTF_8),
    private val issuer: String? = System.getenv("JWT_ISSUER")?.takeIf { it.isNotBlank() },
    private val audience: String? = System.getenv("JWT_AUDIENCE")?.takeIf { it.isNotBlank() },
    private val clockSkewSec: Long = (System.getenv("JWT_CLOCK_SKEW_SEC") ?: "30").toLong(),
    private val mapper: ObjectMapper = Json.mapper
) {
    sealed class Result {
        data class Ok(val principal: Principal) : Result()
        data class Err(val reason: String) : Result()
    }

    fun authenticate(authorizationHeader: String?): Result {
        if (secret.isEmpty()) return Result.Err("JWT_SECRET_NOT_CONFIGURED")
        val token = parseBearerToken(authorizationHeader) ?: return Result.Err("MISSING_BEARER_TOKEN")
        return verifyHs256(token)
    }

    private fun parseBearerToken(h: String?): String? {
        if (h == null) return null
        val trimmed = h.trim()
        if (!trimmed.startsWith("Bearer ")) return null
        return trimmed.removePrefix("Bearer ").trim().takeIf { it.isNotEmpty() }
    }

    private fun verifyHs256(token: String): Result {
        val parts = token.split('.')
        if (parts.size != 3) return Result.Err("INVALID_JWT_FORMAT")
        val headerB64 = parts[0]
        val payloadB64 = parts[1]
        val sigB64 = parts[2]

        val headerJson = decodeJson(headerB64) ?: return Result.Err("INVALID_JWT_HEADER")
        val payloadJson = decodeJson(payloadB64) ?: return Result.Err("INVALID_JWT_PAYLOAD")

        val alg = headerJson.text("alg") ?: return Result.Err("MISSING_ALG")
        if (alg != "HS256") return Result.Err("UNSUPPORTED_ALG")

        val signingInput = "${headerB64}.${payloadB64}".toByteArray(UTF_8)
        val expectedSig = hmacSha256(secret, signingInput)
        val actualSig = decodeB64Url(sigB64) ?: return Result.Err("INVALID_JWT_SIGNATURE")
        if (!constantTimeEquals(expectedSig, actualSig)) return Result.Err("INVALID_JWT_SIGNATURE")

        val now = Instant.now()
        val exp = payloadJson.long("exp") ?: return Result.Err("MISSING_EXP")
        val nbf = payloadJson.long("nbf")
        if (issuer != null && payloadJson.text("iss") != issuer) return Result.Err("INVALID_ISSUER")
        if (audience != null && !payloadHasAudience(payloadJson, audience)) return Result.Err("INVALID_AUDIENCE")

        val expInstant = Instant.ofEpochSecond(exp)
        if (now.isAfter(expInstant.plusSeconds(clockSkewSec))) return Result.Err("TOKEN_EXPIRED")
        if (nbf != null) {
            val nbfInstant = Instant.ofEpochSecond(nbf)
            if (now.isBefore(nbfInstant.minusSeconds(clockSkewSec))) return Result.Err("TOKEN_NOT_YET_VALID")
        }

        val accountId = payloadJson.text("accountId")?.takeIf { it.isNotBlank() }
            ?: payloadJson.text("sub")?.takeIf { it.isNotBlank() }
            ?: return Result.Err("MISSING_ACCOUNT_ID")

        return Result.Ok(Principal(accountId = accountId))
    }

    private fun payloadHasAudience(payload: JsonNode, expected: String): Boolean {
        val aud = payload.get("aud") ?: return false
        return when {
            aud.isTextual -> aud.asText() == expected
            aud.isArray -> aud.any { it.isTextual && it.asText() == expected }
            else -> false
        }
    }

    private fun decodeJson(b64Url: String): JsonNode? {
        val bytes = decodeB64Url(b64Url) ?: return null
        return try {
            mapper.readTree(bytes)
        } catch (_: Throwable) {
            null
        }
    }

    private fun decodeB64Url(s: String): ByteArray? {
        return try {
            Base64.getUrlDecoder().decode(s)
        } catch (_: IllegalArgumentException) {
            null
        }
    }

    private fun hmacSha256(key: ByteArray, msg: ByteArray): ByteArray {
        val mac = Mac.getInstance("HmacSHA256")
        mac.init(SecretKeySpec(key, "HmacSHA256"))
        return mac.doFinal(msg)
    }

    private fun constantTimeEquals(a: ByteArray, b: ByteArray): Boolean {
        if (a.size != b.size) return false
        var res = 0
        for (i in a.indices) {
            res = res or (a[i].toInt() xor b[i].toInt())
        }
        return res == 0
    }

    private fun JsonNode.text(field: String): String? {
        val n = this.get(field) ?: return null
        if (n.isNull) return null
        return n.asText()
    }

    private fun JsonNode.long(field: String): Long? {
        val n = this.get(field) ?: return null
        if (n.isNull) return null
        if (!n.isNumber) return null
        return n.asLong()
    }
}

