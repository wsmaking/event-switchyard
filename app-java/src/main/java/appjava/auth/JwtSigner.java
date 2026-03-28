package appjava.auth;

import com.fasterxml.jackson.databind.ObjectMapper;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

public final class JwtSigner {
    private final byte[] secret;
    private final String issuer;
    private final String audience;
    private final long ttlSec;
    private final ObjectMapper objectMapper;

    public JwtSigner(byte[] secret, String issuer, String audience, long ttlSec, ObjectMapper objectMapper) {
        this.secret = secret;
        this.issuer = issuer;
        this.audience = audience;
        this.ttlSec = ttlSec;
        this.objectMapper = objectMapper;
    }

    public String sign(String accountId) {
        try {
            long now = Instant.now().getEpochSecond();
            Map<String, Object> header = Map.of(
                "alg", "HS256",
                "typ", "JWT"
            );
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("accountId", accountId);
            payload.put("sub", accountId);
            payload.put("exp", now + ttlSec);
            payload.put("nbf", now - 1);
            if (issuer != null && !issuer.isBlank()) {
                payload.put("iss", issuer);
            }
            if (audience != null && !audience.isBlank()) {
                payload.put("aud", audience);
            }

            String headerB64 = base64Url(objectMapper.writeValueAsBytes(header));
            String payloadB64 = base64Url(objectMapper.writeValueAsBytes(payload));
            String signingInput = headerB64 + "." + payloadB64;
            byte[] signature = hmacSha256(secret, signingInput.getBytes(StandardCharsets.UTF_8));
            return signingInput + "." + base64Url(signature);
        } catch (Exception e) {
            throw new IllegalStateException("jwt_sign_failed", e);
        }
    }

    private byte[] hmacSha256(byte[] key, byte[] message) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(key, "HmacSHA256"));
        return mac.doFinal(message);
    }

    private String base64Url(byte[] input) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(input);
    }

    public static JwtSigner fromEnv() {
        String rawSecret = System.getenv("JWT_HS256_SECRET");
        if (rawSecret == null || rawSecret.isBlank()) {
            return null;
        }
        String issuer = emptyToNull(System.getenv("JWT_ISSUER"));
        String audience = emptyToNull(System.getenv("JWT_AUDIENCE"));
        long ttlSec = Long.parseLong(System.getenv().getOrDefault("JWT_TTL_SEC", "300"));
        return new JwtSigner(
            rawSecret.getBytes(StandardCharsets.UTF_8),
            issuer,
            audience,
            ttlSec,
            new ObjectMapper()
        );
    }

    private static String emptyToNull(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
