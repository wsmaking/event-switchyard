package appjava.clients;

import appjava.auth.JwtSigner;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;

public final class GatewayClient {
    private final String accountId;
    private final JwtSigner jwtSigner;
    private final String baseUrl;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final String staticJwt;
    private final String jwtFilePath;

    public GatewayClient(String accountId, JwtSigner jwtSigner) {
        this.accountId = accountId;
        this.jwtSigner = jwtSigner;
        this.baseUrl = System.getenv().getOrDefault("GATEWAY_BASE_URL", "http://localhost:8081");
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .build();
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        this.staticJwt = emptyToNull(System.getenv("GATEWAY_JWT"));
        this.jwtFilePath = emptyToNull(System.getenv("GATEWAY_JWT_FILE"));
    }

    public GatewaySubmitResult submitOrder(GatewayOrderRequest request, String idempotencyKey) {
        try {
            String jwt = resolveJwt();
            if (jwt == null) {
                return new GatewaySubmitResult(false, null, "JWT_SECRET_NOT_CONFIGURED", 401);
            }
            HttpRequest httpRequest = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/orders"))
                .timeout(Duration.ofSeconds(3))
                .header("Authorization", "Bearer " + jwt)
                .header("Content-Type", "application/json")
                .header("Idempotency-Key", idempotencyKey)
                .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(request)))
                .build();

            HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
            GatewayOrderResponse parsed = parseGatewayResponse(response.body());
            boolean accepted = parsed != null && "ACCEPTED".equalsIgnoreCase(parsed.status);
            String reason = parsed != null ? parsed.reason : trimToNull(response.body());
            String orderId = parsed != null ? parsed.orderId : null;
            return new GatewaySubmitResult(accepted, orderId, reason, response.statusCode());
        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
            return new GatewaySubmitResult(false, null, "GATEWAY_UNAVAILABLE", 503);
        } catch (Exception e) {
            return new GatewaySubmitResult(false, null, "GATEWAY_ERROR", 500);
        }
    }

    public Optional<GatewayOrderSnapshot> fetchOrder(String orderId) {
        try {
            String jwt = resolveJwt();
            if (jwt == null) {
                return Optional.empty();
            }
            HttpRequest httpRequest = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/orders/" + orderId))
                .timeout(Duration.ofSeconds(3))
                .header("Authorization", "Bearer " + jwt)
                .GET()
                .build();
            HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                return Optional.empty();
            }
            return Optional.ofNullable(objectMapper.readValue(response.body(), GatewayOrderSnapshot.class));
        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public Optional<GatewayHealth> fetchHealth() {
        try {
            HttpRequest httpRequest = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/health"))
                .timeout(Duration.ofSeconds(3))
                .GET()
                .build();
            HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                return Optional.empty();
            }
            return Optional.ofNullable(objectMapper.readValue(response.body(), GatewayHealth.class));
        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private GatewayOrderResponse parseGatewayResponse(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return objectMapper.readValue(raw, GatewayOrderResponse.class);
        } catch (Exception ignored) {
            try {
                JsonNode node = objectMapper.readTree(raw);
                String status = text(node, "status");
                String reason = text(node, "reason");
                String orderId = text(node, "orderId");
                return new GatewayOrderResponse(orderId, status, reason);
            } catch (Exception ignoredAgain) {
                return null;
            }
        }
    }

    private String resolveJwt() throws IOException {
        if (jwtFilePath != null) {
            String text = Files.readString(Path.of(jwtFilePath), StandardCharsets.UTF_8).trim();
            if (!text.isBlank()) {
                return text;
            }
        }
        if (staticJwt != null) {
            return staticJwt;
        }
        if (jwtSigner != null) {
            return jwtSigner.sign(accountId);
        }
        return null;
    }

    private static String text(JsonNode node, String fieldName) {
        JsonNode value = node.get(fieldName);
        return value == null || value.isNull() ? null : value.asText();
    }

    private static String trimToNull(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    private static String emptyToNull(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    public record GatewayOrderRequest(
        String symbol,
        String side,
        String type,
        long qty,
        Long price,
        String timeInForce,
        Long expireAt,
        String clientOrderId
    ) {
    }

    public record GatewayOrderSnapshot(
        String orderId,
        String accountId,
        String clientOrderId,
        String symbol,
        String side,
        String type,
        long qty,
        Long price,
        String timeInForce,
        Long expireAt,
        String status,
        long acceptedAt,
        long lastUpdateAt,
        long filledQty
    ) {
    }

    public record GatewayOrderResponse(String orderId, String status, String reason) {
    }

    public record GatewaySubmitResult(boolean accepted, String orderId, String reason, int httpStatus) {
    }

    public record GatewayHealth(
        String status,
        long queueLen,
        long latencyP50Ns,
        long latencyP99Ns
    ) {
    }
}
