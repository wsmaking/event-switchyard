package appjava.clients;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

public final class ExchangeSimulatorClient {
    private final String baseUrl;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public ExchangeSimulatorClient() {
        this.baseUrl = System.getenv().getOrDefault("EXCHANGE_SIM_ADMIN_URL", "http://localhost:9902");
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .build();
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    public Optional<ExchangeSimulatorStatus> fetchStatus() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/admin/status"))
                .timeout(Duration.ofSeconds(2))
                .GET()
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                return Optional.empty();
            }
            return Optional.ofNullable(objectMapper.readValue(response.body(), ExchangeSimulatorStatus.class));
        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public record ExchangeSimulatorStatus(
        String state,
        String mode,
        String sessionState,
        String auctionState,
        String throttleState,
        String entitlementState,
        long delayMs,
        int partialSteps,
        int workingOrders,
        long submittedOrders,
        long rejectedOrders,
        long canceledOrders,
        long partialReports,
        long fillReports,
        boolean dropCopyDivergence,
        List<String> activeIncidents,
        long lastActivityAgeMs
    ) {
    }
}
