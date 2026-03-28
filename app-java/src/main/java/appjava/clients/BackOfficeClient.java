package appjava.clients;

import appjava.account.AccountOverview;
import appjava.order.FillView;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

public final class BackOfficeClient {
    private final String accountId;
    private final String baseUrl;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public BackOfficeClient(String accountId) {
        this.accountId = accountId;
        this.baseUrl = System.getenv().getOrDefault("BACKOFFICE_JAVA_BASE_URL", "http://localhost:18082");
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .build();
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    public AccountOverview fetchOverview(String requestedAccountId) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/accounts/" + requestedAccountId + "/overview"))
                .GET()
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), AccountOverview.class);
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return AccountOverview.empty(requestedAccountId);
    }

    public List<BackOfficePosition> fetchPositions(String requestedAccountId) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/positions?accountId=" + requestedAccountId))
                .GET()
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                PositionsResponse parsed = objectMapper.readValue(response.body(), PositionsResponse.class);
                return parsed.positions == null ? List.of() : parsed.positions;
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return List.of();
    }

    public List<FillView> fetchFills(String orderId) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/fills?orderId=" + orderId))
                .GET()
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                FillsResponse parsed = objectMapper.readValue(response.body(), FillsResponse.class);
                return parsed.fills == null ? List.of() : parsed.fills;
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return List.of();
    }

    public void resetDemo() {
        postNoBody("/demo/reset");
    }

    public void resetState() {
        postNoBody("/internal/reset");
    }

    public void upsertOverview(AccountOverview overview) {
        postJson("/internal/accounts/upsert", overview);
    }

    public void replacePositions(String requestedAccountId, List<BackOfficePosition> positions) {
        postJson("/internal/positions/replace", new ReplacePositionsRequest(requestedAccountId, positions));
    }

    public void replaceFills(String orderId, List<FillView> fills) {
        postJson("/internal/fills/replace", new ReplaceFillsRequest(orderId, fills));
    }

    private void postNoBody(String path) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .POST(HttpRequest.BodyPublishers.noBody())
                .timeout(Duration.ofSeconds(3))
                .build();
            httpClient.send(request, HttpResponse.BodyHandlers.discarding());
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
    }

    private void postJson(String path, Object payload) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(payload)))
                .timeout(Duration.ofSeconds(3))
                .build();
            httpClient.send(request, HttpResponse.BodyHandlers.discarding());
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
    }

    public String accountId() {
        return accountId;
    }

    public record BackOfficePosition(
        String accountId,
        String symbol,
        long netQty,
        double avgPrice
    ) {
    }

    public record PositionsResponse(List<BackOfficePosition> positions) {
    }

    public record ReplacePositionsRequest(String accountId, List<BackOfficePosition> positions) {
    }

    public record FillsResponse(List<FillView> fills) {
    }

    public record ReplaceFillsRequest(String orderId, List<FillView> fills) {
    }
}
