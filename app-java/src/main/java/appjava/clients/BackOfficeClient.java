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

    public void upsertOrderState(BackOfficeOrderState state) {
        postJson("/internal/orders/state/upsert", state);
    }

    public void replaceLedger(List<LedgerEntry> entries) {
        postJson("/internal/ledger/replace", new ReplaceLedgerRequest(entries));
    }

    public BackOfficeStats fetchStats() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/stats"))
                .GET()
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), BackOfficeStats.class);
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return null;
    }

    public BackOfficeReconcile fetchReconcile(String requestedAccountId) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/reconcile?accountId=" + requestedAccountId))
                .GET()
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), BackOfficeReconcile.class);
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return null;
    }

    public BackOfficeBusStats fetchBusStats() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/internal/bus/stats"))
                .GET()
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), BackOfficeBusStats.class);
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return null;
    }

    public List<LedgerEntry> fetchLedger(String requestedAccountId, String orderId, int limit) {
        try {
            StringBuilder uri = new StringBuilder(baseUrl + "/ledger?accountId=" + requestedAccountId + "&limit=" + limit);
            if (orderId != null && !orderId.isBlank()) {
                uri.append("&orderId=").append(orderId);
            }
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(uri.toString()))
                .GET()
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                LedgerResponse parsed = objectMapper.readValue(response.body(), LedgerResponse.class);
                return parsed.entries == null ? List.of() : parsed.entries;
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return List.of();
    }

    public ReplayResult replayGatewayAudit(boolean resetState) {
        return postJsonWithResponse("/internal/audit/replay", new ReplayRequest(resetState), ReplayResult.class);
    }

    public List<DeadLetterEntry> fetchOrphans(String orderId, int limit) {
        try {
            StringBuilder uri = new StringBuilder(baseUrl + "/orphans?limit=" + limit);
            if (orderId != null && !orderId.isBlank()) {
                uri.append("&orderId=").append(orderId);
            }
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(uri.toString()))
                .GET()
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), objectMapper.getTypeFactory().constructCollectionType(List.class, DeadLetterEntry.class));
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return List.of();
    }

    public List<PendingOrphanEntry> fetchPendingOrphans(String orderId, int limit) {
        try {
            StringBuilder uri = new StringBuilder(baseUrl + "/orphans/pending?limit=" + limit);
            if (orderId != null && !orderId.isBlank()) {
                uri.append("&orderId=").append(orderId);
            }
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(uri.toString()))
                .GET()
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), objectMapper.getTypeFactory().constructCollectionType(List.class, PendingOrphanEntry.class));
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return List.of();
    }

    public RequeueResult requeuePendingOrphans(String orderId) {
        return postJsonWithResponse("/internal/orphans/requeue", new RequeueRequest(orderId), RequeueResult.class);
    }

    public DeadLetterRequeueResult requeueDeadLetter(String eventRef) {
        return postJsonWithResponse("/internal/orphans/dlq/requeue", new DeadLetterRequeueRequest(eventRef), DeadLetterRequeueResult.class);
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

    private <T> T postJsonWithResponse(String path, Object payload, Class<T> responseType) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(payload)))
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return objectMapper.readValue(response.body(), responseType);
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return null;
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

    public record BackOfficeOrderState(
        String orderId,
        String accountId,
        String symbol,
        String side,
        long quantity,
        long workingPrice,
        long submittedAt,
        long lastEventAt,
        String status,
        long filledQuantity,
        long reservedAmount
    ) {
    }

    public record ReplaceLedgerRequest(List<LedgerEntry> entries) {
    }

    public record BackOfficeStats(
        boolean enabled,
        String state,
        String auditPath,
        String offsetPath,
        String startMode,
        String startedAt,
        long processed,
        long skipped,
        long duplicates,
        long orphans,
        long sequenceGaps,
        long replays,
        Long lastEventAt,
        long currentOffset,
        long currentAuditSize,
        int ledgerEntryCount,
        int deadLetterCount,
        int pendingOrphanCount,
        int aggregateProgressCount
    ) {
    }

    public record BackOfficeReconcile(
        String accountId,
        long cashBalance,
        long availableBuyingPower,
        long reservedBuyingPower,
        long realizedPnl,
        long expectedCashBalance,
        long expectedReservedBuyingPower,
        long expectedRealizedPnl,
        List<BackOfficePosition> positions,
        List<String> issues
    ) {
    }

    public record BackOfficeBusStats(
        boolean enabled,
        boolean kafkaEnabled,
        String state,
        String topic,
        String groupId,
        String startedAt,
        long received,
        long applied,
        long duplicates,
        long pending,
        long deadLetters,
        long errors,
        Long lastEventAt
    ) {
    }

    public record LedgerEntry(
        String entryId,
        String eventRef,
        String accountId,
        String orderId,
        String eventType,
        String symbol,
        String side,
        long quantityDelta,
        long cashDelta,
        long reservedBuyingPowerDelta,
        long realizedPnlDelta,
        String detail,
        long eventAt,
        String source
    ) {
    }

    public record LedgerResponse(String accountId, List<LedgerEntry> entries) {
    }

    public record ReplayRequest(boolean resetState) {
    }

    public record ReplayResult(
        String status,
        long offset,
        long processed,
        long skipped,
        long duplicates,
        long orphans
    ) {
    }

    public record DeadLetterEntry(
        String entryId,
        String eventRef,
        String accountId,
        String orderId,
        String eventType,
        String reason,
        String detail,
        String rawLine,
        long eventAt,
        long recordedAt,
        String source
    ) {
    }

    public record PendingOrphanEntry(
        String entryId,
        String eventRef,
        String accountId,
        String orderId,
        String eventType,
        String reason,
        String rawLine,
        long eventAt,
        long recordedAt,
        String source
    ) {
    }

    public record RequeueRequest(String orderId) {
    }

    public record RequeueResult(
        String status,
        String orderId,
        int reprocessed,
        int pendingRemaining
    ) {
    }

    public record DeadLetterRequeueRequest(String eventRef) {
    }

    public record DeadLetterRequeueResult(
        String status,
        String eventRef,
        String outcome,
        int pendingRemaining,
        int deadLetterRemaining
    ) {
    }
}
