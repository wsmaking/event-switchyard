package appjava.clients;

import appjava.order.OrderEventView;
import appjava.order.OrderView;
import appjava.order.ReservationView;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

public final class OmsClient {
    private final String baseUrl;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public OmsClient() {
        this.baseUrl = System.getenv().getOrDefault("OMS_JAVA_BASE_URL", "http://localhost:18081");
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .build();
        this.objectMapper = new ObjectMapper();
    }

    public List<OrderView> fetchOrders() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/orders"))
                .timeout(Duration.ofSeconds(3))
                .GET()
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                return List.of();
            }
            return objectMapper.readValue(response.body(), new TypeReference<List<OrderView>>() {});
        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
            return List.of();
        } catch (Exception e) {
            return List.of();
        }
    }

    public Optional<OrderView> fetchOrder(String orderId) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/orders/" + orderId))
                .timeout(Duration.ofSeconds(3))
                .GET()
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                return Optional.empty();
            }
            return Optional.of(objectMapper.readValue(response.body(), OrderView.class));
        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public List<OrderEventView> fetchOrderEvents(String orderId) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/orders/" + orderId + "/events"))
                .timeout(Duration.ofSeconds(3))
                .GET()
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                return List.of();
            }
            return objectMapper.readValue(response.body(), new TypeReference<List<OrderEventView>>() {});
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return List.of();
        } catch (Exception e) {
            return List.of();
        }
    }

    public List<ReservationView> fetchReservations(String accountId) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/accounts/" + accountId + "/reservations"))
                .timeout(Duration.ofSeconds(3))
                .GET()
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                return List.of();
            }
            return objectMapper.readValue(response.body(), new TypeReference<List<ReservationView>>() {});
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return List.of();
        } catch (Exception e) {
            return List.of();
        }
    }

    public void upsertOrder(OrderView orderView) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/internal/orders/upsert"))
                .timeout(Duration.ofSeconds(3))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(orderView)))
                .build();
            httpClient.send(request, HttpResponse.BodyHandlers.discarding());
        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception ignored) {
        }
    }

    public void replaceOrderEvents(String orderId, List<OrderEventView> events) {
        postJson("/internal/orders/events/replace", new ReplaceOrderEventsRequest(orderId, events));
    }

    public void replaceReservations(String accountId, List<ReservationView> reservations) {
        postJson("/internal/accounts/reservations/replace", new ReplaceReservationsRequest(accountId, reservations));
    }

    public void reset() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/internal/orders/reset"))
                .timeout(Duration.ofSeconds(3))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
            httpClient.send(request, HttpResponse.BodyHandlers.discarding());
        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception ignored) {
        }
    }

    public OmsStats fetchStats() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/stats"))
                .timeout(Duration.ofSeconds(3))
                .GET()
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), OmsStats.class);
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        } catch (Exception ignored) {
        }
        return null;
    }

    public OmsReconcile fetchReconcile(String accountId) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/reconcile?accountId=" + accountId))
                .timeout(Duration.ofSeconds(3))
                .GET()
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), OmsReconcile.class);
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        } catch (Exception ignored) {
        }
        return null;
    }

    public OmsBusStats fetchBusStats() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/internal/bus/stats"))
                .timeout(Duration.ofSeconds(3))
                .GET()
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), OmsBusStats.class);
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        } catch (Exception ignored) {
        }
        return null;
    }

    public ReplayResult replayGatewayAudit(boolean resetState) {
        return postJsonWithResponse("/internal/audit/replay", new ReplayRequest(resetState), ReplayResult.class);
    }

    public java.util.List<DeadLetterEntry> fetchOrphans(String orderId, int limit) {
        try {
            StringBuilder uri = new StringBuilder(baseUrl + "/orphans?limit=" + limit);
            if (orderId != null && !orderId.isBlank()) {
                uri.append("&orderId=").append(orderId);
            }
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(uri.toString()))
                .timeout(Duration.ofSeconds(3))
                .GET()
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), new TypeReference<List<DeadLetterEntry>>() {});
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        } catch (Exception ignored) {
        }
        return List.of();
    }

    public java.util.List<PendingOrphanEntry> fetchPendingOrphans(String orderId, int limit) {
        try {
            StringBuilder uri = new StringBuilder(baseUrl + "/orphans/pending?limit=" + limit);
            if (orderId != null && !orderId.isBlank()) {
                uri.append("&orderId=").append(orderId);
            }
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(uri.toString()))
                .timeout(Duration.ofSeconds(3))
                .GET()
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), new TypeReference<List<PendingOrphanEntry>>() {});
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
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

    private void postJson(String path, Object payload) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .timeout(Duration.ofSeconds(3))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(payload)))
                .build();
            httpClient.send(request, HttpResponse.BodyHandlers.discarding());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception ignored) {
        }
    }

    private <T> T postJsonWithResponse(String path, Object payload, Class<T> responseType) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .timeout(Duration.ofSeconds(3))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(payload)))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return objectMapper.readValue(response.body(), responseType);
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        } catch (Exception ignored) {
        }
        return null;
    }

    public record ReplaceOrderEventsRequest(String orderId, List<OrderEventView> events) {
    }

    public record ReplaceReservationsRequest(String accountId, List<ReservationView> reservations) {
    }

    public record OmsStats(
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
        int deadLetterCount,
        int pendingOrphanCount,
        int aggregateProgressCount
    ) {
    }

    public record OmsReconcile(
        String accountId,
        int totalOrders,
        int openOrders,
        long expectedReservedAmount,
        long actualReservedAmount,
        long reservedGapAmount,
        List<String> issues
    ) {
    }

    public record OmsBusStats(
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
