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

    public record ReplaceOrderEventsRequest(String orderId, List<OrderEventView> events) {
    }

    public record ReplaceReservationsRequest(String accountId, List<ReservationView> reservations) {
    }
}
