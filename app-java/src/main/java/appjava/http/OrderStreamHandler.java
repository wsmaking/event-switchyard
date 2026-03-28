package appjava.http;

import appjava.clients.BackOfficeClient;
import appjava.clients.OmsClient;
import appjava.order.OrderView;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

public final class OrderStreamHandler implements HttpHandler {
    private final OmsClient omsClient;
    private final BackOfficeClient backOfficeClient;

    public OrderStreamHandler(OmsClient omsClient, BackOfficeClient backOfficeClient) {
        this.omsClient = omsClient;
        this.backOfficeClient = backOfficeClient;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        JsonHttpHandler.applyCors(exchange);
        if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
            return;
        }
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            exchange.close();
            return;
        }

        Map<String, String> query = JsonHttpHandler.parseQuery(exchange.getRequestURI().getRawQuery());
        String orderId = query.get("orderId");
        if (orderId == null || orderId.isBlank()) {
            byte[] body = JsonHttpHandler.OBJECT_MAPPER.writeValueAsBytes(new JsonHttpHandler.ErrorResponse("order_id_required"));
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(422, body.length);
            try (OutputStream outputStream = exchange.getResponseBody()) {
                outputStream.write(body);
            } finally {
                exchange.close();
            }
            return;
        }

        exchange.getResponseHeaders().set("Content-Type", "text/event-stream");
        exchange.getResponseHeaders().set("Cache-Control", "no-cache");
        exchange.getResponseHeaders().set("Connection", "keep-alive");
        exchange.sendResponseHeaders(200, 0);

        try (OutputStream outputStream = exchange.getResponseBody()) {
            writeEvent(outputStream, "ready", new StreamSnapshot(orderId, null, 0L, 0L, 0L, 0L, 0, 0, Instant.now().toEpochMilli()));
            String lastDigest = null;
            long lastHeartbeatAt = 0L;
            while (true) {
                StreamSnapshot snapshot = snapshot(orderId);
                String digest = snapshotDigest(snapshot);
                if (!digest.equals(lastDigest)) {
                    writeEvent(outputStream, "update", snapshot);
                    outputStream.flush();
                    lastDigest = digest;
                    lastHeartbeatAt = System.currentTimeMillis();
                } else if (System.currentTimeMillis() - lastHeartbeatAt >= 15_000L) {
                    writeComment(outputStream, "heartbeat");
                    outputStream.flush();
                    lastHeartbeatAt = System.currentTimeMillis();
                }
                Thread.sleep(1_000L);
            }
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } finally {
            exchange.close();
        }
    }

    private StreamSnapshot snapshot(String orderId) {
        Optional<OrderView> order = omsClient.fetchOrder(orderId);
        OmsClient.OmsStats omsStats = omsClient.fetchStats();
        BackOfficeClient.BackOfficeStats backOfficeStats = backOfficeClient.fetchStats();
        return new StreamSnapshot(
            orderId,
            order.map(value -> value.status().name()).orElse(null),
            order.map(OrderView::filledQuantity).orElse(0L),
            order.map(OrderView::remainingQuantity).orElse(0L),
            omsStats == null ? 0L : omsStats.processed(),
            backOfficeStats == null ? 0L : backOfficeStats.processed(),
            omsStats == null ? 0 : omsStats.pendingOrphanCount(),
            backOfficeStats == null ? 0 : backOfficeStats.pendingOrphanCount(),
            Instant.now().toEpochMilli()
        );
    }

    private String snapshotDigest(StreamSnapshot snapshot) {
        return snapshot.orderId()
            + "|" + snapshot.status()
            + "|" + snapshot.filledQuantity()
            + "|" + snapshot.remainingQuantity()
            + "|" + snapshot.omsProcessed()
            + "|" + snapshot.backOfficeProcessed()
            + "|" + snapshot.omsPending()
            + "|" + snapshot.backOfficePending();
    }

    private void writeEvent(OutputStream outputStream, String eventType, Object body) throws IOException {
        outputStream.write(("event: " + eventType + "\n").getBytes(StandardCharsets.UTF_8));
        outputStream.write(("data: " + JsonHttpHandler.OBJECT_MAPPER.writeValueAsString(body) + "\n\n").getBytes(StandardCharsets.UTF_8));
    }

    private void writeComment(OutputStream outputStream, String comment) throws IOException {
        outputStream.write((":" + comment + "\n\n").getBytes(StandardCharsets.UTF_8));
    }

    public record StreamSnapshot(
        String orderId,
        String status,
        long filledQuantity,
        long remainingQuantity,
        long omsProcessed,
        long backOfficeProcessed,
        int omsPending,
        int backOfficePending,
        long emittedAt
    ) {
    }
}
