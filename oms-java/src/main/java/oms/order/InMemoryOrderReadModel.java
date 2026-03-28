package oms.order;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryOrderReadModel implements OrderReadModel {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final Map<String, OrderView> orders = new ConcurrentHashMap<>();
    private final Map<String, List<OrderEventView>> eventsByOrderId = new ConcurrentHashMap<>();
    private final Map<String, List<ReservationView>> reservationsByAccountId = new ConcurrentHashMap<>();
    private final Path statePath;

    public InMemoryOrderReadModel(String accountId) {
        this.statePath = resolveStatePath();
        load();
    }

    @Override
    public synchronized List<OrderView> findAll() {
        return orders.values().stream()
            .sorted((left, right) -> Long.compare(right.submittedAt(), left.submittedAt()))
            .toList();
    }

    @Override
    public synchronized Optional<OrderView> findById(String orderId) {
        return Optional.ofNullable(orders.get(orderId));
    }

    @Override
    public synchronized List<OrderEventView> findEventsByOrderId(String orderId) {
        return eventsByOrderId.getOrDefault(orderId, List.of());
    }

    @Override
    public synchronized List<ReservationView> findReservationsByAccountId(String accountId) {
        return reservationsByAccountId.getOrDefault(accountId, List.of());
    }

    @Override
    public synchronized void upsert(OrderView orderView) {
        orders.put(orderView.id(), orderView);
        persist();
    }

    @Override
    public synchronized void replaceEvents(String orderId, List<OrderEventView> events) {
        eventsByOrderId.put(orderId, List.copyOf(events));
        persist();
    }

    @Override
    public synchronized void replaceReservations(String accountId, List<ReservationView> reservations) {
        reservationsByAccountId.put(accountId, List.copyOf(reservations));
        persist();
    }

    @Override
    public synchronized void reset() {
        orders.clear();
        eventsByOrderId.clear();
        reservationsByAccountId.clear();
        persist();
    }

    private void load() {
        try {
            if (!Files.exists(statePath)) {
                reset();
                return;
            }
            Snapshot snapshot = OBJECT_MAPPER.readValue(statePath.toFile(), Snapshot.class);
            orders.clear();
            eventsByOrderId.clear();
            reservationsByAccountId.clear();
            if (snapshot.orders() != null) {
                snapshot.orders().forEach(order -> orders.put(order.id(), order));
            }
            if (snapshot.eventsByOrderId() != null) {
                snapshot.eventsByOrderId().forEach((orderId, events) ->
                    eventsByOrderId.put(orderId, events == null ? List.of() : List.copyOf(events))
                );
            }
            if (snapshot.reservationsByAccountId() != null) {
                snapshot.reservationsByAccountId().forEach((accountId, reservations) ->
                    reservationsByAccountId.put(accountId, reservations == null ? List.of() : List.copyOf(reservations))
                );
            }
        } catch (IOException e) {
            throw new IllegalStateException("failed_to_load_oms_state:" + statePath, e);
        }
    }

    private void persist() {
        try {
            Files.createDirectories(statePath.getParent());
            OBJECT_MAPPER.writeValue(
                statePath.toFile(),
                new Snapshot(
                    findAll(),
                    copyEvents(),
                    copyReservations()
                )
            );
        } catch (IOException e) {
            throw new IllegalStateException("failed_to_persist_oms_state:" + statePath, e);
        }
    }

    private Map<String, List<OrderEventView>> copyEvents() {
        return eventsByOrderId.entrySet().stream()
            .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, entry -> List.copyOf(entry.getValue())));
    }

    private Map<String, List<ReservationView>> copyReservations() {
        return reservationsByAccountId.entrySet().stream()
            .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, entry -> List.copyOf(entry.getValue())));
    }

    private Path resolveStatePath() {
        String configured = System.getProperty(
            "oms.state.path",
            System.getenv().getOrDefault("OMS_STATE_PATH", "var/java-replay/oms/state.json")
        );
        Path path = Path.of(configured);
        return path.isAbsolute() ? path : workspaceRoot().resolve(path).normalize();
    }

    private Path workspaceRoot() {
        Path current = Path.of("").toAbsolutePath().normalize();
        while (current != null && !Files.exists(current.resolve("settings.gradle"))) {
            current = current.getParent();
        }
        return current != null ? current : Path.of("").toAbsolutePath().normalize();
    }

    private record Snapshot(
        List<OrderView> orders,
        Map<String, List<OrderEventView>> eventsByOrderId,
        Map<String, List<ReservationView>> reservationsByAccountId
    ) {
    }
}
