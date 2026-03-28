package backofficejava.account;

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

public final class InMemoryOrderProjectionStateStore implements OrderProjectionStateStore {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final Path statePath;
    private final Map<String, OrderProjectionState> statesByOrderId = new ConcurrentHashMap<>();

    public InMemoryOrderProjectionStateStore() {
        this.statePath = resolveStatePath();
        load();
    }

    @Override
    public synchronized Optional<OrderProjectionState> findByOrderId(String orderId) {
        return Optional.ofNullable(statesByOrderId.get(orderId));
    }

    @Override
    public synchronized List<OrderProjectionState> findByAccountId(String accountId) {
        return statesByOrderId.values().stream()
            .filter(state -> accountId.equals(state.accountId()))
            .sorted((left, right) -> Long.compare(right.lastEventAt(), left.lastEventAt()))
            .toList();
    }

    @Override
    public synchronized void upsert(OrderProjectionState state) {
        statesByOrderId.put(state.orderId(), state);
        persist();
    }

    @Override
    public synchronized void reset() {
        statesByOrderId.clear();
        persist();
    }

    private void load() {
        try {
            if (!Files.exists(statePath)) {
                reset();
                return;
            }
            Snapshot snapshot = OBJECT_MAPPER.readValue(statePath.toFile(), Snapshot.class);
            statesByOrderId.clear();
            if (snapshot.states() != null) {
                snapshot.states().forEach(state -> statesByOrderId.put(state.orderId(), state));
            }
        } catch (IOException exception) {
            throw new IllegalStateException("failed_to_load_backoffice_order_states:" + statePath, exception);
        }
    }

    private void persist() {
        try {
            Files.createDirectories(statePath.getParent());
            OBJECT_MAPPER.writeValue(statePath.toFile(), new Snapshot(statesByOrderId.values().stream().toList()));
        } catch (IOException exception) {
            throw new IllegalStateException("failed_to_persist_backoffice_order_states:" + statePath, exception);
        }
    }

    private Path resolveStatePath() {
        String configured = System.getProperty(
            "backoffice.order.state.path",
            System.getenv().getOrDefault("BACKOFFICE_ORDER_STATE_PATH", "var/java-replay/backoffice/orders.json")
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

    private record Snapshot(List<OrderProjectionState> states) {
    }
}
