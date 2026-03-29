package backofficejava.business;

import backofficejava.support.StateFileRecovery;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryOperatorControlStateReadModel implements OperatorControlStateReadModel {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final Path statePath;
    private final Map<String, OperatorControlStateView> values = new ConcurrentHashMap<>();

    public InMemoryOperatorControlStateReadModel() {
        this.statePath = resolveStatePath(
            "backoffice.operator.control.path",
            "BACKOFFICE_OPERATOR_CONTROL_PATH",
            "var/java-replay/backoffice/operator-controls.json"
        );
        load();
    }

    @Override
    public synchronized Optional<OperatorControlStateView> findByOrderId(String orderId) {
        return Optional.ofNullable(values.get(orderId));
    }

    @Override
    public synchronized void upsert(OperatorControlStateView view) {
        values.put(view.orderId(), view);
        persist();
    }

    @Override
    public synchronized void reset() {
        values.clear();
        persist();
    }

    private void load() {
        try {
            if (!Files.exists(statePath)) {
                reset();
                return;
            }
            Snapshot snapshot = OBJECT_MAPPER.readValue(statePath.toFile(), Snapshot.class);
            values.clear();
            if (snapshot.values() != null) {
                values.putAll(snapshot.values());
            }
        } catch (IOException exception) {
            StateFileRecovery.recover(statePath, "backoffice_operator_controls", exception);
            reset();
        }
    }

    private void persist() {
        try {
            Files.createDirectories(statePath.getParent());
            OBJECT_MAPPER.writeValue(statePath.toFile(), new Snapshot(Map.copyOf(values)));
        } catch (IOException exception) {
            throw new IllegalStateException("failed_to_persist_operator_controls:" + statePath, exception);
        }
    }

    private static Path resolveStatePath(String property, String envKey, String fallback) {
        String configured = System.getProperty(property, System.getenv().getOrDefault(envKey, fallback));
        Path path = Path.of(configured);
        return path.isAbsolute() ? path : workspaceRoot().resolve(path).normalize();
    }

    private static Path workspaceRoot() {
        Path current = Path.of("").toAbsolutePath().normalize();
        while (current != null && !Files.exists(current.resolve("settings.gradle"))) {
            current = current.getParent();
        }
        return current != null ? current : Path.of("").toAbsolutePath().normalize();
    }

    private record Snapshot(Map<String, OperatorControlStateView> values) {
    }
}
