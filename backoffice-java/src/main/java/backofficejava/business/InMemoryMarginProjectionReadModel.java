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

public final class InMemoryMarginProjectionReadModel implements MarginProjectionReadModel {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final Path statePath;
    private final Map<String, MarginProjectionView> values = new ConcurrentHashMap<>();

    public InMemoryMarginProjectionReadModel() {
        this.statePath = resolveStatePath(
            "backoffice.margin.projection.path",
            "BACKOFFICE_MARGIN_PROJECTION_PATH",
            "var/java-replay/backoffice/margin-projections.json"
        );
        load();
    }

    @Override
    public synchronized Optional<MarginProjectionView> findByAccountId(String accountId) {
        return Optional.ofNullable(values.get(accountId));
    }

    @Override
    public synchronized void upsert(MarginProjectionView view) {
        values.put(view.accountId(), view);
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
            StateFileRecovery.recover(statePath, "backoffice_margin_projections", exception);
            reset();
        }
    }

    private void persist() {
        try {
            Files.createDirectories(statePath.getParent());
            OBJECT_MAPPER.writeValue(statePath.toFile(), new Snapshot(Map.copyOf(values)));
        } catch (IOException exception) {
            throw new IllegalStateException("failed_to_persist_margin_projections:" + statePath, exception);
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

    private record Snapshot(Map<String, MarginProjectionView> values) {
    }
}
