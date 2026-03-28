package backofficejava.account;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import backofficejava.support.StateFileRecovery;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryFillReadModel implements FillReadModel {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final Path statePath;
    private final Map<String, List<FillView>> fillsByOrderId = new ConcurrentHashMap<>();

    public InMemoryFillReadModel() {
        this.statePath = resolveStatePath();
        load();
    }

    @Override
    public synchronized List<FillView> findByOrderId(String orderId) {
        return fillsByOrderId.getOrDefault(orderId, List.of());
    }

    @Override
    public synchronized void replaceFills(String orderId, List<FillView> fills) {
        fillsByOrderId.put(orderId, List.copyOf(fills));
        persist();
    }

    @Override
    public synchronized void reset() {
        fillsByOrderId.clear();
        persist();
    }

    private void load() {
        try {
            if (!Files.exists(statePath)) {
                reset();
                return;
            }
            Snapshot snapshot = OBJECT_MAPPER.readValue(statePath.toFile(), Snapshot.class);
            fillsByOrderId.clear();
            if (snapshot.fillsByOrderId() != null) {
                snapshot.fillsByOrderId().forEach((orderId, fills) ->
                    fillsByOrderId.put(orderId, fills == null ? List.of() : List.copyOf(fills))
                );
            }
        } catch (IOException e) {
            StateFileRecovery.recover(statePath, "backoffice_fills", e);
            reset();
        }
    }

    private void persist() {
        try {
            Files.createDirectories(statePath.getParent());
            OBJECT_MAPPER.writeValue(statePath.toFile(), new Snapshot(copyFills()));
        } catch (IOException e) {
            throw new IllegalStateException("failed_to_persist_backoffice_fills:" + statePath, e);
        }
    }

    private Map<String, List<FillView>> copyFills() {
        return fillsByOrderId.entrySet().stream()
            .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, entry -> List.copyOf(entry.getValue())));
    }

    private Path resolveStatePath() {
        String configured = System.getProperty(
            "backoffice.fills.path",
            System.getenv().getOrDefault("BACKOFFICE_FILLS_PATH", "var/java-replay/backoffice/fills.json")
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

    private record Snapshot(Map<String, List<FillView>> fillsByOrderId) {
    }
}
