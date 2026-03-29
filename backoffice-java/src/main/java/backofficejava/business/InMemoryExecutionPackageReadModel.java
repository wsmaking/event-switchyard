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

public final class InMemoryExecutionPackageReadModel implements ExecutionPackageReadModel {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final Path statePath;
    private final Map<String, ExecutionPackageView> packagesByOrderId = new ConcurrentHashMap<>();

    public InMemoryExecutionPackageReadModel() {
        this.statePath = resolveStatePath(
            "backoffice.execution.package.path",
            "BACKOFFICE_EXECUTION_PACKAGE_PATH",
            "var/java-replay/backoffice/execution-packages.json"
        );
        load();
    }

    @Override
    public synchronized Optional<ExecutionPackageView> findByOrderId(String orderId) {
        return Optional.ofNullable(packagesByOrderId.get(orderId));
    }

    @Override
    public synchronized void upsert(ExecutionPackageView view) {
        packagesByOrderId.put(view.orderId(), view);
        persist();
    }

    @Override
    public synchronized void reset() {
        packagesByOrderId.clear();
        persist();
    }

    private void load() {
        try {
            if (!Files.exists(statePath)) {
                reset();
                return;
            }
            Snapshot snapshot = OBJECT_MAPPER.readValue(statePath.toFile(), Snapshot.class);
            packagesByOrderId.clear();
            if (snapshot.packagesByOrderId() != null) {
                packagesByOrderId.putAll(snapshot.packagesByOrderId());
            }
        } catch (IOException exception) {
            StateFileRecovery.recover(statePath, "backoffice_execution_packages", exception);
            reset();
        }
    }

    private void persist() {
        try {
            Files.createDirectories(statePath.getParent());
            OBJECT_MAPPER.writeValue(statePath.toFile(), new Snapshot(Map.copyOf(packagesByOrderId)));
        } catch (IOException exception) {
            throw new IllegalStateException("failed_to_persist_execution_packages:" + statePath, exception);
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

    private record Snapshot(Map<String, ExecutionPackageView> packagesByOrderId) {
    }
}
