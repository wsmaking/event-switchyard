package backofficejava.account;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class InMemoryPositionReadModel implements PositionReadModel {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final String accountId;
    private final Path statePath;
    private final ConcurrentMap<String, List<PositionView>> positionsByAccount = new ConcurrentHashMap<>();

    public InMemoryPositionReadModel(String accountId) {
        this.accountId = accountId;
        this.statePath = resolveStatePath();
        load();
    }

    @Override
    public synchronized List<PositionView> findByAccountId(String requestedAccountId) {
        return positionsByAccount.getOrDefault(requestedAccountId, List.of());
    }

    @Override
    public synchronized void replacePositions(String requestedAccountId, List<PositionView> positions) {
        positionsByAccount.put(requestedAccountId, List.copyOf(positions));
        persist();
    }

    @Override
    public synchronized void reset() {
        positionsByAccount.clear();
        positionsByAccount.put(accountId, List.of());
        persist();
    }

    private void load() {
        try {
            if (!Files.exists(statePath)) {
                reset();
                return;
            }
            Snapshot snapshot = OBJECT_MAPPER.readValue(statePath.toFile(), Snapshot.class);
            positionsByAccount.clear();
            if (snapshot.positionsByAccount() != null) {
                snapshot.positionsByAccount().forEach((key, positions) ->
                    positionsByAccount.put(key, positions == null ? List.of() : List.copyOf(positions))
                );
            }
        } catch (IOException e) {
            throw new IllegalStateException("failed_to_load_backoffice_positions:" + statePath, e);
        }
    }

    private void persist() {
        try {
            Files.createDirectories(statePath.getParent());
            OBJECT_MAPPER.writeValue(statePath.toFile(), new Snapshot(copyPositions()));
        } catch (IOException e) {
            throw new IllegalStateException("failed_to_persist_backoffice_positions:" + statePath, e);
        }
    }

    private Map<String, List<PositionView>> copyPositions() {
        ConcurrentMap<String, List<PositionView>> copy = new ConcurrentHashMap<>();
        positionsByAccount.forEach((key, value) -> copy.put(key, List.copyOf(value)));
        return copy;
    }

    private Path resolveStatePath() {
        String configured = System.getProperty(
            "backoffice.positions.path",
            System.getenv().getOrDefault("BACKOFFICE_POSITIONS_PATH", "var/java-replay/backoffice/positions.json")
        );
        return Path.of(configured).toAbsolutePath();
    }

    private record Snapshot(Map<String, List<PositionView>> positionsByAccount) {
    }
}
