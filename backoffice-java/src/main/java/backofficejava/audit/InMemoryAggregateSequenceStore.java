package backofficejava.audit;

import backofficejava.support.StateFileRecovery;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public final class InMemoryAggregateSequenceStore implements AggregateSequenceStore {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final Path statePath;
    private final Map<String, AggregateProgress> progressByAggregateId = new HashMap<>();

    public InMemoryAggregateSequenceStore() {
        this.statePath = resolveStatePath();
        load();
    }

    @Override
    public synchronized long readLastApplied(String aggregateId) {
        if (aggregateId == null || aggregateId.isBlank()) {
            return 0L;
        }
        AggregateProgress progress = progressByAggregateId.get(aggregateId);
        return progress == null ? 0L : progress.aggregateSeq();
    }

    @Override
    public synchronized void markApplied(String aggregateId, long aggregateSeq, String eventRef, long eventAt) {
        if (aggregateId == null || aggregateId.isBlank() || aggregateSeq <= 0L) {
            return;
        }
        AggregateProgress current = progressByAggregateId.get(aggregateId);
        if (current != null && current.aggregateSeq() >= aggregateSeq) {
            return;
        }
        progressByAggregateId.put(aggregateId, new AggregateProgress(aggregateId, aggregateSeq, eventRef, eventAt));
        persist();
    }

    @Override
    public synchronized int size() {
        return progressByAggregateId.size();
    }

    @Override
    public synchronized void reset() {
        progressByAggregateId.clear();
        persist();
    }

    private void load() {
        try {
            if (!Files.exists(statePath)) {
                reset();
                return;
            }
            Snapshot snapshot = OBJECT_MAPPER.readValue(statePath.toFile(), Snapshot.class);
            progressByAggregateId.clear();
            if (snapshot.progressByAggregateId() != null) {
                progressByAggregateId.putAll(snapshot.progressByAggregateId());
            }
        } catch (IOException exception) {
            StateFileRecovery.recover(statePath, "backoffice_aggregate_sequence", exception);
            reset();
        }
    }

    private void persist() {
        try {
            Files.createDirectories(statePath.getParent());
            OBJECT_MAPPER.writeValue(statePath.toFile(), new Snapshot(Map.copyOf(progressByAggregateId)));
        } catch (IOException exception) {
            throw new IllegalStateException("failed_to_persist_backoffice_aggregate_sequence:" + statePath, exception);
        }
    }

    private Path resolveStatePath() {
        String configured = System.getProperty(
            "backoffice.aggregate.sequence.path",
            System.getenv().getOrDefault("BACKOFFICE_AGGREGATE_SEQUENCE_PATH", "var/java-replay/backoffice/aggregate-sequence.json")
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

    private record Snapshot(Map<String, AggregateProgress> progressByAggregateId) {
    }

    private record AggregateProgress(String aggregateId, long aggregateSeq, String eventRef, long eventAt) {
    }
}
