package oms.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public final class InMemoryDeadLetterStore {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final Path statePath;
    private final List<DeadLetterEntryView> entries = new ArrayList<>();

    public InMemoryDeadLetterStore() {
        this.statePath = resolveStatePath();
        load();
    }

    public synchronized void append(DeadLetterEntryView entry) {
        if (entry.eventRef() != null) {
            entries.removeIf(existing -> entry.eventRef().equals(existing.eventRef()));
        }
        entries.add(entry);
        entries.sort(Comparator.comparingLong(DeadLetterEntryView::recordedAt).reversed());
        persist();
    }

    public synchronized List<DeadLetterEntryView> find(String orderId, int limit) {
        return entries.stream()
            .filter(entry -> orderId == null || orderId.isBlank() || orderId.equals(entry.orderId()))
            .limit(Math.max(1, limit))
            .toList();
    }

    public synchronized int size() {
        return entries.size();
    }

    public synchronized DeadLetterEntryView findByEventRef(String eventRef) {
        return entries.stream()
            .filter(entry -> eventRef != null && eventRef.equals(entry.eventRef()))
            .findFirst()
            .orElse(null);
    }

    public synchronized void removeByEventRef(String eventRef) {
        entries.removeIf(entry -> eventRef != null && eventRef.equals(entry.eventRef()));
        persist();
    }

    public synchronized void reset() {
        entries.clear();
        persist();
    }

    private void load() {
        try {
            if (!Files.exists(statePath)) {
                reset();
                return;
            }
            Snapshot snapshot = OBJECT_MAPPER.readValue(statePath.toFile(), Snapshot.class);
            entries.clear();
            if (snapshot.entries() != null) {
                entries.addAll(snapshot.entries());
                entries.sort(Comparator.comparingLong(DeadLetterEntryView::recordedAt).reversed());
            }
        } catch (IOException exception) {
            throw new IllegalStateException("failed_to_load_oms_dead_letters:" + statePath, exception);
        }
    }

    private void persist() {
        try {
            Files.createDirectories(statePath.getParent());
            OBJECT_MAPPER.writeValue(statePath.toFile(), new Snapshot(List.copyOf(entries)));
        } catch (IOException exception) {
            throw new IllegalStateException("failed_to_persist_oms_dead_letters:" + statePath, exception);
        }
    }

    private Path resolveStatePath() {
        String configured = System.getProperty(
            "oms.dead.letter.path",
            System.getenv().getOrDefault("OMS_DEAD_LETTER_PATH", "var/java-replay/oms/dlq.json")
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

    private record Snapshot(List<DeadLetterEntryView> entries) {
    }
}
