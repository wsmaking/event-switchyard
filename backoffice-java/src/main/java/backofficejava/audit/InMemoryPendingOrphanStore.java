package backofficejava.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public final class InMemoryPendingOrphanStore {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final Path statePath;
    private final List<PendingOrphanEntryView> entries = new ArrayList<>();

    public InMemoryPendingOrphanStore() {
        this.statePath = resolveStatePath();
        load();
    }

    public synchronized void append(PendingOrphanEntryView entry) {
        if (entries.stream().noneMatch(existing -> existing.eventRef().equals(entry.eventRef()))) {
            entries.add(entry);
            entries.sort(Comparator.comparingLong(PendingOrphanEntryView::eventAt));
            persist();
        }
    }

    public synchronized List<PendingOrphanEntryView> find(String orderId, int limit) {
        return entries.stream()
            .filter(entry -> orderId == null || orderId.isBlank() || orderId.equals(entry.orderId()))
            .sorted(Comparator.comparingLong(PendingOrphanEntryView::eventAt))
            .limit(Math.max(1, limit))
            .toList();
    }

    public synchronized void removeByEventRef(String eventRef) {
        entries.removeIf(entry -> eventRef.equals(entry.eventRef()));
        persist();
    }

    public synchronized int size() {
        return entries.size();
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
                entries.sort(Comparator.comparingLong(PendingOrphanEntryView::eventAt));
            }
        } catch (IOException exception) {
            throw new IllegalStateException("failed_to_load_backoffice_pending_orphans:" + statePath, exception);
        }
    }

    private void persist() {
        try {
            Files.createDirectories(statePath.getParent());
            OBJECT_MAPPER.writeValue(statePath.toFile(), new Snapshot(List.copyOf(entries)));
        } catch (IOException exception) {
            throw new IllegalStateException("failed_to_persist_backoffice_pending_orphans:" + statePath, exception);
        }
    }

    private Path resolveStatePath() {
        String configured = System.getProperty(
            "backoffice.pending.orphan.path",
            System.getenv().getOrDefault("BACKOFFICE_PENDING_ORPHAN_PATH", "var/java-replay/backoffice/orphan-pending.json")
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

    private record Snapshot(List<PendingOrphanEntryView> entries) {
    }
}
