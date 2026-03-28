package backofficejava.account;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public final class InMemoryLedgerReadModel implements LedgerReadModel {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final Path statePath;
    private final List<LedgerEntryView> entries = new ArrayList<>();

    public InMemoryLedgerReadModel() {
        this.statePath = resolveStatePath();
        load();
    }

    @Override
    public synchronized List<LedgerEntryView> find(
        String accountId,
        String orderId,
        String eventType,
        int limit,
        Long afterEventAt
    ) {
        return entries.stream()
            .filter(entry -> accountId == null || accountId.isBlank() || accountId.equals(entry.accountId()))
            .filter(entry -> orderId == null || orderId.isBlank() || orderId.equals(entry.orderId()))
            .filter(entry -> eventType == null || eventType.isBlank() || eventType.equals(entry.eventType()))
            .filter(entry -> afterEventAt == null || entry.eventAt() > afterEventAt)
            .sorted(Comparator.comparingLong(LedgerEntryView::eventAt).reversed())
            .limit(Math.max(1, limit))
            .toList();
    }

    @Override
    public synchronized List<LedgerEntryView> findByAccountId(String accountId) {
        return entries.stream()
            .filter(entry -> accountId.equals(entry.accountId()))
            .sorted(Comparator.comparingLong(LedgerEntryView::eventAt))
            .toList();
    }

    @Override
    public synchronized void append(LedgerEntryView entry) {
        entries.add(entry);
        entries.sort(Comparator.comparingLong(LedgerEntryView::eventAt));
        persist();
    }

    @Override
    public synchronized boolean containsEventRef(String eventRef) {
        return entries.stream().anyMatch(entry -> eventRef.equals(entry.eventRef()));
    }

    @Override
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
                entries.sort(Comparator.comparingLong(LedgerEntryView::eventAt));
            }
        } catch (IOException exception) {
            throw new IllegalStateException("failed_to_load_backoffice_ledger:" + statePath, exception);
        }
    }

    private void persist() {
        try {
            Files.createDirectories(statePath.getParent());
            OBJECT_MAPPER.writeValue(statePath.toFile(), new Snapshot(List.copyOf(entries)));
        } catch (IOException exception) {
            throw new IllegalStateException("failed_to_persist_backoffice_ledger:" + statePath, exception);
        }
    }

    private Path resolveStatePath() {
        String configured = System.getProperty(
            "backoffice.ledger.path",
            System.getenv().getOrDefault("BACKOFFICE_LEDGER_PATH", "var/java-replay/backoffice/ledger.json")
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

    private record Snapshot(List<LedgerEntryView> entries) {
    }
}
