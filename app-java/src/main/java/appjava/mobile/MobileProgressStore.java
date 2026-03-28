package appjava.mobile;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.Map;

public final class MobileProgressStore {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final Path stateFile;
    private final String accountId;
    private PersistedProgress state;

    public MobileProgressStore(String accountId) {
        this.accountId = accountId;
        String stateRoot = System.getProperty(
            "app.state.dir",
            System.getenv().getOrDefault("APP_JAVA_STATE_DIR", "var/app-java")
        );
        this.stateFile = Path.of(stateRoot).resolve("mobile-progress.json");
        this.state = load();
    }

    public synchronized ProgressSnapshot snapshot() {
        return toSnapshot(state);
    }

    public synchronized ProgressSnapshot applyAnchor(String route, String orderId, String cardId) {
        state.anchor = new LearningAnchor(route, orderId, cardId, System.currentTimeMillis());
        state.updatedAt = state.anchor.updatedAt();
        persist();
        return toSnapshot(state);
    }

    public synchronized ProgressSnapshot reviewCard(String cardId, boolean correct) {
        PersistedCardProgress card = state.cards.computeIfAbsent(cardId, PersistedCardProgress::new);
        long now = System.currentTimeMillis();
        card.lastReviewedAt = now;
        if (correct) {
            card.correctCount += 1;
            card.masteryLevel = Math.min(3, card.masteryLevel + 1);
            card.completed = true;
        } else {
            card.incorrectCount += 1;
            card.masteryLevel = Math.max(0, card.masteryLevel - 1);
        }
        state.updatedAt = now;
        persist();
        return toSnapshot(state);
    }

    public synchronized ProgressSnapshot setBookmark(String cardId, boolean bookmarked) {
        PersistedCardProgress card = state.cards.computeIfAbsent(cardId, PersistedCardProgress::new);
        card.bookmarked = bookmarked;
        state.updatedAt = System.currentTimeMillis();
        persist();
        return toSnapshot(state);
    }

    private PersistedProgress load() {
        try {
            Files.createDirectories(stateFile.getParent());
            if (Files.exists(stateFile)) {
                PersistedProgress loaded = OBJECT_MAPPER.readValue(stateFile.toFile(), PersistedProgress.class);
                loaded.accountId = loaded.accountId == null || loaded.accountId.isBlank() ? accountId : loaded.accountId;
                if (loaded.cards == null) {
                    loaded.cards = new LinkedHashMap<>();
                }
                if (loaded.anchor == null) {
                    loaded.anchor = new LearningAnchor("/mobile", null, null, 0L);
                }
                return loaded;
            }
        } catch (IOException ignored) {
        }
        PersistedProgress fresh = new PersistedProgress();
        fresh.accountId = accountId;
        fresh.updatedAt = System.currentTimeMillis();
        fresh.anchor = new LearningAnchor("/mobile", null, null, 0L);
        fresh.cards = new LinkedHashMap<>();
        return fresh;
    }

    private void persist() {
        try {
            Files.createDirectories(stateFile.getParent());
            Path tempFile = stateFile.resolveSibling(stateFile.getFileName() + ".tmp");
            OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValue(tempFile.toFile(), state);
            Files.move(tempFile, stateFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException ignored) {
        }
    }

    private static ProgressSnapshot toSnapshot(PersistedProgress state) {
        Map<String, CardProgress> cards = new LinkedHashMap<>();
        for (Map.Entry<String, PersistedCardProgress> entry : state.cards.entrySet()) {
            PersistedCardProgress card = entry.getValue();
            cards.put(entry.getKey(), new CardProgress(
                card.cardId,
                card.bookmarked,
                card.completed,
                card.masteryLevel,
                card.correctCount,
                card.incorrectCount,
                card.lastReviewedAt
            ));
        }
        return new ProgressSnapshot(state.accountId, state.updatedAt, state.anchor, cards);
    }

    public record LearningAnchor(String route, String orderId, String cardId, long updatedAt) {
    }

    public record CardProgress(
        String cardId,
        boolean bookmarked,
        boolean completed,
        int masteryLevel,
        int correctCount,
        int incorrectCount,
        long lastReviewedAt
    ) {
    }

    public record ProgressSnapshot(
        String accountId,
        long updatedAt,
        LearningAnchor anchor,
        Map<String, CardProgress> cards
    ) {
    }

    static final class PersistedProgress {
        public String accountId;
        public long updatedAt;
        public LearningAnchor anchor;
        public Map<String, PersistedCardProgress> cards = new LinkedHashMap<>();
    }

    static final class PersistedCardProgress {
        public String cardId;
        public boolean bookmarked;
        public boolean completed;
        public int masteryLevel;
        public int correctCount;
        public int incorrectCount;
        public long lastReviewedAt;

        PersistedCardProgress() {
        }

        PersistedCardProgress(String cardId) {
            this.cardId = cardId;
        }
    }
}
