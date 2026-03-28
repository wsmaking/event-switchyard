package backofficejava.account;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import backofficejava.support.StateFileRecovery;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class InMemoryAccountOverviewReadModel implements AccountOverviewReadModel {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final String seedAccountId;
    private final Path statePath;
    private final ConcurrentMap<String, AccountOverviewView> accounts = new ConcurrentHashMap<>();

    public InMemoryAccountOverviewReadModel(String accountId) {
        this.seedAccountId = accountId;
        this.statePath = resolveStatePath();
        load();
    }

    @Override
    public synchronized Optional<AccountOverviewView> findByAccountId(String accountId) {
        return Optional.ofNullable(accounts.get(accountId));
    }

    @Override
    public synchronized void upsert(AccountOverviewView view) {
        accounts.put(view.accountId(), view);
        persist();
    }

    @Override
    public synchronized void reset() {
        accounts.clear();
        accounts.put(
            seedAccountId,
            new AccountOverviewView(
                seedAccountId,
                10_000_000L,
                10_000_000L,
                0L,
                0,
                0L,
                Instant.parse("2026-03-28T00:00:01Z")
            )
        );
        persist();
    }

    private void load() {
        try {
            if (!Files.exists(statePath)) {
                reset();
                return;
            }
            Snapshot snapshot = OBJECT_MAPPER.readValue(statePath.toFile(), Snapshot.class);
            accounts.clear();
            if (snapshot.accounts() != null) {
                snapshot.accounts().forEach(view -> accounts.put(view.accountId(), view));
            }
        } catch (IOException e) {
            StateFileRecovery.recover(statePath, "backoffice_accounts", e);
            reset();
        }
    }

    private void persist() {
        try {
            Files.createDirectories(statePath.getParent());
            OBJECT_MAPPER.writeValue(statePath.toFile(), new Snapshot(accounts.values().stream().toList()));
        } catch (IOException e) {
            throw new IllegalStateException("failed_to_persist_backoffice_accounts:" + statePath, e);
        }
    }

    private Path resolveStatePath() {
        String configured = System.getProperty(
            "backoffice.accounts.path",
            System.getenv().getOrDefault("BACKOFFICE_ACCOUNTS_PATH", "var/java-replay/backoffice/accounts.json")
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

    private record Snapshot(java.util.List<AccountOverviewView> accounts) {
    }
}
