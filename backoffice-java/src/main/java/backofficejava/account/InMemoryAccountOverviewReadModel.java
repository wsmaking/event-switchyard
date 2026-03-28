package backofficejava.account;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class InMemoryAccountOverviewReadModel implements AccountOverviewReadModel {
    private final String seedAccountId;
    private final ConcurrentMap<String, AccountOverviewView> accounts = new ConcurrentHashMap<>();

    public InMemoryAccountOverviewReadModel(String accountId) {
        this.seedAccountId = accountId;
        reset();
    }

    @Override
    public Optional<AccountOverviewView> findByAccountId(String accountId) {
        return Optional.ofNullable(accounts.get(accountId));
    }

    @Override
    public void upsert(AccountOverviewView view) {
        accounts.put(view.accountId(), view);
    }

    @Override
    public void reset() {
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
    }
}
