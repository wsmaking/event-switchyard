package backofficejava.account;

import java.util.Optional;

public interface AccountOverviewReadModel {
    Optional<AccountOverviewView> findByAccountId(String accountId);

    void upsert(AccountOverviewView view);

    void reset();
}
