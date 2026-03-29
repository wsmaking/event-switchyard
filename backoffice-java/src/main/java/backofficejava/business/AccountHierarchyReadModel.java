package backofficejava.business;

import java.util.Optional;

public interface AccountHierarchyReadModel {
    Optional<AccountHierarchyView> findByAccountId(String accountId);

    void upsert(AccountHierarchyView view);

    void reset();
}
