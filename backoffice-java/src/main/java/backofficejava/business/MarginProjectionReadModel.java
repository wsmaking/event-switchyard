package backofficejava.business;

import java.util.Optional;

public interface MarginProjectionReadModel {
    Optional<MarginProjectionView> findByAccountId(String accountId);

    void upsert(MarginProjectionView view);

    void reset();
}
