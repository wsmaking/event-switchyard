package backofficejava.business;

import java.util.Optional;

public interface BacktestHistoryReadModel {
    Optional<BacktestHistoryView> findByAccountId(String accountId);

    void upsert(BacktestHistoryView view);

    void reset();
}
