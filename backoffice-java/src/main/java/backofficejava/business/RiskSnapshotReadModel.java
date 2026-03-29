package backofficejava.business;

import java.util.Optional;

public interface RiskSnapshotReadModel {
    Optional<RiskSnapshotView> findByAccountId(String accountId);

    void upsert(RiskSnapshotView view);

    void reset();
}
