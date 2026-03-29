package backofficejava.business;

import java.util.Optional;

public interface SettlementProjectionReadModel {
    Optional<SettlementProjectionView> findByOrderId(String orderId);

    void upsert(SettlementProjectionView view);

    void reset();
}
