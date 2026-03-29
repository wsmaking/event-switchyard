package backofficejava.business;

import java.util.Optional;

public interface StatementProjectionReadModel {
    Optional<StatementProjectionView> findByOrderId(String orderId);

    void upsert(StatementProjectionView view);

    void reset();
}
