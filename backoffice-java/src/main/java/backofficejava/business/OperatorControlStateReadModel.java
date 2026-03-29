package backofficejava.business;

import java.util.Optional;

public interface OperatorControlStateReadModel {
    Optional<OperatorControlStateView> findByOrderId(String orderId);

    void upsert(OperatorControlStateView view);

    void reset();
}
