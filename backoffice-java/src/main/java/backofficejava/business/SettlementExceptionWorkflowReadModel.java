package backofficejava.business;

import java.util.Optional;

public interface SettlementExceptionWorkflowReadModel {
    Optional<SettlementExceptionWorkflowView> findByOrderId(String orderId);

    void upsert(SettlementExceptionWorkflowView view);

    void reset();
}
