package backofficejava.business;

import java.util.Optional;

public interface CorporateActionWorkflowReadModel {
    Optional<CorporateActionWorkflowView> findByOrderId(String orderId);

    void upsert(CorporateActionWorkflowView view);

    void reset();
}
