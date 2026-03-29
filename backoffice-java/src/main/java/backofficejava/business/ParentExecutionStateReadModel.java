package backofficejava.business;

import java.util.Optional;

public interface ParentExecutionStateReadModel {
    Optional<ParentExecutionStateView> findByOrderId(String orderId);

    void upsert(ParentExecutionStateView view);

    void reset();
}
