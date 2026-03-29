package backofficejava.business;

import java.util.Optional;

public interface AllocationStateReadModel {
    Optional<AllocationStateView> findByOrderId(String orderId);

    void upsert(AllocationStateView view);

    void reset();
}
