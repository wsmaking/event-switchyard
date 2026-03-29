package backofficejava.business;

import java.util.Optional;

public interface ExecutionPackageReadModel {
    Optional<ExecutionPackageView> findByOrderId(String orderId);

    void upsert(ExecutionPackageView view);

    void reset();
}
