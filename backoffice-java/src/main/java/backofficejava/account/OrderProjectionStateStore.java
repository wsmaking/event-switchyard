package backofficejava.account;

import java.util.List;
import java.util.Optional;

public interface OrderProjectionStateStore {
    Optional<OrderProjectionState> findByOrderId(String orderId);

    List<OrderProjectionState> findByAccountId(String accountId);

    void upsert(OrderProjectionState state);

    void reset();
}
