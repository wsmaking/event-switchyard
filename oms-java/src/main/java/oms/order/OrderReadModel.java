package oms.order;

import java.util.List;
import java.util.Optional;

public interface OrderReadModel {
    List<OrderView> findAll();

    Optional<OrderView> findById(String orderId);

    void upsert(OrderView orderView);

    void reset();
}
