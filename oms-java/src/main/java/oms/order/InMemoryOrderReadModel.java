package oms.order;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryOrderReadModel implements OrderReadModel {
    private final Map<String, OrderView> orders = new ConcurrentHashMap<>();

    public InMemoryOrderReadModel(String accountId) {
    }

    @Override
    public List<OrderView> findAll() {
        return orders.values().stream()
            .sorted((left, right) -> Long.compare(right.submittedAt(), left.submittedAt()))
            .toList();
    }

    @Override
    public Optional<OrderView> findById(String orderId) {
        return Optional.ofNullable(orders.get(orderId));
    }

    @Override
    public void upsert(OrderView orderView) {
        orders.put(orderView.id(), orderView);
    }

    @Override
    public void reset() {
        orders.clear();
    }
}
