package oms.order;

import java.util.List;
import java.util.Optional;

public interface OrderReadModel {
    List<OrderView> findAll();

    Optional<OrderView> findById(String orderId);

    List<OrderEventView> findEventsByOrderId(String orderId);

    List<ReservationView> findReservationsByAccountId(String accountId);

    void upsert(OrderView orderView);

    void replaceEvents(String orderId, List<OrderEventView> events);

    void replaceReservations(String accountId, List<ReservationView> reservations);

    void reset();
}
