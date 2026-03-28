package oms.order;

public record ReservationView(
    String reservationId,
    String accountId,
    String orderId,
    String symbol,
    String side,
    long reservedQuantity,
    long reservedAmount,
    long releasedAmount,
    String status,
    long openedAt,
    long updatedAt
) {
}
