package appjava.order;

public record OrderView(
    String id,
    String accountId,
    String symbol,
    String side,
    String type,
    int quantity,
    Double price,
    String timeInForce,
    Long expireAt,
    OrderStatus status,
    long submittedAt,
    Long filledAt,
    Double executionTimeMs,
    String statusReason,
    long filledQuantity,
    long remainingQuantity
) {
}
