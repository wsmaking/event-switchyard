package backofficejava.account;

public record OrderProjectionState(
    String orderId,
    String accountId,
    String symbol,
    String side,
    long quantity,
    long workingPrice,
    long submittedAt,
    long lastEventAt,
    String status,
    long filledQuantity,
    long reservedAmount
) {
}
