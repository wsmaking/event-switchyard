package appjava.order;

public record FillView(
    String fillId,
    String orderId,
    String accountId,
    String symbol,
    String side,
    long quantity,
    double price,
    long notional,
    String liquidity,
    long filledAt
) {
}
