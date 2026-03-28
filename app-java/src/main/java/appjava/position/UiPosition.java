package appjava.position;

public record UiPosition(
    String symbol,
    int quantity,
    double avgPrice,
    double currentPrice,
    double unrealizedPnL,
    double unrealizedPnLPercent
) {
}
