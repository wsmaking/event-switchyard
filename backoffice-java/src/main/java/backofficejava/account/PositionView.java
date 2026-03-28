package backofficejava.account;

public record PositionView(
    String accountId,
    String symbol,
    long netQty,
    double avgPrice
) {
}
