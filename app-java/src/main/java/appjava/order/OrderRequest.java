package appjava.order;

public record OrderRequest(
    String symbol,
    String side,
    String type,
    int quantity,
    Double price,
    String timeInForce,
    Long expireAt
) {
}
