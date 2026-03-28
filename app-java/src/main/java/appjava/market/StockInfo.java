package appjava.market;

public record StockInfo(
    String symbol,
    String name,
    double currentPrice,
    double change,
    double changePercent,
    double high,
    double low,
    long volume
) {
}
