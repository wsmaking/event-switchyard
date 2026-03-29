package appjava.order;

public record ExecutionBenchmark(
    String orderId,
    String symbol,
    long capturedAt,
    double arrivalLastPrice,
    double arrivalBidPrice,
    double arrivalAskPrice,
    double arrivalMidPrice,
    double arrivalSpreadBps,
    String venueState,
    String benchmarkLabel
) {
}
