package appjava.order;

public record ExecutionQualityView(
    String benchmarkLabel,
    String venueState,
    double arrivalLastPrice,
    double arrivalBidPrice,
    double arrivalAskPrice,
    double arrivalMidPrice,
    double arrivalSpreadBps,
    Double averageExecutionPrice,
    Double fillRatePercent,
    Double slippagePerShare,
    Double slippageAmount,
    Double slippageBps,
    String note
) {
}
