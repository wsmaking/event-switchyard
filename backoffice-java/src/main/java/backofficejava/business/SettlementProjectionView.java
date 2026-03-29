package backofficejava.business;

import java.util.List;

public record SettlementProjectionView(
    long generatedAt,
    String orderId,
    String accountId,
    String symbol,
    String settlementStatus,
    String tradeDateLabel,
    String settlementDateLabel,
    long grossNotional,
    long netCashMovement,
    long settledQuantity,
    String cashLegStatus,
    String securitiesLegStatus,
    List<String> exceptionFlags,
    String nextAction
) {
}
