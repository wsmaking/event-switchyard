package backofficejava.account;

public record LedgerEntryView(
    String entryId,
    String eventRef,
    String accountId,
    String orderId,
    String eventType,
    String symbol,
    String side,
    long quantityDelta,
    long cashDelta,
    long reservedBuyingPowerDelta,
    long realizedPnlDelta,
    String detail,
    long eventAt,
    String source
) {
}
