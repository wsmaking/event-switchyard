package oms.audit;

public record DeadLetterEntryView(
    String entryId,
    String eventRef,
    String accountId,
    String orderId,
    String eventType,
    String reason,
    String detail,
    String rawLine,
    long eventAt,
    long recordedAt,
    String source
) {
}
