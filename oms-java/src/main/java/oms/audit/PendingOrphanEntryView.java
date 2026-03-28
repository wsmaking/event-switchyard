package oms.audit;

public record PendingOrphanEntryView(
    String entryId,
    String eventRef,
    String accountId,
    String orderId,
    String eventType,
    String reason,
    String rawLine,
    long eventAt,
    long recordedAt,
    String source
) {
}
