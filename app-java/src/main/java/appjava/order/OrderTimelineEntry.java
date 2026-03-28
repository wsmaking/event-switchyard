package appjava.order;

public record OrderTimelineEntry(
    String eventType,
    long eventAt,
    String label,
    String detail
) {
}
