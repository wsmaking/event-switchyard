package appjava.order;

public record OrderEventView(
    String orderId,
    String eventType,
    long eventAt,
    String label,
    String detail,
    String source,
    String eventRef
) {
}
