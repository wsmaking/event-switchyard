package backofficejava.business;

import java.util.List;

public record AllocationStateView(
    long generatedAt,
    String orderId,
    String accountId,
    String symbol,
    String allocationStatus,
    long allocatedQuantity,
    long pendingQuantity,
    double allocationAveragePrice,
    List<BookAllocationView> books,
    List<String> controls
) {
    public record BookAllocationView(
        String book,
        long targetQuantity,
        long allocatedQuantity,
        String status,
        String rationale
    ) {
    }
}
