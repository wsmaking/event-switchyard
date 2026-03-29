package backofficejava.business;

import java.util.List;

public record CorporateActionWorkflowView(
    long generatedAt,
    String orderId,
    String accountId,
    String symbol,
    String eventName,
    String workflowStatus,
    String recordDateLabel,
    String effectiveDateLabel,
    String customerImpact,
    String ledgerImpact,
    String booksRecordImpact,
    String ledgerContinuityCheck,
    String nextAction,
    List<String> controls
) {
}
