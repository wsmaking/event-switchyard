package backofficejava.business;

import java.util.List;

public record SettlementExceptionWorkflowView(
    long generatedAt,
    String orderId,
    String accountId,
    String symbol,
    String workflowStatus,
    String exceptionType,
    String blockedStage,
    String ageingLabel,
    String rootCause,
    String nextAction,
    List<String> controls,
    List<String> operatorNotes
) {
}
