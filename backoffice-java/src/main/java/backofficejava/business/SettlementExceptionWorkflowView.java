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
    String exceptionOwner,
    String resolutionEtaLabel,
    long cashBreakAmount,
    long securitiesBreakQuantity,
    boolean cancelCorrectRequired,
    String failAgingBucket,
    String nextAction,
    List<String> breakDetails,
    List<String> controls,
    List<String> operatorNotes
) {
}
