package backofficejava.business;

import java.util.List;

public record OperatorControlStateView(
    long generatedAt,
    String orderId,
    String accountId,
    String workflowState,
    String escalationLevel,
    List<ApprovalRequirementView> requiredApprovals,
    List<OperatorAcknowledgementView> acknowledgements,
    List<String> permittedActions,
    List<String> blockedActions,
    List<String> auditTrail
) {
    public record ApprovalRequirementView(
        String name,
        String role,
        String state,
        String reason,
        String nextAction
    ) {
    }

    public record OperatorAcknowledgementView(
        String actor,
        String action,
        String atLabel,
        String note
    ) {
    }
}
