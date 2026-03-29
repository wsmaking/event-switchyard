package backofficejava.business;

import java.util.List;

public record ParentExecutionStateView(
    long generatedAt,
    String orderId,
    String accountId,
    String symbol,
    String parentStatus,
    String executionStyle,
    long targetQuantity,
    long executedQuantity,
    long remainingQuantity,
    double participationTargetPercent,
    double participationActualPercent,
    String scheduleWindowLabel,
    List<ChildExecutionStateView> childStates,
    List<String> operatorAlerts
) {
    public record ChildExecutionStateView(
        String childId,
        String state,
        String venueIntent,
        long plannedQuantity,
        long executedQuantity,
        long remainingQuantity,
        double benchmarkPrice,
        double averageFillPrice,
        double slippageBps,
        String nextAction
    ) {
    }
}
