package backofficejava.business;

import java.util.List;

public record ExecutionPackageView(
    long generatedAt,
    String orderId,
    String accountId,
    String symbol,
    String symbolName,
    String clientIntent,
    List<ExecutionStyleView> executionStyles,
    ParentOrderPlanView parentOrderPlan,
    List<ChildOrderSliceView> childOrders,
    AllocationPlanView allocationPlan,
    List<String> operatorChecks
) {
    public record ExecutionStyleView(
        String name,
        String useCase,
        String businessRule,
        String systemImplication,
        List<String> tradeoffs
    ) {
    }

    public record ParentOrderPlanView(
        String side,
        long totalQuantity,
        double arrivalMidPrice,
        double targetParticipationPercent,
        int scheduleWindowMinutes,
        String chosenStyle,
        List<String> whyNotOtherChoices
    ) {
    }

    public record ChildOrderSliceView(
        String id,
        String venueIntent,
        long plannedQuantity,
        double benchmarkPrice,
        double expectedFillPrice,
        double expectedSlippageBps,
        String timeBucketLabel,
        String learningPoint
    ) {
    }

    public record AllocationPlanView(
        String blockBook,
        double averagePrice,
        long totalQuantity,
        List<AllocationSliceView> allocations,
        String settlementNote,
        List<String> controlChecks
    ) {
    }

    public record AllocationSliceView(
        String targetBook,
        long quantity,
        double ratioPercent,
        String note
    ) {
    }
}
