package backofficejava.business;

import java.util.List;

public record ScenarioEvaluationHistoryView(
    long generatedAt,
    String accountId,
    String lastEvaluatedAtLabel,
    String governanceState,
    String modelVersion,
    List<String> approvals,
    List<ScenarioEvaluationEntryView> evaluations
) {
    public record ScenarioEvaluationEntryView(
        String title,
        String shock,
        double pnlDelta,
        String note
    ) {
    }
}
