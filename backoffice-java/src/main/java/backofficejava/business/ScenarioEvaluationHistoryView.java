package backofficejava.business;

import java.util.List;

public record ScenarioEvaluationHistoryView(
    long generatedAt,
    String accountId,
    String lastEvaluatedAtLabel,
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
