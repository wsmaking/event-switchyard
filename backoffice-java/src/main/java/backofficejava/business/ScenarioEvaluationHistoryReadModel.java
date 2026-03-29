package backofficejava.business;

import java.util.Optional;

public interface ScenarioEvaluationHistoryReadModel {
    Optional<ScenarioEvaluationHistoryView> findByAccountId(String accountId);

    void upsert(ScenarioEvaluationHistoryView view);

    void reset();
}
