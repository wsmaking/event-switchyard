package backofficejava.http;

import backofficejava.business.ScenarioEvaluationHistoryReadModel;

public final class ScenarioEvaluationHistoryHttpHandler extends JsonHttpHandler {
    public ScenarioEvaluationHistoryHttpHandler(ScenarioEvaluationHistoryReadModel readModel) {
        super(exchange -> {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                throw new MethodNotAllowedException(exchange.getRequestMethod());
            }
            String accountId = parseQuery(exchange.getRequestURI().getRawQuery()).get("accountId");
            if (accountId == null || accountId.isBlank()) {
                throw new NotFoundException("missing_account_id");
            }
            return readModel.findByAccountId(accountId)
                .<JsonResponse>map(JsonResponse::ok)
                .orElseThrow(() -> new NotFoundException("scenario_evaluation_history_not_found:" + accountId));
        });
    }
}
