package backofficejava.http;

import backofficejava.business.RiskSnapshotReadModel;

public final class RiskSnapshotHttpHandler extends JsonHttpHandler {
    public RiskSnapshotHttpHandler(RiskSnapshotReadModel readModel) {
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
                .orElseThrow(() -> new NotFoundException("risk_snapshot_not_found:" + accountId));
        });
    }
}
