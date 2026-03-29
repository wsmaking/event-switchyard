package backofficejava.http;

import backofficejava.business.MarginProjectionReadModel;

public final class MarginProjectionHttpHandler extends JsonHttpHandler {
    public MarginProjectionHttpHandler(MarginProjectionReadModel readModel) {
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
                .orElseThrow(() -> new NotFoundException("margin_projection_not_found:" + accountId));
        });
    }
}
