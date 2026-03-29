package backofficejava.http;

import backofficejava.business.AccountHierarchyReadModel;

public final class AccountHierarchyHttpHandler extends JsonHttpHandler {
    public AccountHierarchyHttpHandler(AccountHierarchyReadModel readModel) {
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
                .orElseThrow(() -> new NotFoundException("account_hierarchy_not_found:" + accountId));
        });
    }
}
