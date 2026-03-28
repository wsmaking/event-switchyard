package backofficejava.http;

import backofficejava.audit.GatewayAuditIntakeService;

import java.util.Map;

public final class BackOfficeReconcileHttpHandler extends JsonHttpHandler {
    public BackOfficeReconcileHttpHandler(GatewayAuditIntakeService intakeService) {
        super(exchange -> {
            Map<String, String> query = parseQuery(exchange.getRequestURI().getRawQuery());
            return JsonResponse.ok(intakeService.reconcile(query.get("accountId")));
        });
    }
}
