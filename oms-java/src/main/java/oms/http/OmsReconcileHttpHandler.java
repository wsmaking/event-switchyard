package oms.http;

import oms.audit.GatewayAuditIntakeService;

import java.util.Map;

public final class OmsReconcileHttpHandler extends JsonHttpHandler {
    public OmsReconcileHttpHandler(GatewayAuditIntakeService intakeService) {
        super(exchange -> {
            Map<String, String> query = parseQuery(exchange.getRequestURI().getRawQuery());
            return JsonResponse.ok(intakeService.reconcile(query.get("accountId")));
        });
    }
}
