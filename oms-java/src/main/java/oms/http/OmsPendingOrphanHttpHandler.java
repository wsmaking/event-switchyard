package oms.http;

import oms.audit.GatewayAuditIntakeService;

import java.util.Map;

public final class OmsPendingOrphanHttpHandler extends JsonHttpHandler {
    public OmsPendingOrphanHttpHandler(GatewayAuditIntakeService intakeService) {
        super(exchange -> {
            Map<String, String> query = parseQuery(exchange.getRequestURI().getRawQuery());
            int limit = query.containsKey("limit") ? Integer.parseInt(query.get("limit")) : 50;
            return JsonResponse.ok(intakeService.findPendingOrphans(query.get("orderId"), limit));
        });
    }
}
